// Copyright 2023 The grovemq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package acl implements topic/clientid/username based publish/subscribe
// authorization for grovemq, analogous to EMQX's emqx_acl. It mirrors the
// structure of the pkg/blacklist package: an in-memory rule manager plus an
// engine that is consulted on every SUBSCRIBE/PUBLISH.
package acl

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Permission values for a rule.
const (
	PermissionAllow = "allow"
	PermissionDeny  = "deny"
)

// Action values for a rule. "all" / "pubsub" match both publish and subscribe.
const (
	ActionPublish   = "publish"
	ActionSubscribe = "subscribe"
	ActionPubSub    = "pubsub"
	ActionAll       = "all"
)

// AnonymousSubject is the special username value that matches an empty (anonymous)
// authenticated username.
const AnonymousSubject = "anonymous"

// Rule represents a single ACL rule. A rule matches a request when all of its
// non-empty dimensions match; the first matching rule (ordered by priority then
// insertion order) decides allow/deny.
type Rule struct {
	ID              string                 `json:"id"`
	Permission      string                 `json:"permission"` // allow | deny
	Action          string                 `json:"action"`     // publish | subscribe | pubsub | all
	Username        string                 `json:"username"`   // exact; "" = any; "anonymous" = empty username
	UsernamePattern string                 `json:"username_pattern,omitempty"`
	ClientID        string                 `json:"clientid"` // exact; "" = any
	ClientIDPattern string                 `json:"clientid_pattern,omitempty"`
	Topic           string                 `json:"topic"` // MQTT topic filter with +/#; "" = any
	Enabled         bool                   `json:"enabled"`
	Priority        int                    `json:"priority"` // lower = evaluated first
	Description     string                 `json:"description,omitempty"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`

	// seq is an internal insertion sequence used to break priority ties
	// deterministically. It is not serialized.
	seq int
}

// compiledPatterns caches compiled regexps for a rule keyed by rule ID.
type compiledPatterns struct {
	username *regexp.Regexp
	clientid *regexp.Regexp
}

// Manager holds the in-memory set of ACL rules and performs access decisions.
type Manager struct {
	mu       sync.RWMutex
	rules    map[string]*Rule
	order    []string
	seq      int
	compiled map[string]*compiledPatterns

	stats ManagerStats
}

// ManagerStats holds counters and recent denials for observability.
type ManagerStats struct {
	TotalRules    int         `json:"total_rules"`
	EnabledRules  int         `json:"enabled_rules"`
	AllowRules    int         `json:"allow_rules"`
	DenyRules     int         `json:"deny_rules"`
	MatchCount    int         `json:"match_count"`
	DenyCount     int         `json:"deny_count"`
	AllowCount    int         `json:"allow_count"`
	RecentDenials []ACLDenial `json:"recent_denials"`
}

// ACLDenial records a denied access attempt.
type ACLDenial struct {
	Timestamp time.Time `json:"timestamp"`
	ClientID  string    `json:"clientid"`
	Username  string    `json:"username"`
	Topic     string    `json:"topic"`
	Action    string    `json:"action"`
	Reason    string    `json:"reason"`
	RuleID    string    `json:"rule_id"`
}

// ACLStats is the public statistics snapshot.
type ACLStats struct {
	ManagerStats
	LastUpdated time.Time `json:"last_updated"`
}

// NewManager creates an empty ACL manager.
func NewManager() *Manager {
	return &Manager{
		rules:    make(map[string]*Rule),
		compiled: make(map[string]*compiledPatterns),
	}
}

func validateRule(rule *Rule) error {
	switch rule.Permission {
	case PermissionAllow, PermissionDeny:
	default:
		return fmt.Errorf("invalid permission %q (want allow or deny)", rule.Permission)
	}
	switch rule.Action {
	case ActionPublish, ActionSubscribe, ActionPubSub, ActionAll, "":
	default:
		return fmt.Errorf("invalid action %q (want publish, subscribe, pubsub, or all)", rule.Action)
	}
	return nil
}

// AddRule inserts a new rule. An empty ID is generated. Zero timestamp fields are
// populated. Returns an error if the rule is invalid or the ID already exists.
func (m *Manager) AddRule(rule *Rule) error {
	if rule == nil {
		return fmt.Errorf("nil rule")
	}
	if err := validateRule(rule); err != nil {
		return err
	}
	if rule.Permission == "" {
		rule.Permission = PermissionAllow
	}
	if rule.Action == "" {
		rule.Action = ActionAll
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if rule.ID == "" {
		m.seq++
		rule.ID = fmt.Sprintf("acl_%d", time.Now().UnixNano())
	} else if _, exists := m.rules[rule.ID]; exists {
		return fmt.Errorf("rule %q already exists", rule.ID)
	}

	now := time.Now()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}
	if rule.UpdatedAt.IsZero() {
		rule.UpdatedAt = now
	}
	rule.seq = m.seq
	m.seq++

	m.rules[rule.ID] = rule
	m.order = append(m.order, rule.ID)
	m.compiled[rule.ID] = m.compileLocked(rule)
	m.updateStatsLocked()
	return nil
}

// UpdateRule updates mutable fields of an existing rule. Empty fields are left
// unchanged. The Permission/Action fields may be reset by passing the canonical
// value explicitly.
func (m *Manager) UpdateRule(id string, updates *Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.rules[id]
	if !ok {
		return fmt.Errorf("rule %q not found", id)
	}

	if updates.Permission != "" {
		existing.Permission = updates.Permission
	}
	if updates.Action != "" {
		existing.Action = updates.Action
	}
	if updates.Username != "" || updates.Username == AnonymousSubject {
		existing.Username = updates.Username
	}
	if updates.UsernamePattern != "" {
		existing.UsernamePattern = updates.UsernamePattern
	}
	if updates.ClientID != "" {
		existing.ClientID = updates.ClientID
	}
	if updates.ClientIDPattern != "" {
		existing.ClientIDPattern = updates.ClientIDPattern
	}
	if updates.Topic != "" {
		existing.Topic = updates.Topic
	}
	existing.Enabled = updates.Enabled
	existing.Priority = updates.Priority
	if updates.Description != "" {
		existing.Description = updates.Description
	}
	if updates.Metadata != nil {
		existing.Metadata = updates.Metadata
	}
	existing.UpdatedAt = time.Now()

	if err := validateRule(existing); err != nil {
		return err
	}

	m.compiled[id] = m.compileLocked(existing)
	m.updateStatsLocked()
	return nil
}

// RemoveRule deletes a rule by ID. Returns an error if it does not exist.
func (m *Manager) RemoveRule(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.rules[id]; !ok {
		return fmt.Errorf("rule %q not found", id)
	}
	delete(m.rules, id)
	delete(m.compiled, id)
	for i, rid := range m.order {
		if rid == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.updateStatsLocked()
	return nil
}

// GetRule returns a rule by ID.
func (m *Manager) GetRule(id string) *Rule {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if r, ok := m.rules[id]; ok {
		return r
	}
	return nil
}

// ListRules returns rules. When enabled is non-nil only rules whose Enabled
// matches are returned. The result is ordered by priority then insertion order.
func (m *Manager) ListRules(enabled *bool) []*Rule {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]*Rule, 0, len(m.rules))
	for _, id := range m.order {
		r := m.rules[id]
		if enabled != nil && r.Enabled != *enabled {
			continue
		}
		out = append(out, r)
	}
	sortRules(out)
	return out
}

// sortRules orders rules by priority (ascending) then insertion sequence
// (ascending) so the first rule in the slice is the highest-priority match.
func sortRules(rules []*Rule) {
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}
		return rules[i].seq < rules[j].seq
	})
}

// compileLocked compiles the regex patterns of a rule. Caller must hold the lock.
func (m *Manager) compileLocked(rule *Rule) *compiledPatterns {
	cp := &compiledPatterns{}
	if rule.UsernamePattern != "" {
		if re, err := regexp.Compile(rule.UsernamePattern); err == nil {
			cp.username = re
		}
	}
	if rule.ClientIDPattern != "" {
		if re, err := regexp.Compile(rule.ClientIDPattern); err == nil {
			cp.clientid = re
		}
	}
	return cp
}

// CheckAccess evaluates the rules for a single request and returns whether access
// is allowed, the matching rule (if any), and an error. It does NOT apply the
// default action; the engine layers that on top so the default is configurable.
func (m *Manager) CheckAccess(clientID, username, topic, action string) (bool, *Rule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, id := range m.order {
		rule := m.rules[id]
		if !rule.Enabled {
			continue
		}
		if !actionMatches(rule.Action, action) {
			continue
		}
		if !usernameMatches(rule, m.compiled[id], username) {
			continue
		}
		if !clientIDMatches(rule, m.compiled[id], clientID) {
			continue
		}
		if !topicMatches(rule.Topic, topic) {
			continue
		}
		// First matching rule decides.
		allow := rule.Permission == PermissionAllow
		m.recordMatchLocked(rule, allow)
		return allow, rule, nil
	}
	return true, nil, nil // no match -> caller applies default action
}

func actionMatches(ruleAction, requestAction string) bool {
	if ruleAction == "" || ruleAction == ActionAll || ruleAction == ActionPubSub {
		return true
	}
	return ruleAction == requestAction
}

func usernameMatches(rule *Rule, cp *compiledPatterns, username string) bool {
	if rule.Username == "" {
		// any username (including anonymous)
	} else if rule.Username == AnonymousSubject {
		if username != "" {
			return false
		}
	} else if rule.Username != username {
		return false
	}
	if cp != nil && cp.username != nil && !cp.username.MatchString(username) {
		return false
	}
	return true
}

func clientIDMatches(rule *Rule, cp *compiledPatterns, clientID string) bool {
	if rule.ClientID != "" && rule.ClientID != clientID {
		return false
	}
	if cp != nil && cp.clientid != nil && !cp.clientid.MatchString(clientID) {
		return false
	}
	return true
}

func topicMatches(ruleTopic, topic string) bool {
	if ruleTopic == "" {
		return true
	}
	return matchTopicFilter(topic, ruleTopic)
}

func (m *Manager) recordMatchLocked(rule *Rule, allow bool) {
	m.stats.MatchCount++
	if allow {
		m.stats.AllowCount++
	} else {
		m.stats.DenyCount++
	}
}

func (m *Manager) updateStatsLocked() {
	m.stats.TotalRules = len(m.rules)
	m.stats.EnabledRules = 0
	m.stats.AllowRules = 0
	m.stats.DenyRules = 0
	for _, id := range m.order {
		r := m.rules[id]
		if r.Enabled {
			m.stats.EnabledRules++
		}
		switch r.Permission {
		case PermissionAllow:
			m.stats.AllowRules++
		case PermissionDeny:
			m.stats.DenyRules++
		}
	}
}

// GetStats returns a snapshot of statistics including recent denials.
func (m *Manager) GetStats() ACLStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return ACLStats{
		ManagerStats: m.stats,
		LastUpdated:  time.Now(),
	}
}

// recordDenial records a denied access for observability (called by the engine).
func (m *Manager) recordDenial(d ACLDenial) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.RecentDenials = append(m.stats.RecentDenials, d)
	if len(m.stats.RecentDenials) > 100 {
		m.stats.RecentDenials = m.stats.RecentDenials[len(m.stats.RecentDenials)-100:]
	}
}

// Clear removes all rules.
func (m *Manager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules = make(map[string]*Rule)
	m.order = nil
	m.compiled = make(map[string]*compiledPatterns)
	m.updateStatsLocked()
}

// matchTopicFilter implements MQTT topic filter matching with + and # wildcards,
// mirroring pkg/blacklist/manager.go's matchMQTTTopicFilter.
func matchTopicFilter(topic, filter string) bool {
	topicLevels := strings.Split(topic, "/")
	filterLevels := strings.Split(filter, "/")
	return matchTopicLevels(topicLevels, filterLevels, 0, 0)
}

func matchTopicLevels(topicLevels, filterLevels []string, topicIndex, filterIndex int) bool {
	if filterIndex >= len(filterLevels) {
		return topicIndex >= len(topicLevels)
	}
	if topicIndex >= len(topicLevels) {
		return filterIndex == len(filterLevels)-1 && filterLevels[filterIndex] == "#"
	}

	filterLevel := filterLevels[filterIndex]
	if filterLevel == "#" {
		return true
	}
	if filterLevel == "+" {
		return matchTopicLevels(topicLevels, filterLevels, topicIndex+1, filterIndex+1)
	}
	if filterLevel == topicLevels[topicIndex] {
		return matchTopicLevels(topicLevels, filterLevels, topicIndex+1, filterIndex+1)
	}
	return false
}
