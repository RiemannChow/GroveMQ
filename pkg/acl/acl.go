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

package acl

import (
	"log"
	"time"
)

// DefaultAction values.
const (
	DefaultActionAllow = "allow"
	DefaultActionDeny  = "deny"
)

// ACLConfig controls engine behavior.
type ACLConfig struct {
	// Enabled toggles enforcement. When false, all access is allowed.
	Enabled bool `json:"enabled"`
	// DefaultAction is applied when no rule matches: "allow" or "deny".
	DefaultAction string `json:"default_action"`
	// LogDenials logs each denied access.
	LogDenials bool `json:"log_denials"`
}

// DefaultACLConfig returns the default config: disabled, default allow.
func DefaultACLConfig() *ACLConfig {
	return &ACLConfig{
		Enabled:       false,
		DefaultAction: DefaultActionAllow,
		LogDenials:    true,
	}
}

// Engine enforces ACL rules on publish/subscribe requests.
type Engine struct {
	manager *Manager
	config  *ACLConfig
}

// NewEngine creates an ACL engine wrapping the given manager and config.
func NewEngine(manager *Manager, config *ACLConfig) *Engine {
	if config == nil {
		config = DefaultACLConfig()
	}
	return &Engine{manager: manager, config: config}
}

// IsEnabled reports whether ACL enforcement is active.
func (e *Engine) IsEnabled() bool {
	return e.config != nil && e.config.Enabled
}

// GetManager returns the underlying rule manager.
func (e *Engine) GetManager() *Manager {
	return e.manager
}

// GetConfig returns the active config (copy-safe values).
func (e *Engine) GetConfig() *ACLConfig {
	if e.config == nil {
		return DefaultACLConfig()
	}
	c := *e.config
	return &c
}

// UpdateConfig replaces the active config.
func (e *Engine) UpdateConfig(config *ACLConfig) {
	if config == nil {
		config = DefaultACLConfig()
	}
	e.config = config
}

// CheckPublishAccess checks whether a client may publish to a topic.
func (e *Engine) CheckPublishAccess(clientID, username, ip, topic string) (bool, string, error) {
	return e.checkAccess(clientID, username, ip, topic, ActionPublish)
}

// CheckSubscribeAccess checks whether a client may subscribe to a topic filter.
func (e *Engine) CheckSubscribeAccess(clientID, username, ip, topic string) (bool, string, error) {
	return e.checkAccess(clientID, username, ip, topic, ActionSubscribe)
}

func (e *Engine) checkAccess(clientID, username, ip, topic, action string) (bool, string, error) {
	if !e.IsEnabled() {
		return true, "", nil
	}

	allow, matched, err := e.manager.CheckAccess(clientID, username, topic, action)
	if err != nil {
		return false, "internal acl error", err
	}

	if matched != nil {
		reason := ""
		if !allow {
			reason = "denied by acl rule " + matched.ID
			e.manager.recordDenial(ACLDenial{
				Timestamp: time.Now(),
				ClientID:  clientID,
				Username:  username,
				Topic:     topic,
				Action:    action,
				Reason:    reason,
				RuleID:    matched.ID,
			})
			if e.config.LogDenials {
				log.Printf("[ACL] DENY %s client=%s user=%s topic=%s rule=%s", action, clientID, username, topic, matched.ID)
			}
		}
		return allow, reason, nil
	}

	// No rule matched: apply default action.
	allow = e.config.DefaultAction != DefaultActionDeny
	if !allow {
		reason := "denied by acl default action"
		e.manager.recordDenial(ACLDenial{
			Timestamp: time.Now(),
			ClientID:  clientID,
			Username:  username,
			Topic:     topic,
			Action:    action,
			Reason:    reason,
		})
		if e.config.LogDenials {
			log.Printf("[ACL] DENY %s client=%s user=%s topic=%s (default action)", action, clientID, username, topic)
		}
		return allow, reason, nil
	}
	return allow, "", nil
}

// GetStats returns engine + manager statistics.
func (e *Engine) GetStats() ACLStats {
	return e.manager.GetStats()
}
