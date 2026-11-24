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

// Package registry implements the A2A Registry: collection of Agent Cards,
// in-memory indexing and online-status tracking. Cards are retained MQTT
// messages published to $a2a/v1/discovery/{org}/{unit}/{agent}; the Registry
// observes them through broker hooks and keeps a queryable index without an
// external database.
package registry

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/hooks"
)

// Default values matching EMQX 6.2 A2A Registry defaults.
const (
	DefaultMaxCardSize        = 65536
	DefaultRegistrationLimit  = 10
	DefaultBindingURL         = "mqtt://localhost:1883"
	StatusOnline              = "online"
	StatusOffline             = "offline"
	StatusSourceBroker        = "broker"
	StatusSourceAgent         = "agent"
	StatusSourceLWT           = "lwt"
	PropertyAgentStatus       = "a2a-status"
	PropertyAgentStatusSource = "a2a-status-source"
)

// Config controls Registry behaviour.
type Config struct {
	Enabled       bool
	Validate      schema.ValidationMode
	MaxCardSize   int
	Registrations int // max card updates per agent per minute; 0 = unlimited
	BindingURL    string
}

// DefaultConfig returns a disabled Registry with safe defaults.
func DefaultConfig() *Config {
	return &Config{
		Enabled:       false,
		Validate:      schema.ModeStrict,
		MaxCardSize:   DefaultMaxCardSize,
		Registrations: DefaultRegistrationLimit,
		BindingURL:    DefaultBindingURL,
	}
}

// Entry is one registered agent in the index.
type Entry struct {
	Address      string            `json:"address"`
	OrgID        string            `json:"org_id"`
	UnitID       string            `json:"unit_id"`
	AgentID      string            `json:"agent_id"`
	Card         *schema.AgentCard `json:"card"`
	Status       string            `json:"status"`
	StatusSource string            `json:"status_source"`
	RegisteredAt time.Time         `json:"registered_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

// Stats is a summary of registry state.
type Stats struct {
	Enabled         bool  `json:"enabled"`
	Registered      int   `json:"registered"`
	Online          int   `json:"online"`
	Offline         int   `json:"offline"`
	ValidationFails int64 `json:"validation_failures"`
	RateLimited     int64 `json:"rate_limited"`
	RejectedBySize  int64 `json:"rejected_by_size"`
}

// Registry indexes Agent Cards and tracks agent online status.
type Registry struct {
	hooks.BaseHook
	cfg Config

	mu      sync.RWMutex
	cards   map[string]*Entry
	updates map[string][]time.Time
	stats   Stats

	// now is overridden in tests for deterministic rate-limit checks.
	now func() time.Time
}

// New creates a Registry. It does not register itself on any hook manager.
func New(cfg Config) *Registry {
	if cfg.MaxCardSize <= 0 {
		cfg.MaxCardSize = DefaultMaxCardSize
	}
	if cfg.BindingURL == "" {
		cfg.BindingURL = DefaultBindingURL
	}
	return &Registry{
		cfg:     cfg,
		cards:   make(map[string]*Entry),
		updates: make(map[string][]time.Time),
		stats:   Stats{Enabled: cfg.Enabled},
		now:     time.Now,
	}
}

// Name implements Hook.
func (r *Registry) Name() string { return "a2a-registry" }

// Priority runs the Registry after the identity hook (priority 0), so a client
// cannot pollute the index with a card it is not allowed to publish.
func (r *Registry) Priority() int { return 10 }

// Points reports the hook points the Registry consumes.
func (r *Registry) Points() []string {
	return []string{
		hooks.OnMessagePublishPoint,
		hooks.OnClientConnectedPoint,
		hooks.OnClientDisconnectedPoint,
	}
}

// OnMessagePublish observes A2A discovery publishes. It validates and indexes
// the card, or removes it when the payload is empty (retained clear).
// Non-discovery topics are ignored.
func (r *Registry) OnMessagePublish(ctx context.Context, c *hooks.MessagePublishCtx) (*hooks.MessagePublishResult, error) {
	allow := &hooks.MessagePublishResult{Allow: true}
	if c == nil || !strings.HasPrefix(c.Topic, hooks.A2ADiscoveryPrefix) {
		return allow, nil
	}

	address := strings.TrimPrefix(c.Topic, hooks.A2ADiscoveryPrefix)
	// The identity hook already rejects malformed addresses; stay defensive.
	if !hooks.ValidA2AAgentAddress(address) || address != c.ClientID {
		return allow, nil
	}

	if len(c.Payload) > r.cfg.MaxCardSize {
		r.mu.Lock()
		r.stats.RejectedBySize++
		r.mu.Unlock()
		log.Printf("[A2A] rejected card from %s: payload %d bytes exceeds %d", address, len(c.Payload), r.cfg.MaxCardSize)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: agent card exceeds max_card_size"}, nil
	}

	if !r.allowUpdate(address) {
		r.mu.Lock()
		r.stats.RateLimited++
		r.mu.Unlock()
		log.Printf("[A2A] rate-limited card update from %s", address)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: card update rate limit exceeded"}, nil
	}

	// Empty payload clears the retained card and unregisters the agent.
	if len(c.Payload) == 0 {
		r.Delete(address)
		return allow, nil
	}

	card, err := schema.ValidateAndNormalize(c.Payload, r.cfg.Validate, r.cfg.BindingURL)
	if err != nil {
		r.mu.Lock()
		r.stats.ValidationFails++
		r.mu.Unlock()
		log.Printf("[A2A] rejected card from %s: %v", address, err)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: invalid agent card: " + err.Error()}, nil
	}

	parts := strings.Split(address, "/")
	now := r.now()
	r.mu.Lock()
	prev := r.cards[address]
	entry := &Entry{
		Address:      address,
		OrgID:        parts[0],
		UnitID:       parts[1],
		AgentID:      parts[2],
		Card:         card,
		Status:       StatusOffline,
		StatusSource: StatusSourceBroker,
		RegisteredAt: now,
		UpdatedAt:    now,
	}
	if prev != nil {
		entry.RegisteredAt = prev.RegisteredAt
		entry.Status = prev.Status
		entry.StatusSource = prev.StatusSource
	}
	r.cards[address] = entry
	r.mu.Unlock()

	log.Printf("[A2A] registered agent %s (%s)", address, card.Name)
	return allow, nil
}

// OnClientConnected marks a registered agent online.
func (r *Registry) OnClientConnected(_ context.Context, c *hooks.ClientCtx) {
	if c == nil {
		return
	}
	r.setStatus(c.ClientID, StatusOnline, StatusSourceBroker)
}

// OnClientDisconnected marks a registered agent offline. A clean DISCONNECT
// is attributed to the agent; an abrupt close is attributed to LWT.
func (r *Registry) OnClientDisconnected(_ context.Context, c *hooks.ClientCtx) {
	if c == nil {
		return
	}
	source := StatusSourceLWT
	if strings.Contains(strings.ToLower(c.Reason), "disconnect") {
		source = StatusSourceAgent
	}
	r.setStatus(c.ClientID, StatusOffline, source)
}

func (r *Registry) setStatus(address, status, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cards[address]
	if !ok {
		return
	}
	e.Status = status
	e.StatusSource = source
	e.UpdatedAt = r.now()
}

// allowUpdate implements a per-agent registration rate limit (sliding window).
func (r *Registry) allowUpdate(address string) bool {
	if r.cfg.Registrations <= 0 {
		return true
	}
	now := r.now()
	cutoff := now.Add(-time.Minute)
	times := r.updates[address][:0]
	for _, t := range r.updates[address] {
		if t.After(cutoff) {
			times = append(times, t)
		}
	}
	if len(times) >= r.cfg.Registrations {
		r.updates[address] = times
		return false
	}
	r.updates[address] = append(times, now)
	return true
}

// Get returns a copy of one registry entry.
func (r *Registry) Get(address string) (*Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.cards[address]
	if !ok {
		return nil, false
	}
	copyEntry := *e
	copyEntry.Card = e.Card
	return &copyEntry, true
}

// Delete unregisters an agent from the index.
func (r *Registry) Delete(address string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.cards[address]; !ok {
		return false
	}
	delete(r.cards, address)
	delete(r.updates, address)
	log.Printf("[A2A] unregistered agent %s", address)
	return true
}

// List returns a snapshot of all registered agents, sorted by address.
func (r *Registry) List() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.cards))
	for _, e := range r.cards {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out
}

// Stats returns a snapshot of registry counters.
func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st := r.stats
	for _, e := range r.cards {
		if e.Status == StatusOnline {
			st.Online++
		} else {
			st.Offline++
		}
	}
	st.Registered = len(r.cards)
	return st
}

// AnnotateRetained adds a2a-status user properties to a retained card at
// delivery time. It is safe to call with nil properties and for non-A2A topics.
func (r *Registry) AnnotateRetained(topic string, props map[string][]byte) map[string][]byte {
	if !strings.HasPrefix(topic, hooks.A2ADiscoveryPrefix) {
		return props
	}
	address := strings.TrimPrefix(topic, hooks.A2ADiscoveryPrefix)
	r.mu.RLock()
	e, ok := r.cards[address]
	status, source := StatusOffline, StatusSourceBroker
	if ok {
		status, source = e.Status, e.StatusSource
	}
	r.mu.RUnlock()

	out := make(map[string][]byte, len(props)+2)
	for k, v := range props {
		out[k] = v
	}
	out[PropertyAgentStatus] = []byte(status)
	out[PropertyAgentStatusSource] = []byte(source)
	return out
}
