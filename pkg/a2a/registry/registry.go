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
	"encoding/json"
	stderrors "errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	a2ametrics "github.com/turtacn/grovemq/pkg/a2a"
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
	StatusLWT                 = "lwt"
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
	Security      *schema.SecurityConfig // nil disables signature verification
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
	Enabled          bool  `json:"enabled"`
	Registered       int   `json:"registered"`
	Online           int   `json:"online"`
	Offline          int   `json:"offline"`
	ValidationFails  int64 `json:"validation_failures"`
	SignatureFails   int64 `json:"signature_failures"`
	RateLimited      int64 `json:"rate_limited"`
	RejectedBySize   int64 `json:"rejected_by_size"`
	SecurityVerified int64 `json:"security_verified"`
}

// Registry indexes Agent Cards and tracks agent online status.
type Registry struct {
	hooks.BaseHook
	cfg      Config
	security *schema.SecurityVerifier

	mu        sync.RWMutex
	cards     map[string]*Entry
	connected map[string]bool
	updates   map[string][]time.Time
	stats     Stats

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
		cfg:       cfg,
		security:  schema.NewSecurityVerifier(cfg.Security),
		cards:     make(map[string]*Entry),
		connected: make(map[string]bool),
		updates:   make(map[string][]time.Time),
		stats:     Stats{Enabled: cfg.Enabled},
		now:       time.Now,
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
	if c == nil || !r.cfg.Enabled || !strings.HasPrefix(c.Topic, hooks.A2ADiscoveryPrefix) {
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
		a2ametrics.IncRegistration("rejected_size")
		log.Printf("[A2A] rejected card from %s: payload %d bytes exceeds %d", address, len(c.Payload), r.cfg.MaxCardSize)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: agent card exceeds max_card_size"}, nil
	}

	if !r.allowUpdate(address) {
		r.mu.Lock()
		r.stats.RateLimited++
		r.mu.Unlock()
		a2ametrics.IncRegistration("rate_limited")
		log.Printf("[A2A] rate-limited card update from %s", address)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: card update rate limit exceeded"}, nil
	}

	// Empty payload clears the retained card and unregisters the agent.
	if len(c.Payload) == 0 {
		r.Delete(address)
		a2ametrics.IncRegistration("unregistered")
		return allow, nil
	}

	card, err := schema.Parse(c.Payload)
	if err == nil {
		err = schema.Validate(card, r.cfg.Validate)
	}
	if err == nil {
		verified := len(card.Signatures) > 0
		if err = r.security.Verify(ctx, c.Payload, card); err == nil && verified {
			r.mu.Lock()
			r.stats.SecurityVerified++
			r.mu.Unlock()
		}
	}
	if err == nil && r.cfg.Validate == schema.ModeLoose {
		card = schema.Normalize(card, r.cfg.BindingURL)
	}
	if err != nil {
		var secErr *schema.SecurityError
		if stderrors.As(err, &secErr) {
			r.mu.Lock()
			r.stats.SignatureFails++
			r.mu.Unlock()
			log.Printf("[A2A] rejected unsigned or incorrectly signed card from %s: %v", address, err)
		}
		r.mu.Lock()
		r.stats.ValidationFails++
		r.mu.Unlock()
		a2ametrics.CardValidationFailures.Inc()
		a2ametrics.IncRegistration("rejected_invalid")
		log.Printf("[A2A] rejected card from %s: %v", address, err)
		return &hooks.MessagePublishResult{Allow: false, Reason: "a2a: invalid agent card: " + err.Error()}, nil
	}

	parts := strings.Split(address, "/")
	now := r.now()
	r.mu.Lock()
	prev := r.cards[address]
	// A client may connect before it publishes its card. In that case the
	// connection event has no entry to update, so derive the initial state from
	// the live connection set to keep a fresh card online at discovery time.
	status, source := StatusOffline, StatusSourceBroker
	if r.connected[address] {
		status, source = StatusOnline, StatusSourceBroker
	}
	entry := &Entry{
		Address:      address,
		OrgID:        parts[0],
		UnitID:       parts[1],
		AgentID:      parts[2],
		Card:         card,
		Status:       status,
		StatusSource: source,
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

	a2ametrics.IncRegistration("registered")
	log.Printf("[A2A] registered agent %s (%s)", address, card.Name)
	return allow, nil
}

// OnClientConnected marks a registered agent online. The connection set is
// updated first so a card published later starts in the online state.
func (r *Registry) OnClientConnected(_ context.Context, c *hooks.ClientCtx) {
	if c == nil || !r.cfg.Enabled {
		return
	}
	r.mu.Lock()
	r.connected[c.ClientID] = true
	r.mu.Unlock()
	r.setStatus(c.ClientID, StatusOnline, StatusSourceBroker)
}

// OnClientDisconnected records the final state of an agent. A clean DISCONNECT
// is attributed to the agent, a broker-side termination to the broker, and an
// abrupt close to LWT.
func (r *Registry) OnClientDisconnected(_ context.Context, c *hooks.ClientCtx) {
	if c == nil || !r.cfg.Enabled {
		return
	}
	status, source := classifyDisconnect(c.Reason)
	r.mu.Lock()
	delete(r.connected, c.ClientID)
	r.mu.Unlock()
	r.setStatus(c.ClientID, status, source)
}

// classifyDisconnect maps the broker's disconnect reason to the status and
// origin advertised with retained cards. Only an explicit client DISCONNECT is
// an agent-originated offline event; admin/kick actions are broker decisions.
func classifyDisconnect(reason string) (status, source string) {
	lower := strings.ToLower(reason)
	switch {
	case strings.Contains(lower, "client sent disconnect"):
		return StatusOffline, StatusSourceAgent
	case strings.Contains(lower, "disconnected by admin"),
		strings.Contains(lower, "kicked out"),
		strings.HasPrefix(lower, "broker "):
		return StatusOffline, StatusSourceBroker
	default:
		return StatusLWT, StatusSourceLWT
	}
}

// SetStatus applies a broker-originated status decision, for example from a
// health check. It is a no-op when the Registry is disabled or the address is
// not registered.
func (r *Registry) SetStatus(address, status, source string) {
	if !r.cfg.Enabled {
		return
	}
	r.setStatus(address, status, source)
}

func (r *Registry) setStatus(address, status, source string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cards[address]
	if !ok {
		return
	}
	if e.Status == status && e.StatusSource == source {
		return
	}
	e.Status = status
	e.StatusSource = source
	e.UpdatedAt = r.now()
	a2ametrics.IncStatusChange(status, source)
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

// RestoreCard reindexes one retained Agent Card without going through the
// publish hook. It is used at startup to rebuild the in-memory index from
// durable retained messages. A client that is already connected when the card
// is restored is marked online; otherwise the card starts offline/broker.
// Invalid cards and empty payloads are skipped.
func (r *Registry) RestoreCard(address string, payload []byte) bool {
	if r == nil || !r.cfg.Enabled || len(payload) == 0 || !hooks.ValidA2AAgentAddress(address) {
		return false
	}
	card, err := schema.Parse(payload)
	if err == nil {
		err = schema.Validate(card, r.cfg.Validate)
	}
	if err == nil && r.cfg.Validate == schema.ModeLoose {
		card = schema.Normalize(card, r.cfg.BindingURL)
	}
	if err != nil {
		log.Printf("[A2A] skipping invalid retained card for %s during warm-up: %v", address, err)
		return false
	}

	parts := strings.Split(address, "/")
	status, source := StatusOffline, StatusSourceBroker
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connected[address] {
		status, source = StatusOnline, StatusSourceBroker
	}
	_, exists := r.cards[address]
	entry := &Entry{
		Address:      address,
		OrgID:        parts[0],
		UnitID:       parts[1],
		AgentID:      parts[2],
		Card:         card,
		Status:       status,
		StatusSource: source,
		RegisteredAt: r.now(),
		UpdatedAt:    r.now(),
	}
	if prev := r.cards[address]; prev != nil {
		entry.RegisteredAt = prev.RegisteredAt
	}
	r.cards[address] = entry
	if !exists {
		a2ametrics.IncRegistration("restored")
	}
	return true
}

// ExtendedCard returns the x-extended-card extension from a registered Agent
// Card. The extension is authenticated by the fact that only the agent itself
// can publish its discovery topic (Client ID binding), so no separate secret is
// needed. A missing or empty extension returns false.
func (r *Registry) ExtendedCard(address string) (json.RawMessage, bool) {
	if r == nil || !r.cfg.Enabled {
		return nil, false
	}
	e, ok := r.Get(address)
	if !ok || e.Card == nil || len(e.Card.Extra) == 0 {
		return nil, false
	}
	raw, ok := e.Card.Extra["x-extended-card"]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return nil, false
	}
	return append(json.RawMessage(nil), raw...), true
}

// Capabilities reports the registered agent's optional capability flags.
func (r *Registry) Capabilities(address string) (streaming, push, extended bool) {
	if r == nil || !r.cfg.Enabled {
		return false, false, false
	}
	e, ok := r.Get(address)
	if !ok || e.Card == nil {
		return false, false, false
	}
	return e.Card.Capabilities.Streaming, e.Card.Capabilities.PushNotifications, e.Card.Capabilities.ExtendedAgentCard
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
	if !r.cfg.Enabled {
		return false
	}
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
	a2ametrics.SetRegistryAgents(st.Registered, st.Online, st.Offline)
	return st
}

// AnnotateRetained adds a2a-status user properties to a retained card at
// delivery time. It is safe to call with nil properties and for non-A2A topics.
func (r *Registry) AnnotateRetained(topic string, props map[string][]byte) map[string][]byte {
	if !r.cfg.Enabled || !strings.HasPrefix(topic, hooks.A2ADiscoveryPrefix) {
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
