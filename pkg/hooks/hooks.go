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

// Package hooks provides a general, extensible hook-chain framework in the
// spirit of EMQX's hook points (on_client_connect, on_message_publish, ...).
//
// Historically grovemq only invoked the rule/integration engines ad-hoc on the
// PUBLISH path. This package introduces a typed hook chain so that new
// cross-cutting capabilities — auditing, rate limiting, bridging/forwarding,
// custom metrics, message rewriting, ... — can be added by simply registering a
// hook, without editing the broker's core connection loop.
//
// Two kinds of hooks exist:
//
//   - Decision hooks (OnClientConnect, OnMessagePublish) run SYNCHRONOUSLY in the
//     caller's goroutine because their result can reject a connection or publish.
//     Any hook returning Allow=false rejects; the first rejection wins.
//
//   - Notification hooks (everything else) are dispatched ASYNCHRONOUSLY through a
//     buffered queue consumed by a single worker goroutine. This bounds
//     concurrency (no per-message goroutine explosion) and keeps the hot path
//     non-blocking; if the queue is full the event is dropped and counted.
//
// Every hook embeds BaseHook and overrides only the points it cares about.
package hooks

import (
	"context"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Hook-point names (EMQX-style). Used for introspection via Manager.List.
const (
	OnClientConnectPoint       = "on_client_connect"
	OnClientConnectedPoint     = "on_client_connected"
	OnClientDisconnectedPoint  = "on_client_disconnected"
	OnSessionSubscribedPoint   = "on_session_subscribed"
	OnSessionUnsubscribedPoint = "on_session_unsubscribed"
	OnMessagePublishPoint      = "on_message_publish"
	OnMessageDeliveredPoint    = "on_message_delivered"
	OnMessageAckedPoint        = "on_message_acked"
	OnMessageDroppedPoint      = "on_message_dropped"
)

// ClientConnectCtx is passed to OnClientConnect decision hooks. It is read-only;
// the broker fills it at the call site.
type ClientConnectCtx struct {
	ClientID   string
	Username   string
	RemoteAddr string
	KeepAlive  int
	CleanStart bool
	ProtoVer   int
}

// ClientCtx is passed to connection-lifecycle notification hooks.
type ClientCtx struct {
	ClientID   string
	Username   string
	RemoteAddr string
	Reason     string
}

// SubscriptionCtx is passed to session subscribe/unsubscribe notification hooks.
type SubscriptionCtx struct {
	ClientID string
	Username string
	Topic    string
	QoS      int
}

// MessagePublishCtx is passed to OnMessagePublish decision hooks. It is read-only.
type MessagePublishCtx struct {
	ClientID        string
	Username        string
	RemoteAddr      string
	Topic           string
	QoS             int
	Retain          bool
	Payload         []byte
	UserProperties  map[string][]byte
	ResponseTopic   string
	CorrelationData []byte
}

// MessageCtx is passed to message delivery/ack notification hooks.
type MessageCtx struct {
	ClientID string
	Username string
	Topic    string
	QoS      int
	Payload  []byte
	Size     int
}

// MessageDroppedCtx is passed to the message-dropped notification hook.
type MessageDroppedCtx struct {
	ClientID string
	Username string
	Topic    string
	QoS      int
	Reason   string
}

// Decision is returned by the synchronous OnClientConnect hook.
type Decision struct {
	Allow  bool
	Reason string
}

// MessagePublishResult is returned by the synchronous OnMessagePublish hook.
type MessagePublishResult struct {
	Allow  bool
	Reason string
}

// Hook is the minimal identity every hook implements.
type Hook interface {
	Name() string
	Priority() int
}

// PointReporter lets a hook declare which points it actually implements; it is
// used purely for introspection (Manager.List). Hooks that do not implement it
// are reported with the full point set.
type PointReporter interface {
	Points() []string
}

// FullHook is the complete hook contract. Concrete hooks embed BaseHook and
// override only the points they need. Register therefore accepts anything that
// satisfies FullHook (the embedded BaseHook satisfies the rest).
type FullHook interface {
	Hook
	OnClientConnect(ctx context.Context, c *ClientConnectCtx) (*Decision, error)
	OnClientConnected(ctx context.Context, c *ClientCtx)
	OnClientDisconnected(ctx context.Context, c *ClientCtx)
	OnSessionSubscribed(ctx context.Context, c *SubscriptionCtx)
	OnSessionUnsubscribed(ctx context.Context, c *SubscriptionCtx)
	OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error)
	OnMessageDelivered(ctx context.Context, c *MessageCtx)
	OnMessageAcked(ctx context.Context, c *MessageCtx)
	OnMessageDropped(ctx context.Context, c *MessageDroppedCtx)
}

// BaseHook provides no-op default implementations of every hook point so that
// concrete hooks only override what they need. A default (base) hook always
// allows connections/publishes and performs no notification side effects.
type BaseHook struct{}

func (BaseHook) Name() string  { return "base" }
func (BaseHook) Priority() int { return 0 }
func (BaseHook) Points() []string {
	return []string{
		OnClientConnectPoint, OnClientConnectedPoint, OnClientDisconnectedPoint,
		OnSessionSubscribedPoint, OnSessionUnsubscribedPoint, OnMessagePublishPoint,
		OnMessageDeliveredPoint, OnMessageAckedPoint, OnMessageDroppedPoint,
	}
}
func (BaseHook) OnClientConnect(ctx context.Context, c *ClientConnectCtx) (*Decision, error) {
	return &Decision{Allow: true}, nil
}
func (BaseHook) OnClientConnected(ctx context.Context, c *ClientCtx)           {}
func (BaseHook) OnClientDisconnected(ctx context.Context, c *ClientCtx)        {}
func (BaseHook) OnSessionSubscribed(ctx context.Context, c *SubscriptionCtx)   {}
func (BaseHook) OnSessionUnsubscribed(ctx context.Context, c *SubscriptionCtx) {}
func (BaseHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	return &MessagePublishResult{Allow: true}, nil
}
func (BaseHook) OnMessageDelivered(ctx context.Context, c *MessageCtx)      {}
func (BaseHook) OnMessageAcked(ctx context.Context, c *MessageCtx)          {}
func (BaseHook) OnMessageDropped(ctx context.Context, c *MessageDroppedCtx) {}

// HookInfo describes a registered hook for introspection.
type HookInfo struct {
	Name     string   `json:"name"`
	Priority int      `json:"priority"`
	Points   []string `json:"points"`
}

// eventCall is a queued notification dispatched to the worker goroutine.
type eventCall struct {
	hook  FullHook
	point string
	run   func(h FullHook)
}

const (
	// eventQueueSize bounds the notification queue. When full, events are dropped.
	eventQueueSize = 1024
	// hookTimeout bounds a single hook invocation inside the worker so a slow or
	// stuck hook cannot stall the whole notification pipeline.
	hookTimeout = 100 * time.Millisecond
)

// Manager owns the registered hooks and dispatches both synchronous decision
// hooks and asynchronous notification hooks.
type Manager struct {
	mu      sync.RWMutex
	hooks   []FullHook
	events  chan eventCall
	dropped uint64
	stop    chan struct{}
	stopped sync.Once
	wg      sync.WaitGroup
}

// NewManager creates a hook manager and starts its notification worker.
func NewManager() *Manager {
	m := &Manager{
		events: make(chan eventCall, eventQueueSize),
		stop:   make(chan struct{}),
	}
	m.wg.Add(1)
	go m.worker()
	return m
}

// Register adds (or replaces, by name) a hook and re-sorts by priority.
func (m *Manager) Register(h FullHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.hooks {
		if existing.Name() == h.Name() {
			m.hooks[i] = h
			m.sortLocked()
			return
		}
	}
	m.hooks = append(m.hooks, h)
	m.sortLocked()
}

// Unregister removes a hook by name.
func (m *Manager) Unregister(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, h := range m.hooks {
		if h.Name() == name {
			m.hooks = append(m.hooks[:i], m.hooks[i+1:]...)
			return
		}
	}
}

// Get returns a registered hook by name, if present.
func (m *Manager) Get(name string) (FullHook, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, h := range m.hooks {
		if h.Name() == name {
			return h, true
		}
	}
	return nil, false
}

// List returns introspection info for all registered hooks, in priority order.
func (m *Manager) List() []HookInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]HookInfo, 0, len(m.hooks))
	for _, h := range m.hooks {
		var points []string
		if pr, ok := h.(PointReporter); ok {
			points = pr.Points()
		}
		if points == nil {
			points = BaseHook{}.Points()
		}
		out = append(out, HookInfo{Name: h.Name(), Priority: h.Priority(), Points: points})
	}
	return out
}

// DroppedCount returns how many notification events were dropped because the
// queue was full. Useful for observability/alerting.
func (m *Manager) DroppedCount() uint64 {
	return atomic.LoadUint64(&m.dropped)
}

// Stop terminates the notification worker, draining any pending events first.
func (m *Manager) Stop() {
	m.stopped.Do(func() {
		close(m.stop)
		m.wg.Wait()
	})
}

func (m *Manager) sortLocked() {
	sort.SliceStable(m.hooks, func(i, j int) bool {
		return m.hooks[i].Priority() < m.hooks[j].Priority()
	})
}

func (m *Manager) snapshot() []FullHook {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]FullHook(nil), m.hooks...)
}

// enqueue pushes one notification event per registered hook. If the queue is
// full the event is dropped and the dropped counter is incremented.
func (m *Manager) enqueue(point string, run func(h FullHook)) {
	for _, h := range m.snapshot() {
		ev := eventCall{hook: h, point: point, run: run}
		select {
		case m.events <- ev:
		default:
			atomic.AddUint64(&m.dropped, 1)
		}
	}
}

// worker consumes notification events sequentially and dispatches them to hooks
// with a per-hook timeout. On stop it drains remaining events before exiting.
func (m *Manager) worker() {
	defer m.wg.Done()
	for {
		select {
		case ev := <-m.events:
			m.dispatch(ev.hook, ev.point, ev.run)
		case <-m.stop:
			for {
				select {
				case ev := <-m.events:
					m.dispatch(ev.hook, ev.point, ev.run)
				default:
					return
				}
			}
		}
	}
}

// dispatch runs a single hook invocation, bounding it with hookTimeout so a stuck
// hook cannot stall the pipeline. Concurrency stays at ~1 (the worker waits for
// each), so this does not reintroduce per-message goroutine explosion.
func (m *Manager) dispatch(hook FullHook, point string, run func(FullHook)) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(hook)
	}()
	select {
	case <-done:
	case <-time.After(hookTimeout):
		log.Printf("[HOOKS] hook %q point %q exceeded %s timeout", hook.Name(), point, hookTimeout)
	}
}

// OnClientConnect is the synchronous decision hook for CONNECT. The first hook
// that returns Allow=false rejects the connection; its reason is returned. A nil
// or Allow=true result from every hook means the connection is allowed.
func (m *Manager) OnClientConnect(ctx context.Context, c *ClientConnectCtx) (*Decision, error) {
	for _, h := range m.snapshot() {
		dec, err := h.OnClientConnect(ctx, c)
		if err != nil {
			return nil, err
		}
		if dec != nil && !dec.Allow {
			reason := dec.Reason
			if reason == "" {
				reason = "rejected by hook " + h.Name()
			}
			return &Decision{Allow: false, Reason: reason}, nil
		}
	}
	return &Decision{Allow: true}, nil
}

// OnMessagePublish is the synchronous decision hook for PUBLISH. The first hook
// that returns Allow=false rejects the publish; its reason is returned.
func (m *Manager) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	for _, h := range m.snapshot() {
		res, err := h.OnMessagePublish(ctx, c)
		if err != nil {
			return nil, err
		}
		if res != nil && !res.Allow {
			reason := res.Reason
			if reason == "" {
				reason = "rejected by hook " + h.Name()
			}
			return &MessagePublishResult{Allow: false, Reason: reason}, nil
		}
	}
	return &MessagePublishResult{Allow: true}, nil
}

// ClientConnected notifies hooks that a client completed CONNECT.
func (m *Manager) ClientConnected(ctx context.Context, c *ClientCtx) {
	m.enqueue(OnClientConnectedPoint, func(h FullHook) { h.OnClientConnected(ctx, c) })
}

// ClientDisconnected notifies hooks that a client disconnected.
func (m *Manager) ClientDisconnected(ctx context.Context, c *ClientCtx) {
	m.enqueue(OnClientDisconnectedPoint, func(h FullHook) { h.OnClientDisconnected(ctx, c) })
}

// SessionSubscribed notifies hooks that a client subscribed to a topic.
func (m *Manager) SessionSubscribed(ctx context.Context, c *SubscriptionCtx) {
	m.enqueue(OnSessionSubscribedPoint, func(h FullHook) { h.OnSessionSubscribed(ctx, c) })
}

// SessionUnsubscribed notifies hooks that a client unsubscribed from a topic.
func (m *Manager) SessionUnsubscribed(ctx context.Context, c *SubscriptionCtx) {
	m.enqueue(OnSessionUnsubscribedPoint, func(h FullHook) { h.OnSessionUnsubscribed(ctx, c) })
}

// MessageDelivered notifies hooks that a message was delivered to a subscriber.
func (m *Manager) MessageDelivered(ctx context.Context, c *MessageCtx) {
	m.enqueue(OnMessageDeliveredPoint, func(h FullHook) { h.OnMessageDelivered(ctx, c) })
}

// MessageAcked notifies hooks that a delivered message was acknowledged.
func (m *Manager) MessageAcked(ctx context.Context, c *MessageCtx) {
	m.enqueue(OnMessageAckedPoint, func(h FullHook) { h.OnMessageAcked(ctx, c) })
}

// MessageDropped notifies hooks that a message was dropped (e.g. no subscribers).
func (m *Manager) MessageDropped(ctx context.Context, c *MessageDroppedCtx) {
	m.enqueue(OnMessageDroppedPoint, func(h FullHook) { h.OnMessageDropped(ctx, c) })
}
