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

package hooks

import (
	"context"
	"log"
	"sync"
)

// AuditHook records broker lifecycle and message events to the standard logger,
// prefixed with [AUDIT]. It is OPT-IN per hook point and OFF by default, so it
// has zero behaviour until explicitly enabled. This matters because per-message
// auditing can produce a log storm under load; enable only the points you need.
type AuditHook struct {
	BaseHook
	mu      sync.Mutex
	enabled map[string]bool
	logger  *log.Logger
}

// NewAuditHook returns an audit hook with every point disabled.
func NewAuditHook() *AuditHook {
	return &AuditHook{
		enabled: make(map[string]bool),
		logger:  log.Default(),
	}
}

// SetLogger overrides the logger used for audit records.
func (h *AuditHook) SetLogger(l *log.Logger) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if l != nil {
		h.logger = l
	}
}

// Enable turns on auditing for a specific hook point (e.g. OnClientConnectedPoint).
func (h *AuditHook) Enable(point string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enabled[point] = true
}

// Disable turns off auditing for a specific hook point.
func (h *AuditHook) Disable(point string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enabled[point] = false
}

// IsEnabled reports whether a hook point is being audited.
func (h *AuditHook) IsEnabled(point string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.enabled[point]
}

// Points returns the currently-enabled audit points (for introspection).
func (h *AuditHook) Points() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	pts := make([]string, 0, len(h.enabled))
	for p, on := range h.enabled {
		if on {
			pts = append(pts, p)
		}
	}
	return pts
}

// Name implements Hook.
func (h *AuditHook) Name() string { return "audit" }

func (h *AuditHook) logf(format string, args ...any) {
	h.mu.Lock()
	l := h.logger
	h.mu.Unlock()
	if l != nil {
		l.Printf(format, args...)
	}
}

// OnClientConnected records a successful connection.
func (h *AuditHook) OnClientConnected(ctx context.Context, c *ClientCtx) {
	if h.IsEnabled(OnClientConnectedPoint) {
		h.logf("[AUDIT] CONNECT client=%s user=%s remote=%s", c.ClientID, c.Username, c.RemoteAddr)
	}
}

// OnClientDisconnected records a disconnection, including the reason when known.
func (h *AuditHook) OnClientDisconnected(ctx context.Context, c *ClientCtx) {
	if h.IsEnabled(OnClientDisconnectedPoint) {
		h.logf("[AUDIT] DISCONNECT client=%s user=%s remote=%s reason=%s", c.ClientID, c.Username, c.RemoteAddr, c.Reason)
	}
}

// OnSessionSubscribed records a new subscription.
func (h *AuditHook) OnSessionSubscribed(ctx context.Context, c *SubscriptionCtx) {
	if h.IsEnabled(OnSessionSubscribedPoint) {
		h.logf("[AUDIT] SUBSCRIBE client=%s user=%s topic=%s qos=%d", c.ClientID, c.Username, c.Topic, c.QoS)
	}
}

// OnSessionUnsubscribed records an unsubscription.
func (h *AuditHook) OnSessionUnsubscribed(ctx context.Context, c *SubscriptionCtx) {
	if h.IsEnabled(OnSessionUnsubscribedPoint) {
		h.logf("[AUDIT] UNSUBSCRIBE client=%s user=%s topic=%s", c.ClientID, c.Username, c.Topic)
	}
}

// OnMessagePublish records a publish (topic + size). Payload is intentionally
// not logged by default to avoid a log storm; enable OnMessagePublishPoint and
// call SetLogPayload if you truly need payloads.
func (h *AuditHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	if h.IsEnabled(OnMessagePublishPoint) {
		h.logf("[AUDIT] PUBLISH client=%s user=%s topic=%s qos=%d size=%d retain=%t",
			c.ClientID, c.Username, c.Topic, c.QoS, len(c.Payload), c.Retain)
	}
	return &MessagePublishResult{Allow: true}, nil
}

// OnMessageDropped records a dropped message and its reason.
func (h *AuditHook) OnMessageDropped(ctx context.Context, c *MessageDroppedCtx) {
	if h.IsEnabled(OnMessageDroppedPoint) {
		h.logf("[AUDIT] DROPPED client=%s user=%s topic=%s qos=%d reason=%s",
			c.ClientID, c.Username, c.Topic, c.QoS, c.Reason)
	}
}
