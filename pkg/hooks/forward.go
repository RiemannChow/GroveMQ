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
	"sync"

	"github.com/turtacn/grovemq/pkg/retainer"
)

// PublishFunc is the broker-provided primitive used by the forward hook to
// re-publish a message onto a target topic. It MUST route the message through the
// broker's RAW delivery path (e.g. RouteToLocalSubscribersWithQoS) and must NOT
// re-trigger OnMessagePublish, otherwise a forwarded message would re-enter the
// hook chain and could loop forever.
type PublishFunc func(topic string, qos byte, payload []byte, retain bool) error

// ForwardRule describes one bridging/forwarding mapping: messages published to
// TopicFilter (an MQTT filter with +/# wildcards) are republished to TargetTopic
// with the given QoS and Retain flags.
type ForwardRule struct {
	TopicFilter string
	TargetTopic string
	QoS         byte
	Retain      bool
}

// ForwardHook bridges/forwards messages: any publish matching one of its rules
// is republished onto the rule's target topic. It never rejects a publish — it
// only adds a side effect. It is INERT until at least one rule is added.
//
// IMPORTANT: avoid rule cycles (A -> B and B -> A) since the injected PublishFunc
// does not re-trigger OnMessagePublish, a direct A<->B cycle would still forward
// repeatedly per original publish only once per matching rule; but nested chains
// of rules should be configured with care.
type ForwardHook struct {
	BaseHook
	mu      sync.Mutex
	publish PublishFunc
	rules   []ForwardRule
}

// NewForwardHook creates a forward hook with no rules (completely inert). The
// publish callback is the broker's raw routing primitive.
func NewForwardHook(publish PublishFunc) *ForwardHook {
	return &ForwardHook{
		publish: publish,
		rules:   make([]ForwardRule, 0),
	}
}

// AddRule appends a forwarding rule.
func (h *ForwardHook) AddRule(rule ForwardRule) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules = append(h.rules, rule)
}

// RemoveRule removes all rules whose TopicFilter equals the given filter.
func (h *ForwardHook) RemoveRule(filter string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	kept := h.rules[:0]
	for _, r := range h.rules {
		if r.TopicFilter != filter {
			kept = append(kept, r)
		}
	}
	h.rules = kept
}

// ClearRules removes all forwarding rules.
func (h *ForwardHook) ClearRules() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.rules = h.rules[:0]
}

// Rules returns a snapshot of the current forwarding rules.
func (h *ForwardHook) Rules() []ForwardRule {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]ForwardRule, len(h.rules))
	copy(out, h.rules)
	return out
}

// Name implements Hook.
func (h *ForwardHook) Name() string { return "forward" }

// Points reports the point this hook acts on.
func (h *ForwardHook) Points() []string {
	return []string{OnMessagePublishPoint}
}

// OnMessagePublish is a decision hook that does NOT reject; it republishes the
// message to every matching rule's target topic (as a side effect) and then
// allows the original publish to proceed normally.
func (h *ForwardHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	h.mu.Lock()
	publish := h.publish
	rules := make([]ForwardRule, len(h.rules))
	copy(rules, h.rules)
	h.mu.Unlock()

	if publish == nil {
		return &MessagePublishResult{Allow: true}, nil
	}

	payload := append([]byte(nil), c.Payload...)
	for _, r := range rules {
		if r.TopicFilter == "" || r.TargetTopic == "" {
			continue
		}
		if retainer.MatchTopicFilter(c.Topic, r.TopicFilter) {
			_ = publish(r.TargetTopic, r.QoS, payload, r.Retain)
		}
	}
	return &MessagePublishResult{Allow: true}, nil
}
