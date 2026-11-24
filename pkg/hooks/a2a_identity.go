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
	"strings"
)

// A2ADiscoveryPrefix is the standard A2A over MQTT discovery topic prefix.
// Agent Cards are published as retained messages below this prefix using the
// form $a2a/v1/discovery/{org_id}/{unit_id}/{agent_id}.
const A2ADiscoveryPrefix = "$a2a/v1/discovery/"

// A2AIdentityHook enforces that a client may only register or unregister its
// own Agent Card. The Client ID must equal the {org_id}/{unit_id}/{agent_id}
// address encoded in the discovery topic (the convention used by EMQX 6.2 and
// the A2A over MQTT specification).
//
// The hook is inert for every other topic, so enabling it has no effect on
// ordinary MQTT traffic. It deliberately does NOT validate the Agent Card
// payload; schema validation belongs to the Registry layer (P1).
type A2AIdentityHook struct {
	BaseHook
}

// NewA2AIdentityHook creates the A2A registration identity hook.
func NewA2AIdentityHook() *A2AIdentityHook { return &A2AIdentityHook{} }

// Name implements Hook.
func (h *A2AIdentityHook) Name() string { return "a2a-identity" }

// Points reports the single point this hook acts on.
func (h *A2AIdentityHook) Points() []string {
	return []string{OnMessagePublishPoint}
}

// OnMessagePublish rejects discovery publishes whose Client ID does not match
// the agent address in the topic. Both registration (non-empty payload) and
// de-registration (empty retained payload) are protected.
func (h *A2AIdentityHook) OnMessagePublish(_ context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	if c == nil || !strings.HasPrefix(c.Topic, A2ADiscoveryPrefix) {
		return &MessagePublishResult{Allow: true}, nil
	}

	address := strings.TrimPrefix(c.Topic, A2ADiscoveryPrefix)
	if !ValidA2AAgentAddress(address) || address != c.ClientID {
		return &MessagePublishResult{
			Allow:  false,
			Reason: "a2a: client id does not match discovery topic address",
		}, nil
	}
	return &MessagePublishResult{Allow: true}, nil
}

// ValidA2AAgentAddress reports whether s is a well-formed
// {org_id}/{unit_id}/{agent_id} address. Each component may contain only
// letters, digits, hyphen, underscore and dot, matching the A2A over MQTT
// topic-naming rules (no '/', '+', '#' or whitespace beyond the separators).
func ValidA2AAgentAddress(s string) bool {
	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, r := range p {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			default:
				return false
			}
		}
	}
	return true
}
