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

	"github.com/turtacn/grovemq/pkg/a2a/task"
)

// rejectA2AUnauthorized maps an identity failure to the A2A semantic error and
// the MQTT v5 Not authorized reason code.
func rejectA2AUnauthorized() *MessagePublishResult {
	sem := task.NewError(task.ErrorUnauthorized, "a2a: task request identity is not authorized")
	code := sem.Name.MQTTReason()
	return &MessagePublishResult{
		Allow:          false,
		Reason:         sem.Error(),
		ReasonCode:     &code,
		UserProperties: sem.UserProperties(),
	}
}

// A2APropertyPrincipal is the MQTT 5 user property carrying the authenticated
// principal from the HTTP JSON-RPC gateway into the broker identity check.
const A2APropertyPrincipal = "A2A-Principal"

// A2A request identity policy modes. A2ARequestAuthPolicyOff keeps the legacy
// behaviour where task requests are not identity-checked by A2A hooks.
const (
	A2ARequestAuthPolicyOff            = "off"
	A2ARequestAuthPolicyCaller         = "caller"
	A2ARequestAuthPolicyAgentOrGateway = "agent_or_gateway"
)

// A2ARequestAuthConfig configures task request identity binding.
//
//   - Policy "off" (default) disables the extra check entirely, so deployments
//     that do not enable A2A see no behaviour change.
//   - Policy "caller" requires the publisher's Client ID to be exactly the
//     three-part agent address in the request topic. This is the strictest
//     binding: only the agent itself can submit work to its own request topic.
//   - Policy "agent_or_gateway" accepts either the agent address or a Client ID
//     and user name pair listed in AllowedClients / AllowedUsernames (typically
//     the JSON-RPC gateway service). Requiring both prevents an HTTP caller
//     from impersonating the gateway by guessing its Client ID alone.
type A2ARequestAuthConfig struct {
	Policy           string
	AllowedClients   []string
	AllowedUsernames []string
}

// NormalizeA2ARequestAuthPolicy maps shorthand values to a supported policy.
func NormalizeA2ARequestAuthPolicy(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "off", "disabled", "false":
		return A2ARequestAuthPolicyOff
	case "caller", "strict", "self":
		return A2ARequestAuthPolicyCaller
	case "agent_or_gateway", "gateway", "agent-or-gateway":
		return A2ARequestAuthPolicyAgentOrGateway
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

// A2ARequestAuthHook binds an MQTT client's identity to the Agent address in
// $a2a/v1/{org}/{unit}/{agent}/requests. It is inert for all other topics.
type A2ARequestAuthHook struct {
	BaseHook
	cfg A2ARequestAuthConfig
}

// NewA2ARequestAuthHook creates the hook with the supplied policy.
func NewA2ARequestAuthHook(cfg A2ARequestAuthConfig) *A2ARequestAuthHook {
	cfg.Policy = NormalizeA2ARequestAuthPolicy(cfg.Policy)
	return &A2ARequestAuthHook{cfg: cfg}
}

// Name implements Hook.
func (h *A2ARequestAuthHook) Name() string { return "a2a-request-auth" }

// Priority runs after the A2A task validation hook (priority 0) so malformed
// requests are rejected with the usual semantic error before identity checks.
func (h *A2ARequestAuthHook) Priority() int { return 5 }

// Points reports the single point this hook acts on.
func (h *A2ARequestAuthHook) Points() []string { return []string{OnMessagePublishPoint} }

// OnMessagePublish enforces the configured identity policy for task requests.
func (h *A2ARequestAuthHook) OnMessagePublish(_ context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	allow := &MessagePublishResult{Allow: true}
	if h == nil || h.cfg.Policy == A2ARequestAuthPolicyOff || c == nil {
		return allow, nil
	}
	address, ok := task.ParseRequestTopic(c.Topic)
	if !ok {
		return allow, nil
	}

	if h.cfg.Policy == A2ARequestAuthPolicyCaller {
		if c.ClientID == address {
			return allow, nil
		}
		return rejectA2AUnauthorized(), nil
	}

	if h.cfg.Policy == A2ARequestAuthPolicyAgentOrGateway {
		if c.ClientID == address {
			return allow, nil
		}
		if h.contains(h.cfg.AllowedClients, c.ClientID) && h.contains(h.cfg.AllowedUsernames, c.Username) {
			return allow, nil
		}
		return rejectA2AUnauthorized(), nil
	}

	// Unknown policy names are treated as an explicit fail-closed setting so a
	// configuration typo cannot silently disable identity enforcement.
	return rejectA2AUnauthorized(), nil
}

func (h *A2ARequestAuthHook) contains(list []string, value string) bool {
	if value == "" {
		return false
	}
	for _, candidate := range list {
		if candidate == value {
			return true
		}
	}
	return false
}
