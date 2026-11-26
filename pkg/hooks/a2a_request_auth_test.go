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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

func TestA2ARequestAuthPolicyOffIsInert(t *testing.T) {
	h := NewA2ARequestAuthHook(A2ARequestAuthConfig{Policy: A2ARequestAuthPolicyOff})
	res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{
		ClientID: "attacker",
		Topic:    "$a2a/v1/org/unit/agent/requests",
		Payload:  []byte(`{"jsonrpc":"2.0","id":"1","method":"message/send"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.Allow)
}

func TestA2ARequestAuthCallerBinding(t *testing.T) {
	h := NewA2ARequestAuthHook(A2ARequestAuthConfig{Policy: A2ARequestAuthPolicyCaller})
	ctx := context.Background()

	tests := []struct {
		name     string
		topic    string
		clientID string
		want     bool
	}{
		{"agent publishes to own requests", "$a2a/v1/org/unit/agent/requests", "org/unit/agent", true},
		{"other client blocked", "$a2a/v1/org/unit/agent/requests", "org/unit/other", false},
		{"gateway client blocked", "$a2a/v1/org/unit/agent/requests", "grovemq-gateway-1", false},
		{"non-request topic unaffected", "$a2a/v1/discovery/org/unit/agent", "anyone", true},
		{"malformed address unaffected", "$a2a/v1/org/agent/requests", "anyone", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.OnMessagePublish(ctx, &MessagePublishCtx{ClientID: tc.clientID, Topic: tc.topic})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tc.want, res.Allow)
			if !tc.want {
				assert.Equal(t, task.ErrorUnauthorized.MQTTReason(), *res.ReasonCode)
				assert.Equal(t, []byte(task.ErrorUnauthorized), res.UserProperties[task.PropertyAgentError])
			}
		})
	}
}

func TestA2ARequestAuthAgentOrGateway(t *testing.T) {
	h := NewA2ARequestAuthHook(A2ARequestAuthConfig{
		Policy:           A2ARequestAuthPolicyAgentOrGateway,
		AllowedClients:   []string{"grovemq-gateway"},
		AllowedUsernames: []string{"sdk-service"},
	})
	ctx := context.Background()
	topic := "$a2a/v1/org/unit/agent/requests"

	cases := []struct {
		name     string
		clientID string
		username string
		want     bool
	}{
		{"agent itself", "org/unit/agent", "", true},
		{"gateway with principal", "grovemq-gateway", "sdk-service", true},
		{"gateway without principal", "grovemq-gateway", "", false},
		{"gateway with wrong principal", "grovemq-gateway", "attacker", false},
		{"random client with right principal", "attacker", "sdk-service", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.OnMessagePublish(ctx, &MessagePublishCtx{
				ClientID: tc.clientID, Username: tc.username, Topic: topic,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tc.want, res.Allow)
		})
	}
}

func TestA2ARequestAuthUnknownPolicyFailsClosed(t *testing.T) {
	h := NewA2ARequestAuthHook(A2ARequestAuthConfig{Policy: "typo"})
	res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{
		ClientID: "org/unit/agent", Topic: "$a2a/v1/org/unit/agent/requests",
	})
	require.NoError(t, err)
	assert.False(t, res.Allow)
}

func TestNormalizeA2ARequestAuthPolicy(t *testing.T) {
	assert.Equal(t, A2ARequestAuthPolicyOff, NormalizeA2ARequestAuthPolicy(""))
	assert.Equal(t, A2ARequestAuthPolicyOff, NormalizeA2ARequestAuthPolicy("disabled"))
	assert.Equal(t, A2ARequestAuthPolicyCaller, NormalizeA2ARequestAuthPolicy("strict"))
	assert.Equal(t, A2ARequestAuthPolicyAgentOrGateway, NormalizeA2ARequestAuthPolicy("gateway"))
	assert.Equal(t, "custom", NormalizeA2ARequestAuthPolicy("custom"))
}
