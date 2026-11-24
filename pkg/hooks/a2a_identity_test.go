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
)

func TestA2AIdentityHook(t *testing.T) {
	h := NewA2AIdentityHook()
	ctx := context.Background()

	tests := []struct {
		name     string
		topic    string
		clientID string
		want     bool
	}{
		{
			name:     "matching registration",
			topic:    "$a2a/v1/discovery/com.example/factory-a/iot-ops-agent-001",
			clientID: "com.example/factory-a/iot-ops-agent-001",
			want:     true,
		},
		{
			name:     "matching de-registration with empty payload",
			topic:    "$a2a/v1/discovery/com.example/factory-a/iot-ops-agent-001",
			clientID: "com.example/factory-a/iot-ops-agent-001",
			want:     true,
		},
		{
			name:     "impersonating another agent",
			topic:    "$a2a/v1/discovery/com.example/factory-a/victim-agent",
			clientID: "com.example/factory-a/attacker-agent",
			want:     false,
		},
		{
			name:     "malformed topic extra level",
			topic:    "$a2a/v1/discovery/com.example/factory-a/agent/extra",
			clientID: "com.example/factory-a/agent/extra",
			want:     false,
		},
		{
			name:     "malformed topic wildcard",
			topic:    "$a2a/v1/discovery/com.example/+/agent",
			clientID: "com.example/+/agent",
			want:     false,
		},
		{
			name:     "non-a2a topic is unaffected",
			topic:    "sensors/room1/temperature",
			clientID: "device-001",
			want:     true,
		},
		{
			name:     "system topic is unaffected by identity hook",
			topic:    "$SYS/broker/uptime",
			clientID: "monitor",
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := h.OnMessagePublish(ctx, &MessagePublishCtx{
				Topic:    tt.topic,
				ClientID: tt.clientID,
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, tt.want, res.Allow)
			if !tt.want {
				assert.NotEmpty(t, res.Reason)
			}
		})
	}
}

func TestValidA2AAgentAddress(t *testing.T) {
	valid := []string{
		"com.example/factory-a/iot-ops-agent-001",
		"org/unit/agent_1.2",
		"a/b/c",
	}
	for _, s := range valid {
		assert.True(t, ValidA2AAgentAddress(s), s)
	}

	invalid := []string{
		"",
		"org/agent",
		"org/unit/agent/extra",
		"org/unit/agent with space",
		"org/unit/agent#",
		"org/unit/+",
		"org//agent",
	}
	for _, s := range invalid {
		assert.False(t, ValidA2AAgentAddress(s), s)
	}
}

func TestA2AIdentityHookRegistration(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	m.Register(NewA2AIdentityHook())

	_, ok := m.Get("a2a-identity")
	require.True(t, ok)

	infos := m.List()
	var found *HookInfo
	for i := range infos {
		if infos[i].Name == "a2a-identity" {
			found = &infos[i]
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, []string{OnMessagePublishPoint}, found.Points)
}
