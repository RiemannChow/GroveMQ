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

func TestA2ATaskHookValidRequest(t *testing.T) {
	h := NewA2ATaskHook()
	res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{
		Topic:          "$a2a/v1/org/unit/agent/requests",
		Payload:        []byte(`{"jsonrpc":"2.0","id":"1","method":"message/send","params":{}}`),
		UserProperties: map[string][]byte{"A2A-Version": []byte("1.0")},
	})
	require.NoError(t, err)
	assert.True(t, res.Allow)
}

func TestA2ATaskHookSemanticErrors(t *testing.T) {
	h := NewA2ATaskHook()
	tests := []struct {
		name   string
		topic  string
		body   string
		props  map[string][]byte
		wantID string
		reason byte
	}{
		{
			name:   "invalid request",
			topic:  "$a2a/v1/org/unit/agent/requests",
			body:   `{`,
			wantID: string(task.ErrorInvalidRequest),
		},
		{
			name:   "unsupported version",
			topic:  "$a2a/v1/org/unit/agent/requests",
			body:   `{"jsonrpc":"2.0","id":"1","method":"tasks/get"}`,
			props:  map[string][]byte{"A2A-Version": []byte("0.9")},
			wantID: string(task.ErrorVersionNotSupported),
		},
		{
			name:   "unsupported method",
			topic:  "$a2a/v1/org/unit/agent/requests",
			body:   `{"jsonrpc":"2.0","id":"1","method":"unknown/op"}`,
			wantID: string(task.ErrorMethodNotFound),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{
				Topic:          tt.topic,
				Payload:        []byte(tt.body),
				UserProperties: tt.props,
			})
			require.NoError(t, err)
			require.False(t, res.Allow)
			require.NotNil(t, res.ReasonCode)
			assert.Equal(t, []byte(tt.wantID), res.UserProperties[task.PropertyAgentError])
			assert.Contains(t, res.Reason, string(tt.wantID))
		})
	}
}

func TestA2ATaskHookInertForOtherTopics(t *testing.T) {
	res, err := NewA2ATaskHook().OnMessagePublish(context.Background(), &MessagePublishCtx{
		Topic:   "sensors/line-7/temperature",
		Payload: []byte("not json"),
	})
	require.NoError(t, err)
	assert.True(t, res.Allow)
}
