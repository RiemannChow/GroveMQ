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

package task

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskStateMachine(t *testing.T) {
	assert.Len(t, AllStates, 9)

	tk := NewTask("task-1", "ctx-1")
	assert.Equal(t, TaskStateSubmitted, tk.Status.State)
	assert.NotEmpty(t, tk.Status.Timestamp)

	require.NoError(t, tk.Transition(TaskStateWorking))
	require.NoError(t, tk.Transition(TaskStateInputRequired))
	assert.True(t, IsInterruptible(tk.Status.State))
	require.NoError(t, tk.Transition(TaskStateWorking))
	require.NoError(t, tk.AddArtifact(Artifact{ArtifactID: "artifact-1", Parts: []Part{{Text: "done"}}}))
	require.NoError(t, tk.Transition(TaskStateCompleted))
	assert.True(t, IsTerminal(tk.Status.State))

	// Terminal is absorbing: no further transition, including self-loop.
	assert.Error(t, tk.Transition(TaskStateCompleted))
	assert.Error(t, tk.Transition(TaskStateWorking))
	assert.False(t, CanTransition(TaskStateCompleted, TaskStateFailed))
}

func TestTaskStateMachineRejectsIllegalTransitions(t *testing.T) {
	assert.False(t, CanTransition(TaskStateSubmitted, TaskStateCompleted))
	assert.False(t, CanTransition(TaskStateUnspecified, TaskStateWorking))
	assert.False(t, CanTransition(TaskStateRejected, TaskStateWorking))

	tk := NewTask("task-1", "")
	require.NoError(t, tk.Transition(TaskStateWorking))
	require.NoError(t, tk.Transition(TaskStateCanceled))
	assert.Error(t, tk.Transition(TaskStateCompleted))
}

func TestJSONRPCEvelopeRoundTrip(t *testing.T) {
	req, err := NewRequest("req-1", MethodMessageSend, map[string]interface{}{
		"message": Message{MessageID: "msg-1", Role: "ROLE_USER", Parts: []Part{{Text: "hello"}}},
	})
	require.NoError(t, err)

	raw, err := json.Marshal(req)
	require.NoError(t, err)
	parsed, err := ParseRequest(raw)
	require.NoError(t, err)
	assert.JSONEq(t, `"req-1"`, string(parsed.ID))
	assert.Equal(t, MethodMessageSend, parsed.Method)

	var params struct {
		Message Message `json:"message"`
	}
	require.NoError(t, parsed.ParamsInto(&params))
	assert.Equal(t, "msg-1", params.Message.MessageID)

	resp, err := NewResponse(parsed.ID, &Task{ID: "task-1", Status: TaskStatus{State: TaskStateSubmitted}})
	require.NoError(t, err)
	raw, err = json.Marshal(resp)
	require.NoError(t, err)
	parsedResp, err := ParseResponse(raw)
	require.NoError(t, err)
	var result Task
	require.NoError(t, parsedResp.ResultInto(&result))
	assert.Equal(t, "task-1", result.ID)

	badID, err := NewRequest(nil, MethodTasksGet, nil)
	assert.Error(t, err)
	assert.Nil(t, badID)
}

func TestJSONRPCValidation(t *testing.T) {
	_, err := ParseRequest([]byte(`{"jsonrpc":"1.0","id":"1","method":"tasks/get"}`))
	assert.Error(t, err)
	_, err = ParseRequest([]byte(`{"jsonrpc":"2.0","method":"tasks/get"}`))
	assert.Error(t, err)
	_, err = ParseResponse([]byte(`{"jsonrpc":"2.0","id":"1"}`))
	assert.Error(t, err)
}

func TestSemanticErrorMapping(t *testing.T) {
	err := TaskNotFound("task-1")
	assert.Equal(t, ErrorTaskNotFound, err.Name)
	assert.Equal(t, -32001, err.Name.RPCCode())
	assert.Equal(t, ReasonImplementationError, err.Name.MQTTReason())
	assert.Equal(t, []byte(ErrorTaskNotFound), err.UserProperties()[PropertyAgentError])

	rpc := err.ToRPC()
	assert.Equal(t, -32001, rpc.Code)
	assert.Contains(t, rpc.Message, "task-1")

	assert.Equal(t, ReasonNotSupported, ErrorVersionNotSupported.MQTTReason())
	assert.Equal(t, ReasonNotAuthorized, ErrorAgentNotAvailable.MQTTReason())
	assert.Equal(t, -32602, ErrorInvalidParams.RPCCode())
}
