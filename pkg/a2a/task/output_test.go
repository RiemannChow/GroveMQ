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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateTaskResultRejectsOutputInMessage(t *testing.T) {
	task := NewTask("task-output", "ctx-1")
	task.Status.Message = &Message{
		MessageID: "msg-1",
		Role:      "ROLE_AGENT",
		Parts:     []Part{{Kind: "data", Data: json.RawMessage(`{"answer":42}`)}},
	}
	err := ValidateTaskResult(task)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "use an Artifact instead")
}

func TestValidateTaskResultAllowsControlMessagesAndArtifacts(t *testing.T) {
	task := NewTask("task-output", "ctx-1")
	task.Status.Message = &Message{
		MessageID: "msg-1",
		Role:      "ROLE_AGENT",
		Parts:     []Part{{Text: "working on it"}},
	}
	task.History = []Message{{
		MessageID: "msg-0",
		Role:      "ROLE_USER",
		Parts:     []Part{{Kind: "data", Data: json.RawMessage(`{"query":1}`)}},
	}}
	require.NoError(t, task.AddArtifact(Artifact{
		ArtifactID: "a-1",
		Parts:      []Part{{Kind: "data", Data: json.RawMessage(`{"answer":42}`)}},
	}))
	assert.NoError(t, ValidateTaskResult(task))
}

func TestValidateResultPlacement(t *testing.T) {
	bad := Task{
		ID: "task-1",
		Status: TaskStatus{State: TaskStateCompleted, Message: &Message{
			Role:  "ROLE_AGENT",
			Parts: []Part{{MIMEType: "application/pdf", Data: json.RawMessage(`"JVBERi0="`)}},
		}},
	}
	raw, err := json.Marshal(bad)
	require.NoError(t, err)

	assert.Error(t, ValidateResultPlacement(raw))
	assert.Error(t, ValidateResultPlacement(json.RawMessage(`{"task":`+string(raw)+`}`)))
	assert.Error(t, ValidateResultPlacement(json.RawMessage(`{"message":{"role":"ROLE_AGENT","parts":[{"kind":"file"}]}}`)))

	good := Task{ID: "task-1", Status: TaskStatus{State: TaskStateCompleted}}
	raw, err = json.Marshal(good)
	require.NoError(t, err)
	assert.NoError(t, ValidateResultPlacement(raw))
	assert.NoError(t, ValidateResultPlacement(json.RawMessage(`{"message":{"role":"ROLE_USER","parts":[{"text":"hi"}]}}`)))
	assert.NoError(t, ValidateResultPlacement(json.RawMessage(`{"anything":true}`)))

	err = ValidateResultPlacement(json.RawMessage(`{"task":{"id":"t","status":{"state":"COMPLETED","message":{"role":"ROLE_AGENT","parts":[{"kind":"data","data":{"x":1}}]}}}}`))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "Artifact"))
}

func TestPublishStatusRejectsMisplacedOutput(t *testing.T) {
	hub := NewStreamHub(4)
	task := NewTask("task-publish", "ctx-1")
	task.Status.Message = &Message{
		Role:  "ROLE_AGENT",
		Parts: []Part{{Kind: "data", Data: json.RawMessage(`{"result":1}`)}},
	}
	err := hub.PublishStatus("org/unit/agent", task)
	require.Error(t, err)
}
