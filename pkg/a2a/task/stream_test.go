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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicBuilders(t *testing.T) {
	req, err := RequestTopic("org/unit/agent")
	require.NoError(t, err)
	assert.Equal(t, "$a2a/v1/org/unit/agent/requests", req)

	status, err := StatusTopic("org/unit/agent", "task.1")
	require.NoError(t, err)
	assert.Equal(t, "$a2a/v1/org/unit/agent/tasks/task.1/status", status)

	artifact, err := ArtifactTopic("org/unit/agent", "task.1")
	require.NoError(t, err)
	assert.Equal(t, "$a2a/v1/org/unit/agent/tasks/task.1/artifacts", artifact)

	address, ok := ParseRequestTopic(req)
	assert.True(t, ok)
	assert.Equal(t, "org/unit/agent", address)

	_, err = RequestTopic("org/unit/agent/+")
	assert.Error(t, err)
	_, err = StatusTopic("org/unit/agent", "bad/#")
	assert.Error(t, err)
}

func TestStreamHubClosesOnTerminalState(t *testing.T) {
	hub := NewStreamHub(8)
	address := "org/unit/agent"
	task := NewTask("task-1", "ctx-1")

	stream, err := hub.Subscribe(address, task.ID, task)
	require.NoError(t, err)
	require.NoError(t, hub.PublishArtifact(address, task.ID, &Artifact{ArtifactID: "a-1"}))

	require.NoError(t, task.Transition(TaskStateWorking))
	require.NoError(t, hub.PublishStatus(address, task))
	require.NoError(t, task.AddArtifact(Artifact{ArtifactID: "a-2", Parts: []Part{{Text: "done"}}}))
	require.NoError(t, task.Transition(TaskStateCompleted))
	require.NoError(t, hub.PublishStatus(address, task))

	// The first event is the current Task from Subscribe.
	select {
	case first := <-stream.Events():
		assert.Equal(t, StreamEventStatus, first.Kind)
		assert.Equal(t, TaskStateSubmitted, first.Task.Status.State)
	case <-time.After(time.Second):
		t.Fatal("missing current task event")
	}

	var kinds []StreamEventKind
	var final bool
	for i := 0; i < 3; i++ {
		select {
		case ev := <-stream.Events():
			kinds = append(kinds, ev.Kind)
			if ev.Kind == StreamEventStatus && ev.Final {
				final = true
			}
		case <-time.After(time.Second):
			t.Fatalf("missing stream event %d", i)
		}
	}
	assert.Equal(t, []StreamEventKind{StreamEventArtifact, StreamEventStatus, StreamEventStatus}, kinds)
	assert.True(t, final)

	select {
	case <-stream.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal task did not close the stream")
	}

	// A terminal task can be subscribed to and immediately yields final state.
	terminal, err := hub.Subscribe(address, task.ID, task)
	require.NoError(t, err)
	select {
	case ev := <-terminal.Events():
		assert.True(t, ev.Final)
	case <-time.After(time.Second):
		t.Fatal("missing terminal task event")
	}
	select {
	case <-terminal.Done():
	case <-time.After(time.Second):
		t.Fatal("terminal task subscription was not closed")
	}
}

func TestStreamHubRejectsInvalidAddress(t *testing.T) {
	hub := NewStreamHub(1)
	_, err := hub.Subscribe("bad", "task-1", nil)
	assert.Error(t, err)
}
