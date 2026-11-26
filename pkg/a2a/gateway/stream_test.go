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

package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

type scriptedStreamBridge struct {
	events []task.StreamEvent
	err    error
}

func (s *scriptedStreamBridge) Stream(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Stream, error) {
	if s.err != nil {
		return nil, s.err
	}
	hub := task.NewStreamHub(len(s.events) + 1)
	stream, err := hub.Subscribe(address, "task-stream", nil)
	if err != nil {
		return nil, err
	}
	go func() {
		for _, ev := range s.events {
			if ev.Kind == task.StreamEventStatus && ev.Task != nil {
				_ = hub.PublishStatus(address, ev.Task)
			} else if ev.Kind == task.StreamEventArtifact && ev.Artifact != nil {
				_ = hub.PublishArtifact(address, ev.TaskID, ev.Artifact)
			}
		}
	}()
	return stream, nil
}

func TestHTTPGatewayStreamsSSE(t *testing.T) {
	working := task.NewTask("task-stream", "ctx")
	require.NoError(t, working.Transition(task.TaskStateWorking))
	completedArtifact := task.Artifact{ArtifactID: "a-1", Parts: []task.Part{{Text: "progress"}}}
	completed := task.NewTask("task-stream", "ctx")
	require.NoError(t, completed.Transition(task.TaskStateWorking))
	require.NoError(t, completed.Transition(task.TaskStateCompleted))

	bridge := &scriptedStreamBridge{events: []task.StreamEvent{
		{Kind: task.StreamEventStatus, Task: working},
		{Kind: task.StreamEventArtifact, TaskID: "task-stream", Artifact: &completedArtifact},
		{Kind: task.StreamEventStatus, Task: completed},
	}}
	handler := NewStreamHandler(NewMemoryBridge(), bridge, nil, nil)

	body := `{"jsonrpc":"2.0","id":"sse-1","method":"message/stream","params":{"message":{"messageId":"m-1","parts":[{"text":"go"}]}}}`
	req := httptest.NewRequest(http.MethodPost, PathPrefix+"org/unit/agent", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	res := rec.Result()
	defer res.Body.Close()

	require.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "text/event-stream", res.Header.Get("Content-Type"))

	reader := bufio.NewReader(res.Body)
	var events []task.StreamEvent
	for i := 0; i < 3; i++ {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(line, "event: "))
		data, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(data, "data: "))
		var ev task.StreamEvent
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(data, "data: "))), &ev))
		events = append(events, ev)
		_, _ = reader.ReadString('\n') // blank separator
	}
	require.Len(t, events, 3)
	assert.False(t, events[0].Final)
	assert.Equal(t, "a-1", events[1].Artifact.ArtifactID)
	assert.True(t, events[2].Final, "terminal status event must be final")
	assert.Equal(t, task.TaskStateCompleted, events[2].Task.Status.State)
}

func TestHTTPGatewayStreamDisabled(t *testing.T) {
	handler := NewHandler(NewMemoryBridge())
	body := `{"jsonrpc":"2.0","id":"sse-2","method":"message/stream","params":{"message":{"parts":[]}}}`
	rr := postRPC(t, handler, PathPrefix+"org/unit/agent", body, nil)
	require.Equal(t, http.StatusNotImplemented, rr.Code)
	var resp task.Response
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.Error)
	assert.Equal(t, task.ErrorUnsupportedOperation.RPCCode(), resp.Error.Code)
}

func TestParseStreamParams(t *testing.T) {
	req, err := task.NewRequest("1", task.MethodMessageStream, map[string]interface{}{
		"message": map[string]interface{}{"messageId": "m"},
	})
	require.NoError(t, err)
	params, err := ParseStreamParams(req)
	require.NoError(t, err)
	require.Contains(t, params.Message, "messageId")

	_, err = ParseStreamParams(&task.Request{Params: json.RawMessage(`{}`)})
	require.Error(t, err)

	_, err = ParseStreamParams(&task.Request{Params: json.RawMessage(`{"taskId":"t-1"}`)})
	require.NoError(t, err)
}
