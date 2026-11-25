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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

func postRPC(t *testing.T, handler http.Handler, path string, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func TestHTTPGatewayBridgesJSONRPC(t *testing.T) {
	bridge := NewMemoryBridge()
	address := "org/unit/agent"
	var gotMethod string
	var gotProps map[string][]byte
	bridge.Register(address, func(_ context.Context, _ string, req *task.Request, props map[string][]byte) (interface{}, *task.SemanticError) {
		gotMethod = req.Method
		gotProps = props
		return &task.Task{ID: "task-1", Status: task.TaskStatus{State: task.TaskStateSubmitted}}, nil
	})

	rr := postRPC(t, NewHandler(bridge), PathPrefix+address,
		`{"jsonrpc":"2.0","id":"req-1","method":"send_message","params":{"message":{"messageId":"msg-1"}}}`,
		map[string]string{"A2A-Version": "1.0", "A2A-Extensions": "https://example.com/ext"})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, task.ContentType, rr.Header().Get("Content-Type"))

	var resp task.Response
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Nil(t, resp.Error)
	var result task.Task
	require.NoError(t, resp.ResultInto(&result))
	assert.Equal(t, "task-1", result.ID)
	assert.Equal(t, task.MethodMessageSend, gotMethod)
	assert.Equal(t, []byte("1.0"), gotProps["A2A-Version"])
	assert.Equal(t, []byte("https://example.com/ext"), gotProps["A2A-Extensions"])
}

func TestHTTPGatewayErrors(t *testing.T) {
	bridge := NewMemoryBridge()
	bridge.Register("org/unit/agent", func(_ context.Context, _ string, _ *task.Request, _ map[string][]byte) (interface{}, *task.SemanticError) {
		return nil, task.TaskNotFound("task-9")
	})
	handler := NewHandler(bridge)

	tests := []struct {
		name       string
		path       string
		body       string
		headers    map[string]string
		wantStatus int
		wantCode   int
	}{
		{
			name:       "invalid address",
			path:       PathPrefix + "bad",
			body:       `{"jsonrpc":"2.0","id":"1","method":"tasks/get"}`,
			wantStatus: http.StatusBadRequest,
			wantCode:   task.ErrorInvalidParams.RPCCode(),
		},
		{
			name:       "malformed json",
			path:       PathPrefix + "org/unit/agent",
			body:       `{`,
			wantStatus: http.StatusBadRequest,
			wantCode:   task.ErrorInvalidRequest.RPCCode(),
		},
		{
			name:       "unsupported version",
			path:       PathPrefix + "org/unit/agent",
			body:       `{"jsonrpc":"2.0","id":"1","method":"tasks/get"}`,
			headers:    map[string]string{"A2A-Version": "0.9"},
			wantStatus: http.StatusOK,
			wantCode:   task.ErrorVersionNotSupported.RPCCode(),
		},
		{
			name:       "unsupported method",
			path:       PathPrefix + "org/unit/agent",
			body:       `{"jsonrpc":"2.0","id":"1","method":"unknown/op"}`,
			wantStatus: http.StatusOK,
			wantCode:   task.ErrorMethodNotFound.RPCCode(),
		},
		{
			name:       "semantic error",
			path:       PathPrefix + "org/unit/agent",
			body:       `{"jsonrpc":"2.0","id":"1","method":"tasks/get"}`,
			wantStatus: http.StatusOK,
			wantCode:   task.ErrorTaskNotFound.RPCCode(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr := postRPC(t, handler, tc.path, tc.body, tc.headers)
			require.Equal(t, tc.wantStatus, rr.Code)
			var resp task.Response
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
			require.NotNil(t, resp.Error)
			assert.Equal(t, tc.wantCode, resp.Error.Code)
		})
	}
}

func TestHTTPGatewayRequiresPostAndBridge(t *testing.T) {
	handler := NewHandler(nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, PathPrefix+"org/unit/agent", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)

	rr = postRPC(t, handler, PathPrefix+"org/unit/agent", `{"jsonrpc":"2.0","id":"1","method":"tasks/get"}`, nil)
	assert.Equal(t, http.StatusNotImplemented, rr.Code)
}
