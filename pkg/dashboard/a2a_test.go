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

package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestA2AAgentsPageAndScript(t *testing.T) {
	server := createTestServer(t)

	rr := httptest.NewRecorder()
	server.mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/a2a-agents", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "A2A Agents")
	assert.Contains(t, rr.Body.String(), "/static/js/a2a-agents.js")

	rr = httptest.NewRecorder()
	server.mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/static/js/a2a-agents.js", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "/api/v5/a2a/agents")
	assert.Contains(t, rr.Body.String(), "/api/v5/a2a/stats")
}

func TestA2AAgentsPageRejectsPost(t *testing.T) {
	server := createTestServer(t)
	rr := httptest.NewRecorder()
	server.mux.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/a2a-agents", nil))
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}
