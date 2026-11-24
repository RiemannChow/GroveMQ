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

package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/registry"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/hooks"
	"github.com/turtacn/grovemq/pkg/metrics"
)

const a2aTestCard = `{
  "name": "Ops Agent", "description": "Does ops work", "version": "1.0.0",
  "url": "mqtt://broker:1883",
  "skills": [{"id":"diag","name":"Diagnostics","description":"Analyzes state."}]
}`

type mockA2AProvider struct {
	agents map[string]A2AAgent
	stats  A2AStats
}

func newMockA2AProvider() *mockA2AProvider {
	now := time.Now()
	return &mockA2AProvider{
		agents: map[string]A2AAgent{
			"org/unit/agent-a": {
				Address:      "org/unit/agent-a",
				OrgID:        "org",
				UnitID:       "unit",
				AgentID:      "agent-a",
				Card:         map[string]interface{}{"name": "Agent A"},
				Status:       registry.StatusOnline,
				StatusSource: registry.StatusSourceBroker,
				RegisteredAt: now,
				UpdatedAt:    now,
			},
			"org/unit/agent-b": {
				Address:      "org/unit/agent-b",
				OrgID:        "org",
				UnitID:       "unit",
				AgentID:      "agent-b",
				Card:         map[string]interface{}{"name": "Agent B"},
				Status:       registry.StatusOffline,
				StatusSource: registry.StatusSourceLWT,
				RegisteredAt: now,
				UpdatedAt:    now,
			},
		},
		stats: A2AStats{Enabled: true, Registered: 2, Online: 1, Offline: 1},
	}
}

func (m *mockA2AProvider) List() []A2AAgent {
	out := make([]A2AAgent, 0, len(m.agents))
	for _, agent := range m.agents {
		out = append(out, agent)
	}
	return out
}

func (m *mockA2AProvider) Get(address string) (*A2AAgent, bool) {
	agent, ok := m.agents[address]
	if !ok {
		return nil, false
	}
	return &agent, true
}

func (m *mockA2AProvider) Delete(address string) bool {
	if _, ok := m.agents[address]; !ok {
		return false
	}
	delete(m.agents, address)
	m.stats.Registered--
	return true
}

func (m *mockA2AProvider) Stats() A2AStats { return m.stats }

type apiEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func decodeAPIResponse(t *testing.T, rr *httptest.ResponseRecorder) apiEnvelope {
	t.Helper()
	var out apiEnvelope
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
	return out
}

func serveA2ARequest(server *APIServer, method, path string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	server.RegisterRoutes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(method, path, nil))
	return rr
}

func TestA2AEndpointsDisabledByDefault(t *testing.T) {
	server := NewAPIServer(metrics.NewMetricsManager(), newMockBroker())

	for _, path := range []string{
		"/api/v5/a2a/agents",
		"/api/v5/a2a/agents/org%2Funit%2Fagent-a",
		"/api/v5/a2a/stats",
	} {
		rr := serveA2ARequest(server, http.MethodGet, path)
		assert.Equal(t, http.StatusNotImplemented, rr.Code, path)
		resp := decodeAPIResponse(t, rr)
		assert.Equal(t, http.StatusNotImplemented, resp.Code)
		assert.Contains(t, resp.Message, "not enabled")
	}
}

func TestA2AAdminAPI(t *testing.T) {
	server := NewAPIServer(metrics.NewMetricsManager(), newMockBroker())
	provider := newMockA2AProvider()
	server.SetA2AProvider(provider)

	t.Run("list", func(t *testing.T) {
		rr := serveA2ARequest(server, http.MethodGet, "/api/v5/a2a/agents")
		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeAPIResponse(t, rr)
		assert.Equal(t, 0, resp.Code)
		var agents []A2AAgent
		require.NoError(t, json.Unmarshal(resp.Data, &agents))
		require.Len(t, agents, 2)
	})

	t.Run("get by encoded address", func(t *testing.T) {
		rr := serveA2ARequest(server, http.MethodGet, "/api/v5/a2a/agents/org%2Funit%2Fagent-a")
		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeAPIResponse(t, rr)
		var agent A2AAgent
		require.NoError(t, json.Unmarshal(resp.Data, &agent))
		assert.Equal(t, "org/unit/agent-a", agent.Address)
		assert.Equal(t, registry.StatusOnline, agent.Status)
	})

	t.Run("stats", func(t *testing.T) {
		rr := serveA2ARequest(server, http.MethodGet, "/api/v5/a2a/stats")
		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeAPIResponse(t, rr)
		var stats A2AStats
		require.NoError(t, json.Unmarshal(resp.Data, &stats))
		assert.Equal(t, 2, stats.Registered)
		assert.Equal(t, 1, stats.Online)
		assert.Equal(t, 1, stats.Offline)
	})

	t.Run("delete and not found", func(t *testing.T) {
		rr := serveA2ARequest(server, http.MethodDelete, "/api/v5/a2a/agents/org%2Funit%2Fagent-b")
		require.Equal(t, http.StatusOK, rr.Code)

		rr = serveA2ARequest(server, http.MethodGet, "/api/v5/a2a/agents/org%2Funit%2Fagent-b")
		assert.Equal(t, http.StatusNotFound, rr.Code)
	})

	t.Run("method and path validation", func(t *testing.T) {
		rr := serveA2ARequest(server, http.MethodPost, "/api/v5/a2a/agents")
		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)

		rr = serveA2ARequest(server, http.MethodGet, "/api/v5/a2a/agents/")
		assert.Equal(t, http.StatusBadRequest, rr.Code)

		rr = serveA2ARequest(server, http.MethodPost, "/api/v5/a2a/stats")
		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	})
}

func TestNewA2AProviderAdapter(t *testing.T) {
	assert.Nil(t, NewA2AProvider(nil))

	cfg := *registry.DefaultConfig()
	cfg.Enabled = true
	cfg.Validate = schema.ModeLoose
	cfg.Registrations = 0
	r := registry.New(cfg)

	address := "org/unit/agent"
	res, err := r.OnMessagePublish(context.Background(), &hooks.MessagePublishCtx{
		ClientID: address,
		Topic:    hooks.A2ADiscoveryPrefix + address,
		Payload:  []byte(a2aTestCard),
	})
	require.NoError(t, err)
	require.True(t, res.Allow)

	provider := NewA2AProvider(r)
	require.NotNil(t, provider)

	agents := provider.List()
	require.Len(t, agents, 1)
	assert.Equal(t, address, agents[0].Address)
	assert.Equal(t, "Ops Agent", agents[0].Card.(*schema.AgentCard).Name)

	agent, ok := provider.Get(address)
	require.True(t, ok)
	assert.Equal(t, "agent", agent.AgentID)

	stats := provider.Stats()
	assert.True(t, stats.Enabled)
	assert.Equal(t, 1, stats.Registered)

	assert.True(t, provider.Delete(address))
	assert.False(t, provider.Delete(address))
}
