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

package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/gateway"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/storage"
)

// rpcCall posts one JSON-RPC request to the gateway and decodes the response.
func rpcCall(t *testing.T, server *httptest.Server, address, body string) *task.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		server.URL+gateway.PathPrefix+address, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", task.ContentType)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out task.Response
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return &out
}

// newCompatServer wires an in-process gateway with a compatibility context.
func newCompatServer(t *testing.T, push bool, extended bool, caps gateway.CapabilityLookup, lookup gateway.AgentLookup, store *task.Store) *httptest.Server {
	t.Helper()
	bridge := gateway.NewMemoryBridge()
	h := gateway.NewHandlerWithAgentLookup(bridge, lookup)
	compat := &gateway.CompatibilityContext{
		PushStore:                  task.NewPushStore(),
		Capabilities:               caps,
		PushNotificationsSupported: push,
		ExtendedCardSupported:      extended,
	}
	if caps == nil {
		compat.ExtendedCard = func(string) (json.RawMessage, bool) {
			return json.RawMessage(`{"name":"extended-agent","version":"2.0"}`), true
		}
	}
	h.SetCompatibilityContext(compat)
	h.SetTaskStore(store)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// TestA2ACompatPushConfigRoundTrip verifies all four A2A 1.0 push config
// methods are answered locally without forwarding to an MQTT agent.
func TestA2ACompatPushConfigRoundTrip(t *testing.T) {
	const address = "org/unit/agent"
	srv := newCompatServer(t, true, false, nil, nil, nil)

	created := rpcCall(t, srv, address,
		`{"jsonrpc":"2.0","id":"1","method":"CreateTaskPushNotificationConfig","params":{"taskId":"task-1","taskPushNotificationConfig":{"url":"https://example.test/hook","authentication":{"schemes":["bearer"]}}}}`)
	require.Nil(t, created.Error, "create must succeed: %+v", created.Error)
	var cfg task.PushNotificationConfig
	require.NoError(t, json.Unmarshal(created.Result, &cfg))
	assert.NotEmpty(t, cfg.ID)
	assert.Equal(t, "task-1", cfg.TaskID)
	assert.Equal(t, "https://example.test/hook", cfg.URL)

	get := rpcCall(t, srv, address,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":"2","method":"GetTaskPushNotificationConfig","params":{"taskId":"task-1","configId":%q}}`, cfg.ID))
	require.Nil(t, get.Error)
	var got task.PushNotificationConfig
	require.NoError(t, json.Unmarshal(get.Result, &got))
	assert.Equal(t, cfg.ID, got.ID)

	list := rpcCall(t, srv, address,
		`{"jsonrpc":"2.0","id":"3","method":"ListTaskPushNotificationConfigs","params":{"taskId":"task-1"}}`)
	require.Nil(t, list.Error)
	var configs []task.PushNotificationConfig
	require.NoError(t, json.Unmarshal(list.Result, &configs))
	require.Len(t, configs, 1)

	del := rpcCall(t, srv, address,
		fmt.Sprintf(`{"jsonrpc":"2.0","id":"4","method":"DeleteTaskPushNotificationConfig","params":{"taskId":"task-1","configId":%q}}`, cfg.ID))
	require.Nil(t, del.Error)

	after := rpcCall(t, srv, address,
		`{"jsonrpc":"2.0","id":"5","method":"ListTaskPushNotificationConfigs","params":{"taskId":"task-1"}}`)
	require.Nil(t, after.Error)
	require.NoError(t, json.Unmarshal(after.Result, &configs))
	assert.Empty(t, configs)
}

// TestA2ACompatPushConfigUnsupported checks the standard error when the agent
// does not advertise the pushNotifications capability.
func TestA2ACompatPushConfigUnsupported(t *testing.T) {
	srv := newCompatServer(t, false, false, nil, nil, nil)
	resp := rpcCall(t, srv, "org/unit/agent",
		`{"jsonrpc":"2.0","id":"1","method":"CreateTaskPushNotificationConfig","params":{"taskId":"t","taskPushNotificationConfig":{"url":"https://example.test"}}}`)
	require.NotNil(t, resp.Error)
	assert.Equal(t, task.ErrorPushNotSupported.RPCCode(), resp.Error.Code)
}

// TestA2ACompatExtendedCard verifies capability gating, per-agent lookup and
// the not-configured error path for GetExtendedAgentCard.
func TestA2ACompatExtendedCard(t *testing.T) {
	cards := map[string]json.RawMessage{
		"org/unit/agent": json.RawMessage(`{"name":"extended","version":"2.0"}`),
	}
	lookup := func(a string) (json.RawMessage, bool) {
		c, ok := cards[a]
		return c, ok
	}
	caps := func(string) (bool, bool, bool) { return false, false, true }

	t.Run("configured", func(t *testing.T) {
		srv := newCompatServer(t, false, true, caps, nil, nil)
		h := srv.Config.Handler.(*gateway.Handler)
		h.SetCompatibilityContext(&gateway.CompatibilityContext{
			Capabilities: caps, ExtendedCard: lookup, ExtendedCardSupported: true,
		})
		resp := rpcCall(t, srv, "org/unit/agent",
			`{"jsonrpc":"2.0","id":"1","method":"GetExtendedAgentCard","params":{}}`)
		require.Nil(t, resp.Error)
		var card map[string]interface{}
		require.NoError(t, json.Unmarshal(resp.Result, &card))
		assert.Equal(t, "extended", card["name"])
	})

	t.Run("not configured", func(t *testing.T) {
		srv := newCompatServer(t, false, true, caps, nil, nil)
		h := srv.Config.Handler.(*gateway.Handler)
		h.SetCompatibilityContext(&gateway.CompatibilityContext{
			Capabilities: caps, ExtendedCard: lookup, ExtendedCardSupported: true,
		})
		resp := rpcCall(t, srv, "org/unit/other",
			`{"jsonrpc":"2.0","id":"2","method":"GetExtendedAgentCard","params":{}}`)
		require.NotNil(t, resp.Error)
		assert.Equal(t, task.ErrorExtendedCardMissing.RPCCode(), resp.Error.Code)
	})

	t.Run("unsupported", func(t *testing.T) {
		srv := newCompatServer(t, false, false, nil, nil, nil)
		resp := rpcCall(t, srv, "org/unit/agent",
			`{"jsonrpc":"2.0","id":"3","method":"GetExtendedAgentCard","params":{}}`)
		require.NotNil(t, resp.Error)
		assert.Equal(t, task.ErrorUnsupportedOperation.RPCCode(), resp.Error.Code)
	})
}

// TestA2ATaskStorePersistence verifies Task/Artifact durability and the
// offline tasks/get fallback through the gateway.
func TestA2ATaskStorePersistence(t *testing.T) {
	const address = "org/unit/agent"
	backend := storage.NewMemStore()
	store := task.NewStore(backend)
	require.True(t, store.Enabled())

	// Simulate a completed task result mirrored by the gateway.
	t1 := task.NewTask("task-durable", "ctx-1")
	require.NoError(t, t1.Transition(task.TaskStateWorking))
	require.NoError(t, t1.AddArtifact(task.Artifact{
		ArtifactID: "art-1",
		Parts:      []task.Part{{Kind: "text", Text: "durable output"}},
	}))
	require.NoError(t, t1.Transition(task.TaskStateCompleted))
	require.NoError(t, store.Save(address, t1))

	// A fresh Store over the same backend simulates a broker restart.
	restored := task.NewStore(backend)
	got, ok := restored.Get(address, "task-durable")
	require.True(t, ok)
	assert.Equal(t, task.TaskStateCompleted, got.Status.State)
	require.Len(t, got.Artifacts, 1)
	assert.Equal(t, "durable output", got.Artifacts[0].Parts[0].Text)
	assert.Len(t, restored.List(address), 1)

	// Agent lookup fails (offline) but tasks/get is served from the store.
	srv := newCompatServer(t, false, false, nil,
		func(string) bool { return false }, restored)
	resp := rpcCall(t, srv, address,
		`{"jsonrpc":"2.0","id":"9","method":"tasks/get","params":{"id":"task-durable"}}`)
	require.Nil(t, resp.Error, "offline tasks/get must hit the store")
	var returned task.Task
	require.NoError(t, json.Unmarshal(resp.Result, &returned))
	assert.Equal(t, "task-durable", returned.ID)

	// An unknown task still reports TaskNotFound semantics.
	missing := rpcCall(t, srv, address,
		`{"jsonrpc":"2.0","id":"10","method":"tasks/get","params":{"id":"nope"}}`)
	require.NotNil(t, missing.Error)
	assert.Equal(t, task.ErrorTaskNotFound.RPCCode(), missing.Error.Code)

	// Delete removes the durable record.
	assert.True(t, restored.Delete(address, "task-durable"))
	_, ok = restored.Get(address, "task-durable")
	assert.False(t, ok)
}

// TestA2ASharedRequestTopicHelpers verifies the shared-subscription filter
// helpers produce and parse valid A2A worker-pool topics.
func TestA2ASharedRequestTopicHelpers(t *testing.T) {
	filter, err := task.SharedRequestTopic("pool", "org/unit/agent")
	require.NoError(t, err)
	assert.Equal(t, "$share/pool/$a2a/v1/org/unit/agent/requests", filter)

	group, address, ok := task.ParseSharedRequestTopic(filter)
	assert.True(t, ok)
	assert.Equal(t, "pool", group)
	assert.Equal(t, "org/unit/agent", address)

	_, _, ok = task.ParseSharedRequestTopic("$a2a/v1/org/unit/agent/requests")
	assert.False(t, ok, "non-shared filter must not parse")

	_, err = task.SharedRequestTopic("bad/group", "org/unit/agent")
	assert.Error(t, err, "'/' is illegal in a shared group")
}
