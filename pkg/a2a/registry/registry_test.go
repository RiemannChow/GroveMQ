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

package registry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/hooks"
)

const validCard = `{
  "name": "Ops Agent", "description": "Does ops work", "version": "1.0.0",
  "url": "mqtt://broker:1883",
  "skills": [{"id":"diag","name":"Diagnostics","description":"Analyzes state."}]
}`

const fullCard = `{
  "name": "Ops Agent", "description": "Does ops work", "version": "1.0.0",
  "supportedInterfaces": [{"url":"mqtt://broker:1883","protocolBinding":"A2A-over-MQTT","protocolVersion":"1.0"}],
  "defaultInputModes": ["application/json"], "defaultOutputModes": ["application/json"],
  "skills": [{"id":"diag","name":"Diagnostics","description":"Analyzes state.","tags":["ops"]}]
}`

func testRegistry() *Registry {
	cfg := *DefaultConfig()
	cfg.Validate = schema.ModeLoose
	return New(cfg)
}

func publish(t *testing.T, r *Registry, clientID, topic string, payload []byte) bool {
	t.Helper()
	res, err := r.OnMessagePublish(context.Background(), &hooks.MessagePublishCtx{
		ClientID: clientID,
		Topic:    topic,
		Payload:  payload,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	return res.Allow
}

func TestRegistryRegisterAndUnregister(t *testing.T) {
	r := testRegistry()
	addr := "com.example/factory-a/agent-1"
	topic := hooks.A2ADiscoveryPrefix + addr

	assert.True(t, publish(t, r, addr, topic, []byte(validCard)))
	e, ok := r.Get(addr)
	require.True(t, ok)
	assert.Equal(t, "Ops Agent", e.Card.Name)
	assert.Equal(t, schema.ProtocolBindingMQTT, e.Card.SupportedInterfaces[0].ProtocolBinding)
	assert.Equal(t, StatusOffline, e.Status)

	st := r.Stats()
	assert.Equal(t, 1, st.Registered)
	assert.Equal(t, 1, st.Offline)

	// Empty retained payload removes the card.
	assert.True(t, publish(t, r, addr, topic, nil))
	_, ok = r.Get(addr)
	assert.False(t, ok)
}

func TestRegistryRejectsInvalidCard(t *testing.T) {
	r := testRegistry()
	addr := "com.example/factory-a/agent-1"
	topic := hooks.A2ADiscoveryPrefix + addr

	assert.False(t, publish(t, r, addr, topic, []byte(`{"name":"x"}`)))
	assert.Equal(t, int64(1), r.Stats().ValidationFails)
}

func TestRegistryStrictMode(t *testing.T) {
	cfg := *DefaultConfig()
	cfg.Validate = schema.ModeStrict
	r := New(cfg)
	addr := "org/unit/agent"
	topic := hooks.A2ADiscoveryPrefix + addr

	assert.False(t, publish(t, r, addr, topic, []byte(validCard)))
	assert.True(t, publish(t, r, addr, topic, []byte(fullCard)))
}

func TestRegistryMaxCardSize(t *testing.T) {
	cfg := *DefaultConfig()
	cfg.Validate = schema.ModeLoose
	cfg.MaxCardSize = 32
	r := New(cfg)
	addr := "org/unit/agent"
	assert.False(t, publish(t, r, addr, hooks.A2ADiscoveryPrefix+addr, []byte(validCard)))
	assert.Equal(t, int64(1), r.Stats().RejectedBySize)
}

func TestRegistryRateLimit(t *testing.T) {
	base := time.Now()
	cfg := *DefaultConfig()
	cfg.Validate = schema.ModeLoose
	cfg.Registrations = 2
	r := New(cfg)
	r.now = func() time.Time { return base }

	addr := "org/unit/agent"
	topic := hooks.A2ADiscoveryPrefix + addr
	assert.True(t, publish(t, r, addr, topic, []byte(validCard)))
	assert.True(t, publish(t, r, addr, topic, []byte(validCard)))
	assert.False(t, publish(t, r, addr, topic, []byte(validCard)))
	assert.Equal(t, int64(1), r.Stats().RateLimited)

	// After a minute the window clears.
	r.now = func() time.Time { return base.Add(time.Minute + time.Second) }
	assert.True(t, publish(t, r, addr, topic, []byte(validCard)))
}

func TestRegistryStatusTracking(t *testing.T) {
	r := testRegistry()
	addr := "org/unit/agent"
	require.True(t, publish(t, r, addr, hooks.A2ADiscoveryPrefix+addr, []byte(validCard)))

	ctx := context.Background()
	r.OnClientConnected(ctx, &hooks.ClientCtx{ClientID: addr})
	e, _ := r.Get(addr)
	assert.Equal(t, StatusOnline, e.Status)
	assert.Equal(t, StatusSourceBroker, e.StatusSource)

	r.OnClientDisconnected(ctx, &hooks.ClientCtx{ClientID: addr, Reason: "client sent DISCONNECT"})
	e, _ = r.Get(addr)
	assert.Equal(t, StatusOffline, e.Status)
	assert.Equal(t, StatusSourceAgent, e.StatusSource)

	r.OnClientConnected(ctx, &hooks.ClientCtx{ClientID: addr})
	r.OnClientDisconnected(ctx, &hooks.ClientCtx{ClientID: addr, Reason: "connection closed"})
	e, _ = r.Get(addr)
	assert.Equal(t, StatusSourceLWT, e.StatusSource)
}

func TestRegistryAnnotation(t *testing.T) {
	r := testRegistry()
	addr := "org/unit/agent"
	topic := hooks.A2ADiscoveryPrefix + addr
	baseProps := map[string][]byte{"custom": []byte("v")}

	// Unknown agent defaults to offline/broker.
	props := r.AnnotateRetained(topic, baseProps)
	assert.Equal(t, []byte(StatusOffline), props[PropertyAgentStatus])
	assert.Equal(t, []byte(StatusSourceBroker), props[PropertyAgentStatusSource])
	assert.Equal(t, []byte("v"), props["custom"])
	assert.Empty(t, baseProps[PropertyAgentStatus], "original map must not be mutated")

	require.True(t, publish(t, r, addr, topic, []byte(validCard)))
	r.OnClientConnected(context.Background(), &hooks.ClientCtx{ClientID: addr})
	props = r.AnnotateRetained(topic, nil)
	assert.Equal(t, []byte(StatusOnline), props[PropertyAgentStatus])

	// Non-A2A topics are untouched.
	assert.Nil(t, r.AnnotateRetained("sensors/temp", nil))
}

func TestRegistryIgnoresNonDiscoveryTopics(t *testing.T) {
	r := testRegistry()
	assert.True(t, publish(t, r, "device-1", "sensors/temp", []byte("not a card")))
	assert.Equal(t, 0, r.Stats().Registered)
	assert.Equal(t, int64(0), r.Stats().ValidationFails)
}

func TestRegistryRequiresSignedCardWhenConfigured(t *testing.T) {
	cfg := *DefaultConfig()
	cfg.Validate = schema.ModeLoose
	cfg.Security = &schema.SecurityConfig{RequireMetadata: true}
	r := New(cfg)
	addr := "org/unit/agent"
	topic := hooks.A2ADiscoveryPrefix + addr

	assert.False(t, publish(t, r, addr, topic, []byte(validCard)))
	_, ok := r.Get(addr)
	assert.False(t, ok)
	assert.Equal(t, int64(1), r.Stats().SignatureFails)
	assert.Equal(t, int64(1), r.Stats().ValidationFails)
}
