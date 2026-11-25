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
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/registry"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/admin"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/hooks"
	"github.com/turtacn/grovemq/pkg/metrics"
)

const p1TestCard = `{
  "name": "Ops Agent", "description": "Does ops work", "version": "1.0.0",
  "url": "mqtt://broker:1883",
  "skills": [{"id":"diag","name":"Diagnostics","description":"Analyzes state."}]
}`

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())
	return port
}

func connectA2AClient(t *testing.T, brokerURL, clientID string, handler mqtt.MessageHandler) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second)
	if handler != nil {
		opts.SetDefaultPublishHandler(handler)
	}

	client := mqtt.NewClient(opts)
	var lastErr error
	for i := 0; i < 20; i++ {
		token := client.Connect()
		token.Wait()
		lastErr = token.Error()
		if lastErr == nil {
			t.Cleanup(func() { client.Disconnect(250) })
			return client
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("connect MQTT client %s: %v", clientID, lastErr)
	return nil
}

func waitForAgentCount(t *testing.T, provider admin.A2AProvider, want int) {
	t.Helper()
	for i := 0; i < 40; i++ {
		if len(provider.List()) == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("agent count = %d, want %d", len(provider.List()), want)
}

func adminA2AData(t *testing.T, api *admin.APIServer, path string) []byte {
	t.Helper()
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var envelope struct {
		Code    int             `json:"code"`
		Message string          `json:"message,omitempty"`
		Data    json.RawMessage `json:"data,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code, envelope.Message)
	return envelope.Data
}

// TestA2ARegistryDiscoveryLoop verifies the P1 closed loop across the real
// broker: retained discovery publish -> hook validation -> registry index ->
// admin API -> retained delivery -> empty retained unregister.
func TestA2ARegistryDiscoveryLoop(t *testing.T) {
	port := freePort(t)
	listenAddr := fmt.Sprintf("127.0.0.1:%d", port)
	brokerURL := fmt.Sprintf("tcp://%s", listenAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.New("a2a-p1-node", nil)
	cfg := *registry.DefaultConfig()
	cfg.Enabled = true
	cfg.Validate = schema.ModeLoose
	cfg.Registrations = 0
	reg := registry.New(cfg)
	b.GetHooks().Register(reg)
	b.RetainedAnnotationFunc = reg.AnnotateRetained

	api := admin.NewAPIServer(metrics.NewMetricsManager(), b)
	provider := admin.NewA2AProvider(reg)
	api.SetA2AProvider(provider)

	go func() {
		_ = b.StartServer(ctx, listenAddr)
	}()

	address := "org/unit/agent"
	topic := hooks.A2ADiscoveryPrefix + address
	agent := connectA2AClient(t, brokerURL, address, nil)

	token := agent.Publish(topic, 1, true, p1TestCard)
	require.True(t, token.WaitTimeout(3*time.Second))
	require.NoError(t, token.Error())

	waitForAgentCount(t, provider, 1)
	entry, ok := reg.Get(address)
	require.True(t, ok)
	assert.Equal(t, "Ops Agent", entry.Card.Name)

	// The connected hook may run before or after the first retained publish,
	// so drive the status transition explicitly instead of assuming a race.
	agent.Disconnect(250)
	require.Eventually(t, func() bool {
		e, found := reg.Get(address)
		return found && e.Status == registry.StatusOffline
	}, 2*time.Second, 25*time.Millisecond)

	data := adminA2AData(t, api, "/api/v5/a2a/agents")
	var agents []admin.A2AAgent
	require.NoError(t, json.Unmarshal(data, &agents))
	require.Len(t, agents, 1)
	assert.Equal(t, address, agents[0].Address)

	// Reconnect the agent so the connected hook marks the existing card online.
	agent = connectA2AClient(t, brokerURL, address, nil)
	require.Eventually(t, func() bool {
		e, found := reg.Get(address)
		return found && e.Status == registry.StatusOnline
	}, 2*time.Second, 25*time.Millisecond)

	annotated := reg.AnnotateRetained(topic, map[string][]byte{"existing": []byte("value")})
	assert.Equal(t, []byte("value"), annotated["existing"])
	assert.Equal(t, []byte(registry.StatusOnline), annotated[registry.PropertyAgentStatus])
	assert.Equal(t, []byte(registry.StatusSourceBroker), annotated[registry.PropertyAgentStatusSource])

	// A subscriber must use the exact reserved topic: '#' never matches '$'.
	msgCh := make(chan mqtt.Message, 1)
	subscriber := connectA2AClient(t, brokerURL, "a2a-subscriber", func(_ mqtt.Client, msg mqtt.Message) {
		msgCh <- msg
	})
	token = subscriber.Subscribe(topic, 1, nil)
	require.True(t, token.WaitTimeout(3*time.Second))
	require.NoError(t, token.Error())

	select {
	case msg := <-msgCh:
		assert.Equal(t, topic, msg.Topic())
		assert.JSONEq(t, p1TestCard, string(msg.Payload()))
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for retained A2A agent card")
	}

	// Clearing the retained card unregisters the agent.
	token = agent.Publish(topic, 1, true, []byte{})
	require.True(t, token.WaitTimeout(3*time.Second))
	require.NoError(t, token.Error())
	waitForAgentCount(t, provider, 0)
	_, ok = reg.Get(address)
	assert.False(t, ok)
}
