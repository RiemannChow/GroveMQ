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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/gateway"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/hooks"
)

func securityStartBroker(t *testing.T, authCfg hooks.A2ARequestAuthConfig) string {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	b := broker.New("a2a-security-node", nil)
	b.GetHooks().Register(hooks.NewA2ARequestAuthHook(authCfg))
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)
	return addr
}

func securityPublishRequest(t *testing.T, conn net.Conn, reader *bufio.Reader, packetID uint16, topic, payload string) *packets.Packet {
	t.Helper()
	p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        packetID,
			TopicName:       topic,
			Payload:         []byte(payload),
		}
		return pk.PublishEncode(buf)
	})
	return p2ReadPacket(t, reader)
}

// TestA2AContractRequestIdentityBinding verifies that a task request from an
// unbound MQTT client is rejected by the broker with the A2A UnauthorizedError
// semantic error in PUBACK, while the agent address itself is accepted.
func TestA2AContractRequestIdentityBinding(t *testing.T) {
	addr := securityStartBroker(t, hooks.A2ARequestAuthConfig{Policy: hooks.A2ARequestAuthPolicyCaller})
	topic := task.TopicPrefix + "org/unit/agent" + task.RequestSuffix
	payload := `{"jsonrpc":"2.0","id":"sec-1","method":"message/send","params":{}}`

	conn, reader := p2Connect(t, addr, "org/unit/attacker")
	ack := securityPublishRequest(t, conn, reader, 7, topic, payload)
	require.Equal(t, packets.Puback, ack.FixedHeader.Type)
	assert.Equal(t, task.ErrorUnauthorized.MQTTReason(), ack.ReasonCode)
	var found bool
	for _, prop := range ack.Properties.User {
		if prop.Key == task.PropertyAgentError {
			assert.Equal(t, string(task.ErrorUnauthorized), prop.Val)
			found = true
		}
	}
	assert.True(t, found, "a2a-error user property missing from rejected PUBACK")

	agentConn, agentReader := p2Connect(t, addr, "org/unit/agent")
	agentAck := securityPublishRequest(t, agentConn, agentReader, 8, topic, payload)
	require.Equal(t, packets.Puback, agentAck.FixedHeader.Type)
	assert.Equal(t, task.ReasonSuccess, agentAck.ReasonCode)
}

// TestA2AContractGatewayBearerAuth verifies that the HTTP gate rejects requests
// without a valid bearer token and admits authenticated ones to the bridge.
func TestA2AContractGatewayBearerAuth(t *testing.T) {
	bridge, err := gateway.NewMQTTBridge(gateway.MQTTConfig{BrokerURL: "tcp://127.0.0.1:1", Timeout: time.Second})
	require.NoError(t, err)
	handler := gateway.NewHandlerWithAuth(bridge, nil,
		&gateway.AuthConfig{Tokens: map[string]string{"tkn": "svc-principal"}})
	server := httptest.NewServer(handler)
	defer server.Close()
	body := `{"jsonrpc":"2.0","id":"1","method":"message/send","params":{}}`

	resp, err := server.Client().Post(server.URL+gateway.PathPrefix+"org/unit/agent", task.ContentType, bytes.NewBufferString(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	_, _ = io.Copy(io.Discard, resp.Body)

	req, err := http.NewRequest(http.MethodPost, server.URL+gateway.PathPrefix+"org/unit/agent", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", task.ContentType)
	req.Header.Set("Authorization", "Bearer tkn")
	resp2, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp2.Body.Close()
	// Authentication passed, so the request reaches the bridge and fails at
	// transport (502) instead of being rejected at the HTTP gate (401).
	assert.Equal(t, http.StatusBadGateway, resp2.StatusCode)
}
