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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/gateway"
	"github.com/turtacn/grovemq/pkg/a2a/registry"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/hooks"
)

const dodAddress = "com.example/factory-a/agent-1"

func dodStartRegistryBroker(t *testing.T) (string, *registry.Registry) {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	b := broker.New("a2a-dod-node", nil)
	cfg := *registry.DefaultConfig()
	cfg.Enabled = true
	cfg.Validate = schema.ModeLoose
	cfg.Registrations = 0
	reg := registry.New(cfg)
	b.GetHooks().Register(reg)
	b.RetainedAnnotationFunc = reg.AnnotateRetained

	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)
	return addr, reg
}

func dodPublishRetained(t *testing.T, conn net.Conn, reader *bufio.Reader, packetID uint16, topic string, payload []byte) {
	t.Helper()
	p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1, Retain: true},
			ProtocolVersion: 5,
			PacketID:        packetID,
			TopicName:       topic,
			Payload:         payload,
		}
		return pk.PublishEncode(buf)
	})
	ack, err := p2ReadPacketSafe(reader)
	require.NoError(t, err)
	require.Equal(t, packets.Puback, ack.FixedHeader.Type)
}

func dodSubscribe(t *testing.T, addr, filter string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, reader := p2Connect(t, addr, fmt.Sprintf("a2a-dod-sub-%d", time.Now().UnixNano()))
	p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        1,
			Filters:         packets.Subscriptions{{Filter: filter, Qos: 1}},
		}
		return pk.SubscribeEncode(buf)
	})
	ack, err := p2ReadPacketSafe(reader)
	require.NoError(t, err)
	require.Equal(t, packets.Suback, ack.FixedHeader.Type)
	require.NotEmpty(t, ack.ReasonCodes)
	require.Less(t, ack.ReasonCodes[0], byte(0x80))
	return conn, reader
}

func dodUserProperty(pk *packets.Packet, key string) string {
	for _, prop := range pk.Properties.User {
		if prop.Key == key {
			return prop.Val
		}
	}
	return ""
}

// TestA2ADoDDiscoveryAndUnregister covers Definition of Done items 1 and 2
// using MQTT 5 clients so status User Properties are observable on the wire.
func TestA2ADoDDiscoveryAndUnregister(t *testing.T) {
	addr, _ := dodStartRegistryBroker(t)
	topic := hooks.A2ADiscoveryPrefix + dodAddress
	filter := hooks.A2ADiscoveryPrefix + "com.example/+/+"

	pub, reader := p2Connect(t, addr, dodAddress)
	dodPublishRetained(t, pub, reader, 1, topic, []byte(p1TestCard))

	_, subReader := dodSubscribe(t, addr, filter)
	msg, err := p2ReadPacketSafe(subReader)
	require.NoError(t, err)
	require.Equal(t, packets.Publish, msg.FixedHeader.Type)
	assert.Equal(t, topic, msg.TopicName)
	assert.JSONEq(t, p1TestCard, string(msg.Payload))
	assert.Equal(t, registry.StatusOnline, dodUserProperty(msg, registry.PropertyAgentStatus))
	assert.Equal(t, registry.StatusSourceBroker, dodUserProperty(msg, registry.PropertyAgentStatusSource))

	// Clearing the retained card must remove it for later subscribers too.
	dodPublishRetained(t, pub, reader, 2, topic, nil)
	lateConn, lateReader := dodSubscribe(t, addr, filter)
	require.NoError(t, lateConn.SetDeadline(time.Now().Add(700*time.Millisecond)))
	late, err := p2ReadPacketSafe(lateReader)
	require.Error(t, err, "a new subscriber must not receive an unregistered card, got %v", late)
}

// TestA2ADoDUnknownAgentError covers Definition of Done item 5: a task sent to
// an unregistered address returns the TaskNotFoundError semantic error through
// the HTTP JSON-RPC gateway.
func TestA2ADoDUnknownAgentError(t *testing.T) {
	addr, reg := dodStartRegistryBroker(t)
	pub, reader := p2Connect(t, addr, dodAddress)
	dodPublishRetained(t, pub, reader, 1, hooks.A2ADiscoveryPrefix+dodAddress, []byte(p1TestCard))
	require.Eventually(t, func() bool {
		_, ok := reg.Get(dodAddress)
		return ok
	}, 2*time.Second, 25*time.Millisecond)

	bridge, err := gateway.NewMQTTBridge(gateway.MQTTConfig{BrokerURL: "tcp://" + addr, Timeout: time.Second})
	require.NoError(t, err)
	handler := gateway.NewHandlerWithAgentLookup(bridge, func(address string) bool {
		_, ok := reg.Get(address)
		return ok
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := server.Client().Post(
		server.URL+gateway.PathPrefix+"com.example/factory-a/missing",
		task.ContentType,
		bytes.NewBufferString(`{"jsonrpc":"2.0","id":"dod-5","method":"message/send","params":{}}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var rpc task.Response
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rpc))
	require.NotNil(t, rpc.Error)
	assert.Equal(t, task.ErrorTaskNotFound.RPCCode(), rpc.Error.Code)
	assert.Contains(t, rpc.Error.Message, "agent not found")
}
