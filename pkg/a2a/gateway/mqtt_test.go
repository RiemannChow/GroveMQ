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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/broker"
)

func waitForTCP(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 40; i++ {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("broker %s did not accept connections", addr)
}

func gatewayTestPort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())
	return port
}

// startRawAgent opens a minimal MQTT 5 client that consumes one A2A request
// and replies with an Artifact-bearing Task using Response Topic and
// Correlation Data. It closes ready after SUBACK so the test never races the
// gateway publish.
func startRawAgent(t *testing.T, brokerAddr, address string, responseTopic *string, correlation *[]byte, ready chan struct{}, errCh chan error) {
	t.Helper()
	requestTopic := fmt.Sprintf("$a2a/v1/%s/requests", address)

	go func() {
		fail := func(err error) {
			select {
			case errCh <- err:
			default:
			}
		}
		conn, err := net.DialTimeout("tcp", brokerAddr, time.Second)
		if err != nil {
			fail(fmt.Errorf("dial: %w", err))
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)

		if err := writePacket(conn, func(buf *bytes.Buffer) error {
			pk := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Connect},
				ProtocolVersion: 5,
				Connect: packets.ConnectParams{
					ProtocolName:     []byte("MQTT"),
					ClientIdentifier: "a2a-test-agent",
					Clean:            true,
					Keepalive:        30,
				},
			}
			return pk.ConnectEncode(buf)
		}); err != nil {
			fail(fmt.Errorf("connect packet: %w", err))
			return
		}
		connack, err := readPacket(reader)
		if err != nil {
			fail(fmt.Errorf("connack: %w", err))
			return
		}
		if connack.FixedHeader.Type != packets.Connack || connack.ReasonCode != 0 {
			fail(fmt.Errorf("connack rejected: type=%d reason=0x%02x", connack.FixedHeader.Type, connack.ReasonCode))
			return
		}
		if err := writePacket(conn, func(buf *bytes.Buffer) error {
			pk := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
				ProtocolVersion: 5,
				PacketID:        subscribePacketID,
				Filters:         packets.Subscriptions{{Filter: requestTopic, Qos: 1}},
			}
			return pk.SubscribeEncode(buf)
		}); err != nil {
			fail(fmt.Errorf("subscribe packet: %w", err))
			return
		}
		suback, err := readPacket(reader)
		if err != nil {
			fail(fmt.Errorf("suback: %w", err))
			return
		}
		if suback.FixedHeader.Type != packets.Suback || len(suback.ReasonCodes) == 0 || suback.ReasonCodes[0] >= 0x80 {
			fail(fmt.Errorf("subscribe rejected: type=%d codes=%v", suback.FixedHeader.Type, suback.ReasonCodes))
			return
		}
		close(ready)

		pk, err := readPacket(reader)
		if err != nil || pk.FixedHeader.Type != packets.Publish {
			return
		}
		if pk.FixedHeader.Qos == 1 {
			ack := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: 5,
				PacketID:        pk.PacketID,
				ReasonCode:      task.ReasonSuccess,
			}
			_ = writePacket(conn, func(buf *bytes.Buffer) error { return ack.PubackEncode(buf) })
		}
		req, err := task.ParseRequest(pk.Payload)
		if err != nil {
			return
		}
		completed := task.NewTask("task-1", "ctx-1")
		_ = completed.Transition(task.TaskStateWorking)
		_ = completed.AddArtifact(task.Artifact{ArtifactID: "artifact-1", Parts: []task.Part{{Text: "answer"}}})
		_ = completed.Transition(task.TaskStateCompleted)
		resp, err := task.NewResponse(req.ID, completed)
		if err != nil {
			return
		}
		payload, err := json.Marshal(resp)
		if err != nil {
			return
		}
		*responseTopic = pk.Properties.ResponseTopic
		*correlation = append([]byte(nil), pk.Properties.CorrelationData...)

		_ = writePacket(conn, func(buf *bytes.Buffer) error {
			out := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
				ProtocolVersion: 5,
				TopicName:       pk.Properties.ResponseTopic,
				Payload:         payload,
				Mods:            packets.Mods{AllowResponseInfo: true},
				Properties: packets.Properties{
					ContentType:     task.ContentType,
					CorrelationData: pk.Properties.CorrelationData,
				},
			}
			return out.PublishEncode(buf)
		})
	}()
}

func TestMQTTBridgeRequestResponseCorrelation(t *testing.T) {
	port := gatewayTestPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := broker.New("a2a-gateway-test", nil)
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)

	bridge, err := NewMQTTBridge(MQTTConfig{BrokerURL: "tcp://" + addr, Timeout: 3 * time.Second})
	require.NoError(t, err)

	var responseTopic string
	var correlation []byte
	ready := make(chan struct{})
	errCh := make(chan error, 1)
	startRawAgent(t, addr, "org/unit/agent", &responseTopic, &correlation, ready, errCh)

	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("raw agent setup failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("raw agent did not subscribe")
	}

	req, err := task.NewRequest("req-1", task.MethodMessageSend, map[string]interface{}{
		"message": task.Message{MessageID: "msg-1", Role: "ROLE_USER", Parts: []task.Part{{Text: "hello"}}},
	})
	require.NoError(t, err)

	resp, err := bridge.Request(context.Background(), "org/unit/agent", req, map[string][]byte{
		"A2A-Extensions": []byte("https://example.com/ext"),
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	var result task.Task
	require.NoError(t, resp.ResultInto(&result))
	assert.Equal(t, "task-1", result.ID)
	assert.Equal(t, task.TaskStateCompleted, result.Status.State)
	require.Len(t, result.Artifacts, 1)
	assert.Equal(t, "answer", result.Artifacts[0].Parts[0].Text)
	assert.Contains(t, responseTopic, "$a2a/v1/gateway/")
	assert.NotEmpty(t, correlation)
}
