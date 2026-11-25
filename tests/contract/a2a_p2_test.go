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

func p2WritePacket(t *testing.T, conn net.Conn, encode func(*bytes.Buffer) error) {
	t.Helper()
	buf := new(bytes.Buffer)
	require.NoError(t, encode(buf))
	_, err := conn.Write(buf.Bytes())
	require.NoError(t, err)
}

func p2ReadPacket(t *testing.T, r *bufio.Reader) *packets.Packet {
	t.Helper()
	header, err := r.ReadByte()
	require.NoError(t, err)
	fh := packets.FixedHeader{}
	require.NoError(t, fh.Decode(header))
	remaining, _, err := packets.DecodeLength(r)
	require.NoError(t, err)
	fh.Remaining = remaining
	body := make([]byte, remaining)
	_, err = io.ReadFull(r, body)
	require.NoError(t, err)
	pk := &packets.Packet{FixedHeader: fh, ProtocolVersion: 5}
	switch fh.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	default:
		t.Fatalf("unexpected MQTT packet type %d", fh.Type)
	}
	require.NoError(t, err)
	return pk
}

func p2Connect(t *testing.T, addr, clientID string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(conn)
	p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: clientID,
				Clean:            true,
				Keepalive:        30,
			},
		}
		return pk.ConnectEncode(buf)
	})
	connack := p2ReadPacket(t, reader)
	require.Equal(t, packets.Connack, connack.FixedHeader.Type)
	require.Zero(t, connack.ReasonCode)
	return conn, reader
}

func p2StartResponder(t *testing.T, addr, address string, ready chan struct{}) {
	t.Helper()
	requestTopic := task.TopicPrefix + address + task.RequestSuffix
	go func() {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		reader := bufio.NewReader(conn)
		p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
			pk := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Connect},
				ProtocolVersion: 5,
				Connect: packets.ConnectParams{
					ProtocolName:     []byte("MQTT"),
					ClientIdentifier: "a2a-p2-responder",
					Clean:            true,
					Keepalive:        30,
				},
			}
			return pk.ConnectEncode(buf)
		})
		if _, err := p2ReadPacketSafe(reader); err != nil {
			return
		}
		p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
			pk := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
				ProtocolVersion: 5,
				PacketID:        1,
				Filters:         packets.Subscriptions{{Filter: requestTopic, Qos: 1}},
			}
			return pk.SubscribeEncode(buf)
		})
		suback, err := p2ReadPacketSafe(reader)
		if err != nil || suback.FixedHeader.Type != packets.Suback || len(suback.ReasonCodes) == 0 || suback.ReasonCodes[0] >= 0x80 {
			return
		}
		close(ready)

		pk, err := p2ReadPacketSafe(reader)
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
			_ = p2WritePacketSafe(conn, func(buf *bytes.Buffer) error { return ack.PubackEncode(buf) })
		}
		req, err := task.ParseRequest(pk.Payload)
		if err != nil {
			return
		}
		completed := task.NewTask("p2-task-1", "p2-context")
		_ = completed.Transition(task.TaskStateWorking)
		_ = completed.AddArtifact(task.Artifact{ArtifactID: "a-1", Parts: []task.Part{{Text: "p2 answer"}}})
		_ = completed.Transition(task.TaskStateCompleted)
		resp, err := task.NewResponse(req.ID, completed)
		if err != nil {
			return
		}
		payload, err := json.Marshal(resp)
		if err != nil {
			return
		}
		_ = p2WritePacketSafe(conn, func(buf *bytes.Buffer) error {
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

func p2ReadPacketSafe(r *bufio.Reader) (*packets.Packet, error) {
	header, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	fh := packets.FixedHeader{}
	if err := fh.Decode(header); err != nil {
		return nil, err
	}
	remaining, _, err := packets.DecodeLength(r)
	if err != nil {
		return nil, err
	}
	fh.Remaining = remaining
	body := make([]byte, remaining)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	pk := &packets.Packet{FixedHeader: fh, ProtocolVersion: 5}
	switch fh.Type {
	case packets.Connack:
		err = pk.ConnackDecode(body)
	case packets.Publish:
		err = pk.PublishDecode(body)
	case packets.Puback:
		err = pk.PubackDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	default:
		return nil, fmt.Errorf("unexpected packet type %d", fh.Type)
	}
	return pk, err
}

func p2WritePacketSafe(conn net.Conn, encode func(*bytes.Buffer) error) error {
	buf := new(bytes.Buffer)
	if err := encode(buf); err != nil {
		return err
	}
	_, err := conn.Write(buf.Bytes())
	return err
}

func TestA2AHTTPJSONRPCToMQTTTaskExchange(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := broker.New("a2a-p2-contract", nil)
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)

	ready := make(chan struct{})
	p2StartResponder(t, addr, "org/unit/agent", ready)
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("raw A2A agent did not subscribe")
	}

	bridge, err := gateway.NewMQTTBridge(gateway.MQTTConfig{BrokerURL: "tcp://" + addr, Timeout: 3 * time.Second})
	require.NoError(t, err)
	server := httptest.NewServer(gateway.NewHandler(bridge))
	defer server.Close()

	body := `{"jsonrpc":"2.0","id":"sdk-1","method":"sendMessage","params":{"message":{"messageId":"m-1","role":"ROLE_USER","parts":[{"text":"hello"}]}}}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+gateway.PathPrefix+"org/unit/agent", bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", task.ContentType)
	req.Header.Set("A2A-Version", "1.0")
	req.Header.Set("A2A-Extensions", "https://example.com/a2a/ext")
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var rpc task.Response
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&rpc))
	require.Nil(t, rpc.Error)
	assert.JSONEq(t, `"sdk-1"`, string(rpc.ID))

	var result task.Task
	require.NoError(t, rpc.ResultInto(&result))
	assert.Equal(t, "p2-task-1", result.ID)
	assert.Equal(t, task.TaskStateCompleted, result.Status.State)
	require.Len(t, result.Artifacts, 1)
	assert.Equal(t, "p2 answer", result.Artifacts[0].Parts[0].Text)
}

func TestA2APUBACKCarriesSemanticError(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := broker.New("a2a-p2-puback", nil)
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)

	conn, reader := p2Connect(t, addr, "a2a-p2-bad-publisher")
	p2WritePacket(t, conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        7,
			TopicName:       task.TopicPrefix + "org/unit/agent" + task.RequestSuffix,
			Payload:         []byte(`{"jsonrpc":"2.0","id":"bad",`),
		}
		return pk.PublishEncode(buf)
	})
	puback := p2ReadPacket(t, reader)
	require.Equal(t, packets.Puback, puback.FixedHeader.Type)
	assert.Equal(t, task.ReasonImplementationError, puback.ReasonCode)
	var found bool
	for _, prop := range puback.Properties.User {
		if prop.Key == task.PropertyAgentError {
			assert.Equal(t, string(task.ErrorInvalidRequest), prop.Val)
			found = true
		}
	}
	assert.True(t, found, "a2a-error user property is missing from PUBACK")
}
