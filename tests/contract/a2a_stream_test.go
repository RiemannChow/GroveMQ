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
	"strings"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/gateway"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/broker"
)

// TestA2AContractStreamingEndToEnd drives message/stream from an HTTP SDK
// through the MQTT request topic and back through the task's status/artifacts
// topics. The SSE stream must deliver progress and terminate on a final event.
func TestA2AContractStreamingEndToEnd(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := broker.New("a2a-stream-contract", nil)
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)

	const agentAddress = "org/unit/agent"
	requestTopic := task.TopicPrefix + agentAddress + task.RequestSuffix

	// A raw MQTT 5 agent subscribes to requests, then publishes status and
	// artifact events to the task topics derived from the request's taskId.
	ready := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		conn, err := netDialWithTimeout(addr, time.Second)
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
		reader := bufio.NewReader(conn)
		writeCONNECT(t, conn, "a2a-stream-agent")
		if _, err := p2ReadPacketSafe(reader); err != nil {
			errCh <- err
			return
		}
		writeSUBSCRIBE(t, conn, requestTopic)
		if _, err := p2ReadPacketSafe(reader); err != nil {
			errCh <- err
			return
		}
		close(ready)

		pk, err := p2ReadPacketSafe(reader)
		if err != nil || pk.FixedHeader.Type != packets.Publish {
			errCh <- fmt.Errorf("no request received: %v", err)
			return
		}
		if pk.FixedHeader.Qos == 1 {
			ack := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: 5, PacketID: pk.PacketID, ReasonCode: task.ReasonSuccess,
			}
			_ = p2WritePacketSafe(conn, ack.PubackEncode)
		}

		// The gateway derives a stream-{id} task ID; read it from the request
		// params and use it to build the event topics.
		var req task.Request
		if err := json.Unmarshal(pk.Payload, &req); err != nil {
			errCh <- err
			return
		}
		taskID := streamTaskIDFromRequest(&req)
		statusTopic, err := task.StatusTopic(agentAddress, taskID)
		if err != nil {
			errCh <- err
			return
		}
		artifactTopic, err := task.ArtifactTopic(agentAddress, taskID)
		if err != nil {
			errCh <- err
			return
		}
		publishStreamEvent(t, conn, reader, 10, statusTopic, workingEvent(taskID))
		publishStreamEvent(t, conn, reader, 11, artifactTopic, artifactEvent(taskID))
		publishStreamEvent(t, conn, reader, 12, statusTopic, completedEvent(taskID))
	}()
	select {
	case <-ready:
	case err := <-errCh:
		t.Fatalf("stream agent setup failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("stream agent did not subscribe")
	}

	bridge, err := gateway.NewMQTTBridge(gateway.MQTTConfig{BrokerURL: "tcp://" + addr, Timeout: 5 * time.Second})
	require.NoError(t, err)
	streamBridge, err := gateway.NewMQTTStreamBridge(gateway.MQTTConfig{BrokerURL: "tcp://" + addr, Timeout: 5 * time.Second}, "contract")
	require.NoError(t, err)
	handler := gateway.NewStreamHandler(bridge, streamBridge, nil, nil)
	server := httptest.NewServer(handler)
	defer server.Close()

	body := `{"jsonrpc":"2.0","id":"stream-1","method":"message/stream","params":{"taskId":"stream-contract-1","message":{"messageId":"m-1","role":"ROLE_USER","parts":[{"text":"start"}]}}}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+gateway.PathPrefix+agentAddress, bytes.NewBufferString(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", task.ContentType)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	reader := bufio.NewReader(resp.Body)
	var events []task.StreamEvent
	for i := 0; i < 3; i++ {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(line, "event: "), line)
		data, err := reader.ReadString('\n')
		require.NoError(t, err)
		var ev task.StreamEvent
		require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(data, "data: "))), &ev))
		events = append(events, ev)
		_, _ = reader.ReadString('\n')
	}
	require.Len(t, events, 3)
	assert.Equal(t, task.TaskStateWorking, events[0].Task.Status.State)
	assert.Equal(t, "a-stream", events[1].Artifact.ArtifactID)
	assert.True(t, events[2].Final)
	assert.Equal(t, task.TaskStateCompleted, events[2].Task.Status.State)
}

// Helper wrappers keep the test body focused on the A2A flow.
type netConn = net.Conn

func netDialWithTimeout(addr string, d time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, d)
}

func writeCONNECT(t *testing.T, conn net.Conn, clientID string) {
	t.Helper()
	p2WritePacket(t, conn, (&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect:         packets.ConnectParams{ProtocolName: []byte("MQTT"), ClientIdentifier: clientID, Clean: true, Keepalive: 30},
	}).ConnectEncode)
}

func writeSUBSCRIBE(t *testing.T, conn net.Conn, topic string) {
	t.Helper()
	p2WritePacket(t, conn, (&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 5, PacketID: 1,
		Filters: packets.Subscriptions{{Filter: topic, Qos: 1}},
	}).SubscribeEncode)
}

func publishStreamEvent(t *testing.T, conn net.Conn, reader *bufio.Reader, packetID uint16, topic string, payload []byte) {
	t.Helper()
	p2WritePacket(t, conn, (&packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
		ProtocolVersion: 5, PacketID: packetID, TopicName: topic, Payload: payload,
	}).PublishEncode)
	ack, err := p2ReadPacketSafe(reader)
	require.NoError(t, err)
	require.Equal(t, packets.Puback, ack.FixedHeader.Type)
}

func workingEvent(taskID string) []byte {
	tk := task.NewTask(taskID, "ctx")
	_ = tk.Transition(task.TaskStateWorking)
	return mustJSON(task.StreamEvent{Kind: task.StreamEventStatus, TaskID: taskID, Task: tk, Timestamp: "now"})
}

func completedEvent(taskID string) []byte {
	tk := task.NewTask(taskID, "ctx")
	_ = tk.Transition(task.TaskStateWorking)
	_ = tk.Transition(task.TaskStateCompleted)
	return mustJSON(task.StreamEvent{Kind: task.StreamEventStatus, TaskID: taskID, Task: tk, Final: true, Timestamp: "now"})
}

func artifactEvent(taskID string) []byte {
	return mustJSON(task.StreamEvent{
		Kind: task.StreamEventArtifact, TaskID: taskID,
		Artifact:  &task.Artifact{ArtifactID: "a-stream", Parts: []task.Part{{Text: "progress"}}},
		Timestamp: "now",
	})
}

func mustJSON(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func streamTaskIDFromRequest(req *task.Request) string {
	var params struct {
		TaskID string `json:"taskId"`
	}
	_ = json.Unmarshal(req.Params, &params)
	// The streaming bridge uses a generated stream- prefix when no taskId is
	// supplied. Read the actual topic from the response topic is not possible
	// here, so the request carries an explicit taskId.
	return params.TaskID
}
