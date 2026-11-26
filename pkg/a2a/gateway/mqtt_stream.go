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
	"strings"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

const (
	streamSubscribeID = 3
	streamPublishID   = 4
)

// MQTTStreamBridge implements StreamBridge over a long-lived MQTT 5 session.
// It subscribes to the task's status and artifact topics before publishing the
// request, so no progress event is lost between request and subscription.
type MQTTStreamBridge struct {
	cfg  MQTTConfig
	hub  *task.StreamHub
	gwID string
}

// NewMQTTStreamBridge creates a streaming bridge sharing gatewayID with the
// request bridge so correlation IDs stay consistent.
func NewMQTTStreamBridge(cfg MQTTConfig, gatewayID string) (*MQTTStreamBridge, error) {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		return nil, fmt.Errorf("a2a gateway: MQTT broker URL is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultMQTTTimeout
	}
	if gatewayID == "" {
		var err error
		gatewayID, err = randomHex(4)
		if err != nil {
			return nil, err
		}
	}
	return &MQTTStreamBridge{cfg: cfg, hub: task.NewStreamHub(64), gwID: gatewayID}, nil
}

// Hub exposes the in-process event hub so embedded agents can also publish.
func (b *MQTTStreamBridge) Hub() *task.StreamHub { return b.hub }

// Stream implements StreamBridge. It returns a Stream that delivers TaskStatus
// and Artifact events from the agent's MQTT topics and closes on terminal state.
func (b *MQTTStreamBridge) Stream(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Stream, error) {
	if b == nil || req == nil {
		return nil, fmt.Errorf("a2a gateway: nil stream bridge or request")
	}
	params, err := ParseStreamParams(req)
	if err != nil {
		return nil, err
	}
	taskID := params.TaskID
	if taskID == "" {
		taskID = "stream-" + randomID()
		if err := task.ValidateTaskID(taskID); err != nil {
			return nil, err
		}
	}
	statusTopic, err := task.StatusTopic(address, taskID)
	if err != nil {
		return nil, err
	}
	artifactTopic, err := task.ArtifactTopic(address, taskID)
	if err != nil {
		return nil, err
	}
	requestTopic := fmt.Sprintf("%s%s%s", task.TopicPrefix, address, task.RequestSuffix)
	responseTopic := fmt.Sprintf("$a2a/v1/gateway/%s/responses/%s", b.gwID, taskID)

	conn, err := dialContext(ctx, b.cfg)
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = conn.Close() }

	connectUser := b.cfg.Username
	if principal := strings.TrimSpace(string(props[PropertyPrincipal])); principal != "" {
		connectUser = principal
	}
	deadline := time.Now().Add(b.cfg.Timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
	reader := bufio.NewReader(conn)

	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		return (&packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: b.cfg.ClientID + "-stream-" + taskID[:minLen(len(taskID), 8)],
				Clean:            true,
				Keepalive:        30,
				UsernameFlag:     connectUser != "",
				Username:         []byte(connectUser),
				PasswordFlag:     b.cfg.Password != "",
				Password:         []byte(b.cfg.Password),
			},
		}).ConnectEncode(buf)
	}); err != nil {
		cleanup()
		return nil, fmt.Errorf("a2a gateway: stream CONNECT: %w", err)
	}
	pk, err := readPacket(reader)
	if err != nil || pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		cleanup()
		return nil, fmt.Errorf("a2a gateway: stream CONNACK rejected: %v", err)
	}

	// Subscribe to both event topics before publishing the request.
	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		return (&packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        streamSubscribeID,
			Filters: packets.Subscriptions{
				{Filter: statusTopic, Qos: 1},
				{Filter: artifactTopic, Qos: 1},
			},
		}).SubscribeEncode(buf)
	}); err != nil {
		cleanup()
		return nil, fmt.Errorf("a2a gateway: stream SUBSCRIBE: %w", err)
	}
	pk, err = readPacket(reader)
	if err != nil || pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) != 2 || pk.ReasonCodes[0] >= 0x80 || pk.ReasonCodes[1] >= 0x80 {
		cleanup()
		return nil, fmt.Errorf("a2a gateway: stream SUBACK rejected: %v", err)
	}

	stream, err := b.hub.Subscribe(address, taskID, nil)
	if err != nil {
		cleanup()
		return nil, err
	}

	userProps := make([]packets.UserProperty, 0, len(props)+1)
	contentType := task.ContentType
	for key, value := range props {
		if key == "Content-Type" {
			contentType = string(value)
			continue
		}
		if key == PropertyPrincipal {
			continue
		}
		userProps = append(userProps, packets.UserProperty{Key: key, Val: string(value)})
	}
	if !hasUserProperty(userProps, "A2A-Version") {
		userProps = append(userProps, packets.UserProperty{Key: "A2A-Version", Val: "1.0"})
	}
	requestJSON, err := json.Marshal(req)
	if err != nil {
		stream.Close()
		cleanup()
		return nil, err
	}
	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		return (&packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        streamPublishID,
			TopicName:       requestTopic,
			Payload:         requestJSON,
			Properties: packets.Properties{
				ContentType:   contentType,
				ResponseTopic: responseTopic,
				User:          userProps,
			},
		}).PublishEncode(buf)
	}); err != nil {
		stream.Close()
		cleanup()
		return nil, fmt.Errorf("a2a gateway: stream PUBLISH: %w", err)
	}

	// Relay MQTT events into the in-process hub until the task terminates or
	// the caller cancels. The goroutine owns the connection and always closes it.
	go b.relay(ctx, stream, conn, reader, statusTopic, artifactTopic)
	return stream, nil
}

func (b *MQTTStreamBridge) relay(ctx context.Context, stream *task.Stream, conn net.Conn, reader *bufio.Reader, statusTopic, artifactTopic string) {
	defer conn.Close()
	defer stream.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stream.Done():
			return
		default:
		}
		pk, err := readPacket(reader)
		if err != nil {
			return
		}
		if pk.FixedHeader.Type != packets.Publish {
			continue
		}
		if pk.FixedHeader.Qos == 1 {
			ack := &packets.Packet{
				FixedHeader:     packets.FixedHeader{Type: packets.Puback},
				ProtocolVersion: 5,
				PacketID:        pk.PacketID,
				ReasonCode:      task.ReasonSuccess,
			}
			if err := writePacket(conn, func(buf *bytes.Buffer) error { return ack.PubackEncode(buf) }); err != nil {
				return
			}
		}
		switch pk.TopicName {
		case statusTopic:
			var ev task.StreamEvent
			if err := json.Unmarshal(pk.Payload, &ev); err != nil || ev.Task == nil {
				// Agents may publish a bare Task as on the request/response path.
				var bare task.Task
				if err := json.Unmarshal(pk.Payload, &bare); err != nil || bare.ID == "" {
					continue
				}
				ev.Task = &bare
			}
			_ = b.hub.PublishStatus(stream.Address(), ev.Task)
		case artifactTopic:
			var ev task.StreamEvent
			if err := json.Unmarshal(pk.Payload, &ev); err != nil || ev.Artifact == nil {
				continue
			}
			_ = b.hub.PublishArtifact(stream.Address(), stream.TaskID(), ev.Artifact)
		}
	}
}

func pkType(pk *packets.Packet) byte {
	if pk == nil {
		return 0
	}
	return pk.FixedHeader.Type
}
func pkTopic(pk *packets.Packet) string {
	if pk == nil {
		return ""
	}
	return pk.TopicName
}

func minLen(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func randomID() string {
	id, err := randomHex(6)
	if err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return id
}
