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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

const (
	defaultMQTTTimeout = 5 * time.Second
	subscribePacketID  = 1
	publishPacketID    = 2
)

// MQTTConfig configures the HTTP-to-MQTT bridge.
type MQTTConfig struct {
	BrokerURL string
	ClientID  string
	Username  string
	Password  string
	Timeout   time.Duration
}

// MQTTBridge implements Bridge over a raw MQTT 5.0 connection. Each request
// uses a short-lived connection subscribed to a unique response topic, which
// makes concurrent gateway calls independent and avoids shared session state.
type MQTTBridge struct {
	cfg       MQTTConfig
	gatewayID string
}

// NewMQTTBridge creates an MQTT bridge. An empty ClientID gets a random value.
func NewMQTTBridge(cfg MQTTConfig) (*MQTTBridge, error) {
	if strings.TrimSpace(cfg.BrokerURL) == "" {
		return nil, fmt.Errorf("a2a gateway: MQTT broker URL is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultMQTTTimeout
	}
	if cfg.ClientID == "" {
		id, err := randomHex(6)
		if err != nil {
			return nil, err
		}
		cfg.ClientID = "a2a-gateway-" + id
	}
	gwID, err := randomHex(4)
	if err != nil {
		return nil, err
	}
	return &MQTTBridge{cfg: cfg, gatewayID: gwID}, nil
}

// Request publishes one A2A JSON-RPC request and waits for the correlated
// response on a dedicated response topic.
func (b *MQTTBridge) Request(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Response, error) {
	if b == nil {
		return nil, fmt.Errorf("a2a gateway: nil MQTT bridge")
	}
	if req == nil {
		return nil, fmt.Errorf("a2a gateway: nil request")
	}

	requestID, err := correlationID(req, b.gatewayID)
	if err != nil {
		return nil, err
	}
	responseTopic := fmt.Sprintf("$a2a/v1/gateway/%s/responses/%s", b.gatewayID, requestID)
	requestTopic := fmt.Sprintf("$a2a/v1/%s/requests", address)

	// The authenticated HTTP principal is promoted to the MQTT connection user
	// so the broker's identity policy sees a real, authenticated MQTT identity
	// rather than a user property a client could forge on the wire.
	connectUsername := b.cfg.Username
	if principal := strings.TrimSpace(string(props[PropertyPrincipal])); principal != "" {
		connectUsername = principal
	}

	conn, err := dialContext(ctx, b.cfg)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(b.cfg.Timeout))
	}
	reader := bufio.NewReader(conn)

	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Connect},
			ProtocolVersion: 5,
			Connect: packets.ConnectParams{
				ProtocolName:     []byte("MQTT"),
				ClientIdentifier: b.cfg.ClientID + "-" + requestID[:8],
				Clean:            true,
				Keepalive:        30,
				UsernameFlag:     connectUsername != "",
				Username:         []byte(connectUsername),
				PasswordFlag:     b.cfg.Password != "",
				Password:         []byte(b.cfg.Password),
			},
		}
		return pk.ConnectEncode(buf)
	}); err != nil {
		return nil, fmt.Errorf("a2a gateway: write CONNECT: %w", err)
	}
	pk, err := readPacket(reader)
	if err != nil {
		return nil, fmt.Errorf("a2a gateway: read CONNACK: %w", err)
	}
	if pk.FixedHeader.Type != packets.Connack || pk.ReasonCode != 0 {
		return nil, fmt.Errorf("a2a gateway: MQTT connect rejected (reason 0x%02x)", pk.ReasonCode)
	}

	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        subscribePacketID,
			Filters: packets.Subscriptions{{
				Filter: responseTopic,
				Qos:    1,
			}},
		}
		return pk.SubscribeEncode(buf)
	}); err != nil {
		return nil, fmt.Errorf("a2a gateway: write SUBSCRIBE: %w", err)
	}
	pk, err = readPacket(reader)
	if err != nil {
		return nil, fmt.Errorf("a2a gateway: read SUBACK: %w", err)
	}
	if pk.FixedHeader.Type != packets.Suback || len(pk.ReasonCodes) == 0 || pk.ReasonCodes[0] >= 0x80 {
		return nil, fmt.Errorf("a2a gateway: MQTT subscribe rejected")
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("a2a gateway: encode request: %w", err)
	}
	userProps := make([]packets.UserProperty, 0, len(props)+2)
	contentType := task.ContentType
	for key, value := range props {
		if key == "Content-Type" {
			contentType = string(value)
			continue
		}
		if key == PropertyPrincipal {
			continue // carried as the MQTT user name instead
		}
		userProps = append(userProps, packets.UserProperty{Key: key, Val: string(value)})
	}
	if !hasUserProperty(userProps, "A2A-Version") {
		userProps = append(userProps, packets.UserProperty{Key: "A2A-Version", Val: "1.0"})
	}

	if err := writePacket(conn, func(buf *bytes.Buffer) error {
		pk := &packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
			ProtocolVersion: 5,
			PacketID:        publishPacketID,
			TopicName:       requestTopic,
			Payload:         payload,
			Mods:            packets.Mods{AllowResponseInfo: true},
			Properties: packets.Properties{
				ContentType:     contentType,
				ResponseTopic:   responseTopic,
				CorrelationData: []byte(requestID),
				User:            userProps,
			},
		}
		return pk.PublishEncode(buf)
	}); err != nil {
		return nil, fmt.Errorf("a2a gateway: write PUBLISH: %w", err)
	}

	for {
		pk, err = readPacket(reader)
		if err != nil {
			return nil, fmt.Errorf("a2a gateway: wait for response: %w", err)
		}
		switch pk.FixedHeader.Type {
		case packets.Puback:
			continue
		case packets.Publish:
			if pk.FixedHeader.Qos == 1 {
				ack := &packets.Packet{
					FixedHeader:     packets.FixedHeader{Type: packets.Puback},
					ProtocolVersion: 5,
					PacketID:        pk.PacketID,
					ReasonCode:      task.ReasonSuccess,
				}
				if err := writePacket(conn, func(buf *bytes.Buffer) error { return ack.PubackEncode(buf) }); err != nil {
					return nil, fmt.Errorf("a2a gateway: write PUBACK: %w", err)
				}
			}
			if pk.TopicName != responseTopic || string(pk.Properties.CorrelationData) != requestID {
				continue
			}
			resp, err := task.ParseResponse(pk.Payload)
			if err != nil {
				return nil, fmt.Errorf("a2a gateway: invalid response payload: %w", err)
			}
			return resp, nil
		case packets.Pingresp, packets.Suback:
			continue
		default:
			continue
		}
	}
}

func dialContext(ctx context.Context, cfg MQTTConfig) (net.Conn, error) {
	address := cfg.BrokerURL
	if strings.Contains(address, "://") {
		u, err := url.Parse(address)
		if err != nil {
			return nil, fmt.Errorf("a2a gateway: invalid broker URL: %w", err)
		}
		if u.Scheme != "tcp" && u.Scheme != "mqtt" {
			return nil, fmt.Errorf("a2a gateway: unsupported broker scheme %q", u.Scheme)
		}
		address = u.Host
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("a2a gateway: dial broker %s: %w", address, err)
	}
	return conn, nil
}

func writePacket(w io.Writer, encode func(*bytes.Buffer) error) error {
	buf := new(bytes.Buffer)
	if err := encode(buf); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// readPacket decodes one MQTT packet from the raw connection. The mochi
// packet codec does not expose a stream reader, so the fixed header and
// remaining bytes are read here and handed to the matching Decode method.
func readPacket(r *bufio.Reader) (*packets.Packet, error) {
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
	case packets.Pubrec:
		err = pk.PubrecDecode(body)
	case packets.Pubrel:
		err = pk.PubrelDecode(body)
	case packets.Pubcomp:
		err = pk.PubcompDecode(body)
	case packets.Suback:
		err = pk.SubackDecode(body)
	case packets.Pingresp:
		// No payload.
	default:
		// Other packets are irrelevant to the gateway request flow.
	}
	if err != nil {
		return nil, fmt.Errorf("decode MQTT packet type %d: %w", fh.Type, err)
	}
	return pk, nil
}

func randomHex(bytesCount int) (string, error) {
	buf := make([]byte, bytesCount)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("a2a gateway: generate random id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func correlationID(req *task.Request, gatewayID string) (string, error) {
	var value interface{}
	if err := json.Unmarshal(req.ID, &value); err != nil {
		return "", fmt.Errorf("a2a gateway: decode request id: %w", err)
	}
	raw := fmt.Sprintf("%v", value)
	id, err := randomHex(6)
	if err != nil {
		return "", err
	}
	// Random suffix prevents collisions between SDKs that reuse request IDs.
	return gatewayID + "-" + sanitizeTopicSegment(raw) + "-" + id, nil
}

func sanitizeTopicSegment(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, s)
	if len(s) > 40 {
		s = s[:40]
	}
	if s == "" {
		return "id"
	}
	return s
}

func hasUserProperty(props []packets.UserProperty, key string) bool {
	for _, p := range props {
		if p.Key == key {
			return true
		}
	}
	return false
}
