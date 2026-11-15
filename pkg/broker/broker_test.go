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

package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/acl"
	"github.com/turtacn/grovemq/pkg/auth"
	"github.com/turtacn/grovemq/pkg/hooks"
	"github.com/turtacn/grovemq/pkg/session"
	"github.com/turtacn/grovemq/pkg/storage"
)

// startTestBroker starts a broker on a random available port and returns the broker instance and its address.
func startTestBroker(ctx context.Context, t *testing.T) (*Broker, string) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	b := New("test-node", nil)

	go func() {
		// This loop mimics the core of StartServer but allows us to control the lifecycle.
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done(): // If context is canceled, this is an expected exit.
					return
				default: // Otherwise, it's an unexpected error during the test.
					if !t.Failed() { // Avoid logging errors on test shutdown
						t.Logf("failed to accept connection: %v", err)
					}
				}
				return
			}
			go b.handleConnection(ctx, conn)
		}
	}()

	// Cleanup the listener when the test finishes.
	t.Cleanup(func() {
		_ = listener.Close()
	})

	return b, fmt.Sprintf("tcp://%s", addr)
}

func TestBroker_Integration_ConnectDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	opts := mqtt.NewClientOptions().AddBroker(addr).SetClientID("test-client-connect")
	client := mqtt.NewClient(opts)

	token := client.Connect()
	require.True(t, token.WaitTimeout(2*time.Second), "timed out connecting")
	require.NoError(t, token.Error())
	assert.True(t, client.IsConnected())

	// The session is registered after CONNACK is sent, so it may not be visible
	// as soon as Connect() returns.
	require.Eventually(t, func() bool {
		_, err := b.sessions.Get("test-client-connect")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "session must be created in the broker")

	client.Disconnect(100)

	// Wait for the broker to process the DISCONNECT instead of a fixed sleep.
	require.Eventually(t, func() bool {
		_, err := b.sessions.Get("test-client-connect")
		return errors.Is(err, storage.ErrNotFound)
	}, 2*time.Second, 10*time.Millisecond, "session must be removed after disconnect")
}

func TestBroker_Integration_SubscribePublish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, addr := startTestBroker(ctx, t)

	msgCh := make(chan mqtt.Message, 1)
	opts := mqtt.NewClientOptions().AddBroker(addr).SetClientID("test-client-subpub")
	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		msgCh <- msg
	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	require.True(t, token.WaitTimeout(2*time.Second), "timed out connecting")
	require.NoError(t, token.Error())
	defer client.Disconnect(100)

	// Subscribe
	subToken := client.Subscribe("test/topic", 0, nil)
	require.True(t, subToken.WaitTimeout(1*time.Second), "timed out subscribing")
	require.NoError(t, subToken.Error())

	// Publish
	pubToken := client.Publish("test/topic", 0, false, "hello world")
	require.True(t, pubToken.WaitTimeout(1*time.Second), "timed out publishing")
	require.NoError(t, pubToken.Error())

	// Verify message received
	select {
	case receivedMsg := <-msgCh:
		assert.Equal(t, "test/topic", receivedMsg.Topic())
		assert.Equal(t, "hello world", string(receivedMsg.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

// TestBroker_Observability verifies that the management-plane getters
// (GetConnections/GetSessions/GetSubscriptions/GetRoutes) report the REAL
// broker state instead of hardcoded mock data.
func TestBroker_Observability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	opts := mqtt.NewClientOptions().AddBroker(addr).
		SetClientID("obs-client").
		SetUsername("alice").
		SetCleanSession(true)
	client := mqtt.NewClient(opts)
	token := client.Connect()
	require.True(t, token.WaitTimeout(2*time.Second), "timed out connecting")
	require.NoError(t, token.Error())
	defer client.Disconnect(100)

	// The broker finishes connection bookkeeping after sending CONNACK, so the
	// management-plane getters can still be empty when paho's Connect() returns.
	// Wait for the state to settle before asserting on it.
	require.Eventually(t, func() bool {
		return len(b.GetConnections()) == 1
	}, 2*time.Second, 10*time.Millisecond, "connection record must be visible")

	conns := b.GetConnections()
	require.Len(t, conns, 1)
	assert.Equal(t, "obs-client", conns[0].ClientID)
	assert.Equal(t, "alice", conns[0].Username)
	assert.Equal(t, 4, conns[0].ProtoVer)
	assert.True(t, conns[0].CleanStart)
	assert.NotEmpty(t, conns[0].PeerHost)
	assert.Greater(t, conns[0].SockPort, 0)

	// Subscribe so the session and subscription are observable.
	subToken := client.Subscribe("obs/topic", 1, nil)
	require.True(t, subToken.WaitTimeout(1*time.Second), "timed out subscribing")
	require.NoError(t, subToken.Error())

	require.Eventually(t, func() bool {
		return len(b.GetSessions()) == 1 && len(b.GetSubscriptions()) == 1
	}, 2*time.Second, 10*time.Millisecond, "session and subscription must be observable")

	sessions := b.GetSessions()
	require.Len(t, sessions, 1)
	assert.Equal(t, "obs-client", sessions[0].ClientID)
	assert.Equal(t, "alice", sessions[0].Username)
	assert.Equal(t, 1, sessions[0].SubscriptionsCount)

	subs := b.GetSubscriptions()
	require.Len(t, subs, 1)
	assert.Equal(t, "obs-client", subs[0].ClientID)
	assert.Equal(t, "obs/topic", subs[0].Topic)
	assert.Equal(t, 1, subs[0].QoS)

	// Routes getter must not panic with cluster == nil.
	assert.NotNil(t, b.GetRoutes())

	// After disconnect the live connection record must be cleared.
	client.Disconnect(100)
	require.Eventually(t, func() bool {
		return len(b.GetConnections()) == 0
	}, 2*time.Second, 50*time.Millisecond)
}

// TestBroker_ACL_Deny verifies that, once the ACL engine is enabled with a deny
// rule, the broker enforces it on publish/subscribe and records the denials, while
// non-matching traffic flows normally.
//
// NOTE: the test client speaks MQTT 3.1.1, where PUBACK/SUBACK carry no rich
// reason code (0x87 is a MQTT 5.0 construct). We therefore assert the denial via
// engine statistics and via end-to-end message delivery rather than via paho token
// errors, which are version-dependent.
func TestBroker_ACL_Deny(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	// Enable ACL enforcement with an allow-by-default posture, then add a deny
	// rule scoped to the acl/deny/# topic.
	engine := b.GetACLEngine()
	require.NotNil(t, engine)
	engine.UpdateConfig(&acl.ACLConfig{Enabled: true, DefaultAction: acl.DefaultActionAllow, LogDenials: false})
	require.NoError(t, engine.GetManager().AddRule(&acl.Rule{
		ID:         "deny-acl-test",
		Permission: acl.PermissionDeny,
		Action:     acl.ActionAll,
		Topic:      "acl/deny/#",
		Enabled:    true,
		Priority:   1,
	}))

	require.Equal(t, 0, engine.GetStats().DenyCount)

	// --- Denied subscribe: subscribing to the denied topic must be recorded. ---
	subClient := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("acl-sub"))
	require.True(t, subClient.Connect().WaitTimeout(2*time.Second))
	defer subClient.Disconnect(100)
	subClient.Subscribe("acl/deny/#", 0, nil).WaitTimeout(2 * time.Second)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 1, engine.GetStats().DenyCount, "denied subscribe must be recorded")

	// --- Denied publish: publishing to the denied topic must be recorded. ---
	pubClient := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("acl-pub"))
	require.True(t, pubClient.Connect().WaitTimeout(2*time.Second))
	defer pubClient.Disconnect(100)
	pubClient.Publish("acl/deny/x", 0, false, "should-not-be-delivered").WaitTimeout(2 * time.Second)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 2, engine.GetStats().DenyCount, "denied publish must be recorded")

	// --- Allowed traffic must flow and MUST NOT add denials. ---
	msgCh := make(chan mqtt.Message, 1)
	allowSubOpts := mqtt.NewClientOptions().AddBroker(addr).SetClientID("acl-allow-sub")
	allowSubOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) { msgCh <- msg })
	allowSub := mqtt.NewClient(allowSubOpts)
	require.True(t, allowSub.Connect().WaitTimeout(2*time.Second))
	defer allowSub.Disconnect(100)
	require.True(t, allowSub.Subscribe("acl/allow/#", 0, nil).WaitTimeout(2*time.Second))

	allowPub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("acl-allow-pub"))
	require.True(t, allowPub.Connect().WaitTimeout(2*time.Second))
	defer allowPub.Disconnect(100)
	require.True(t, allowPub.Publish("acl/allow/z", 0, false, "should-pass").WaitTimeout(2*time.Second))

	select {
	case m := <-msgCh:
		assert.Equal(t, "acl/allow/z", m.Topic())
		assert.Equal(t, "should-pass", string(m.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("allowed message was not delivered")
	}

	// Allowed traffic must not have added any denials.
	assert.Equal(t, 2, engine.GetStats().DenyCount)
}

// testRecordHook is a notification/decision test hook used to verify that the
// broker fires the hook chain on the real connection loop. It reports events on
// buffered channels so tests can observe them asynchronously.
type testRecordHook struct {
	hooks.BaseHook
	connected    chan string
	disconnected chan string
	subscribed   chan string
	published    chan string
}

func (h *testRecordHook) Name() string  { return "test-record" }
func (h *testRecordHook) Priority() int { return 0 }
func (h *testRecordHook) Points() []string {
	return []string{
		hooks.OnClientConnectedPoint,
		hooks.OnClientDisconnectedPoint,
		hooks.OnSessionSubscribedPoint,
		hooks.OnMessagePublishPoint,
	}
}
func (h *testRecordHook) OnClientConnected(ctx context.Context, c *hooks.ClientCtx) {
	if h.connected != nil {
		h.connected <- c.ClientID
	}
}
func (h *testRecordHook) OnClientDisconnected(ctx context.Context, c *hooks.ClientCtx) {
	if h.disconnected != nil {
		h.disconnected <- c.ClientID
	}
}
func (h *testRecordHook) OnSessionSubscribed(ctx context.Context, c *hooks.SubscriptionCtx) {
	if h.subscribed != nil {
		h.subscribed <- c.Topic
	}
}
func (h *testRecordHook) OnMessagePublish(ctx context.Context, c *hooks.MessagePublishCtx) (*hooks.MessagePublishResult, error) {
	if h.published != nil {
		h.published <- c.Topic
	}
	return &hooks.MessagePublishResult{Allow: true}, nil
}

// TestBroker_Hooks_Fire verifies that registering a hook on a real broker causes
// connect / subscribe / publish / disconnect events to be delivered through the
// chain. This proves "add a capability without editing the core loop".
func TestBroker_Hooks_Fire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	rec := &testRecordHook{
		connected:    make(chan string, 4),
		disconnected: make(chan string, 4),
		subscribed:   make(chan string, 4),
		published:    make(chan string, 4),
	}
	b.GetHooks().Register(rec)

	client := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("hook-fire"))
	require.True(t, client.Connect().WaitTimeout(2*time.Second))
	defer client.Disconnect(100)

	require.True(t, client.Subscribe("hook/fire/topic", 0, nil).WaitTimeout(time.Second))
	require.True(t, client.Publish("hook/fire/topic", 0, false, "hi").WaitTimeout(time.Second))
	time.Sleep(100 * time.Millisecond)

	require.Eventually(t, func() bool {
		return len(rec.connected) >= 1 && len(rec.subscribed) >= 1 && len(rec.published) >= 1
	}, 2*time.Second, 20*time.Millisecond)

	assert.Equal(t, "hook-fire", <-rec.connected)
	assert.Equal(t, "hook/fire/topic", <-rec.subscribed)
	assert.Equal(t, "hook/fire/topic", <-rec.published)

	client.Disconnect(100)
	require.Eventually(t, func() bool {
		return len(rec.disconnected) >= 1
	}, 2*time.Second, 20*time.Millisecond)
	assert.Equal(t, "hook-fire", <-rec.disconnected)
}

// TestBroker_Hooks_RateLimitDeny enables the built-in rate-limit hook on a real
// broker and asserts that once the per-client burst is exhausted, further
// publishes are denied (and therefore NOT delivered to a subscriber).
func TestBroker_Hooks_RateLimitDeny(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	rl, ok := b.GetHooks().Get("rate_limit")
	require.True(t, ok)
	rlHook, ok := rl.(*hooks.RateLimitHook)
	require.True(t, ok)
	// 1 token of burst, then a very slow refill: the first publish is allowed,
	// the next is denied.
	rlHook.SetDefaultRate(0.01, 1)

	// Subscriber on the target topic to detect any delivered message.
	msgCh := make(chan mqtt.Message, 4)
	sub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("rl-sub"))
	require.True(t, sub.Connect().WaitTimeout(2*time.Second))
	defer sub.Disconnect(100)
	require.True(t, sub.Subscribe("rl/topic", 0, func(_ mqtt.Client, m mqtt.Message) {
		msgCh <- m
	}).WaitTimeout(time.Second))

	pub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("rl-pub"))
	require.True(t, pub.Connect().WaitTimeout(2*time.Second))
	defer pub.Disconnect(100)

	// First publish should be allowed and delivered.
	require.True(t, pub.Publish("rl/topic", 0, false, "one").WaitTimeout(time.Second))
	select {
	case m := <-msgCh:
		assert.Equal(t, "one", string(m.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("first publish should have been delivered")
	}

	// Second publish should be DENIED by the rate-limit hook: not delivered.
	require.True(t, pub.Publish("rl/topic", 0, false, "two").WaitTimeout(time.Second))
	select {
	case m := <-msgCh:
		t.Fatalf("rate-limited publish was delivered: %s", string(m.Payload()))
	case <-time.After(1 * time.Second):
		// Expected: nothing delivered.
	}
}

// TestBroker_Hooks_Forward configures the built-in forward hook on a real broker
// and verifies that a publish matching a forward rule is republished to the
// target topic (bridging/forwarding) without modifying the core publish loop.
func TestBroker_Hooks_Forward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	fwd, ok := b.GetHooks().Get("forward")
	require.True(t, ok)
	fwdHook, ok := fwd.(*hooks.ForwardHook)
	require.True(t, ok)
	fwdHook.AddRule(hooks.ForwardRule{TopicFilter: "sensors/#", TargetTopic: "fwd/all", QoS: 0})

	// Subscriber on the forwarded target topic.
	msgCh := make(chan mqtt.Message, 4)
	sub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("fwd-sub"))
	require.True(t, sub.Connect().WaitTimeout(2*time.Second))
	defer sub.Disconnect(100)
	require.True(t, sub.Subscribe("fwd/all", 0, func(_ mqtt.Client, m mqtt.Message) {
		msgCh <- m
	}).WaitTimeout(time.Second))
	time.Sleep(100 * time.Millisecond)

	pub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("fwd-pub"))
	require.True(t, pub.Connect().WaitTimeout(2*time.Second))
	defer pub.Disconnect(100)

	// Publish to a topic matching the forward filter.
	require.True(t, pub.Publish("sensors/temp", 0, false, "bridged").WaitTimeout(time.Second))

	select {
	case m := <-msgCh:
		assert.Equal(t, "fwd/all", m.Topic())
		assert.Equal(t, "bridged", string(m.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("forwarded message was not delivered to target topic")
	}
}

// TestBroker_WebSocket_ConnectPublishSubscribe verifies that the MQTT-over-
// WebSocket gateway accepts a WebSocket client (path /mqtt, "mqtt"
// subprotocol), runs it through the SAME handleConnection loop as TCP clients,
// and delivers a published message back to a subscriber on that connection.
func TestBroker_WebSocket_ConnectPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, _ := startTestBroker(ctx, t)

	// Grab a free port for the WebSocket gateway, then start it there.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	wsAddr := l.Addr().String()
	require.NoError(t, l.Close())

	go func() {
		_ = b.StartWebSocketServer(ctx, wsAddr)
	}()

	// Wait until the gateway is accepting TCP connections.
	require.Eventually(t, func() bool {
		c, derr := net.Dial("tcp", wsAddr)
		if derr == nil {
			_ = c.Close()
			return true
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)

	// paho MQTT client over WebSocket. paho v1.5.1 negotiates the "mqtt"
	// subprotocol by default, which matches the gateway's upgrade policy.
	opts := mqtt.NewClientOptions().
		AddBroker("ws://" + wsAddr + "/mqtt").
		SetClientID("ws-client").
		SetConnectTimeout(2 * time.Second)
	client := mqtt.NewClient(opts)
	connectToken := client.Connect()
	require.True(t, connectToken.WaitTimeout(2*time.Second), "timed out connecting over websocket")
	require.NoError(t, connectToken.Error())
	defer client.Disconnect(100)

	// The broker sends CONNACK before it finishes registering the session, so
	// paho's Connect() can return before the session is visible in the store.
	// Wait for the registration instead of racing it.
	require.Eventually(t, func() bool {
		_, err := b.sessions.Get("ws-client")
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "websocket session must be registered like a TCP client")

	// Subscribe then publish on the same connection; message must echo back.
	msgCh := make(chan mqtt.Message, 1)
	require.True(t, client.Subscribe("ws/topic", 0, func(_ mqtt.Client, m mqtt.Message) {
		msgCh <- m
	}).WaitTimeout(time.Second))

	require.True(t, client.Publish("ws/topic", 0, false, "over-ws").WaitTimeout(time.Second))

	select {
	case m := <-msgCh:
		assert.Equal(t, "ws/topic", m.Topic())
		assert.Equal(t, "over-ws", string(m.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("message was not delivered over the websocket gateway")
	}

	// Disconnect must clear the live connection record.
	client.Disconnect(100)
	require.Eventually(t, func() bool {
		return len(b.GetConnections()) == 0
	}, 2*time.Second, 50*time.Millisecond)
}

// TestBroker_QoS1DeliveryAndAck verifies the full QoS 1 outbound path: a QoS 1
// publish to a QoS 1 subscriber is delivered with a unique, non-zero PacketID,
// and once the subscriber acknowledges (PUBACK) the broker clears its inflight
// entry.
func TestBroker_QoS1DeliveryAndAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, addr := startTestBroker(ctx, t)

	const subID = "qos1-sub"
	var received int32
	var receivedPID uint16
	subOpts := mqtt.NewClientOptions().AddBroker(addr).SetClientID(subID).SetCleanSession(true)
	subOpts.SetDefaultPublishHandler(func(_ mqtt.Client, m mqtt.Message) {
		receivedPID = m.MessageID()
		atomic.AddInt32(&received, 1)
	})
	sub := mqtt.NewClient(subOpts)
	require.True(t, sub.Connect().WaitTimeout(2*time.Second))
	defer sub.Disconnect(100)
	require.True(t, sub.Subscribe("qos/topic", 1, nil).WaitTimeout(2*time.Second))

	pub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(addr).SetClientID("qos1-pub").SetCleanSession(true))
	require.True(t, pub.Connect().WaitTimeout(2*time.Second))
	defer pub.Disconnect(100)

	tok := pub.Publish("qos/topic", 1, false, []byte("hello-qos1"))
	require.True(t, tok.WaitTimeout(2*time.Second))
	require.NoError(t, tok.Error())

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&received) == 1
	}, 3*time.Second, 20*time.Millisecond, "subscriber should receive the QoS1 message")
	require.NotZero(t, receivedPID, "QoS1 PUBLISH must carry a non-zero PacketID")

	// After the subscriber's PUBACK is processed, the broker's inflight entry
	// for the subscriber must be cleared.
	require.Eventually(t, func() bool {
		return b.persistentSessionMgr.CountInflight(subID) == 0
	}, 3*time.Second, 20*time.Millisecond, "QoS1 inflight must be cleared after PUBACK")
}

// TestBroker_NoClientIDAuthBypass guards against a remote authentication
// bypass: the broker used to skip the auth chain for client IDs with certain
// "fuzz"/"qos-test" prefixes. Clients choose their own client ID, so any such
// prefix allow-list lets unauthenticated clients straight in.
func TestBroker_NoClientIDAuthBypass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, addr := startTestBroker(ctx, t)

	memAuth := auth.NewMemoryAuthenticator()
	require.NoError(t, memAuth.AddUser("known", "secret", auth.HashPlain))
	b.GetAuthChain().Replace([]auth.Authenticator{memAuth}, true)
	b.GetAuthChain().SetAllowAnonymous(false)

	attacker := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID("fuzz-evil-client").
		SetUsername("attacker").
		SetPassword("wrong-password").
		SetConnectTimeout(2 * time.Second))
	token := attacker.Connect()
	require.True(t, token.WaitTimeout(3*time.Second), "CONNECT must be answered with a refusal")
	require.Error(t, token.Error(), "a fuzz-prefixed client ID must not bypass authentication")

	// A configured user must still connect, including with a look-alike ID.
	good := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker(addr).
		SetClientID("fuzz-looking-but-valid").
		SetUsername("known").
		SetPassword("secret").
		SetConnectTimeout(2 * time.Second))
	goodToken := good.Connect()
	require.True(t, goodToken.WaitTimeout(3*time.Second))
	require.NoError(t, goodToken.Error(), "valid credentials must work with any client ID")
	good.Disconnect(100)
}

// readSessionPublish registers a session with the given protocol version,
// pushes one QoS 0 publish through its mailbox and returns the raw bytes the
// session actor wrote to the connection.
func readSessionPublish(t *testing.T, version byte) []byte {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := New("test-node", nil)
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	mb := b.registerSession(ctx, "version-client", serverConn, version, nil)
	require.NotNil(t, mb)
	mb.Send(session.Publish{Topic: "a/b", Payload: []byte("X"), QoS: 0})

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(3*time.Second)))
	buf := make([]byte, 256)
	n, err := clientConn.Read(buf)
	require.NoError(t, err)
	return buf[:n]
}

// TestBroker_SessionUsesNegotiatedProtocolVersion guards against a regression
// where registerSession hard-coded MQTT 3.1.1, so a v5 client's outbound
// PUBLISH lacked the mandatory MQTT 5.0 Property Length byte and could not be
// parsed by a conformant client.
func TestBroker_SessionUsesNegotiatedProtocolVersion(t *testing.T) {
	// Fixed header(1) + remaining length(1) + topic length(2) + "a/b"(3) = 7.
	const propertyLengthOffset = 1 + 1 + 2 + len("a/b")

	v4 := readSessionPublish(t, 4)
	require.Greater(t, len(v4), propertyLengthOffset)
	require.Equal(t, byte('X'), v4[propertyLengthOffset],
		"MQTT 3.1.1 PUBLISH has no property block; payload starts right after the topic")

	v5 := readSessionPublish(t, 5)
	require.Greater(t, len(v5), propertyLengthOffset+1)
	require.Equal(t, byte(0x00), v5[propertyLengthOffset],
		"MQTT 5.0 PUBLISH must start its variable header properties with Property Length 0")
	require.Equal(t, byte('X'), v5[propertyLengthOffset+1],
		"payload must follow the MQTT 5.0 property length")
}
