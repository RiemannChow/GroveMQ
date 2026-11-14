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

package hooks

import (
	"bytes"
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test hooks ---------------------------------------------------------------

// recordingHook is a notification-only test hook that reports events on channels.
type recordingHook struct {
	BaseHook
	name         string
	prio         int
	connected    chan string
	disconnected chan string
	subscribed   chan string
	published    chan string
}

func (h *recordingHook) Name() string  { return h.name }
func (h *recordingHook) Priority() int { return h.prio }
func (h *recordingHook) Points() []string {
	return []string{OnClientConnectedPoint, OnClientDisconnectedPoint,
		OnSessionSubscribedPoint, OnMessagePublishPoint}
}
func (h *recordingHook) OnClientConnected(ctx context.Context, c *ClientCtx) {
	if h.connected != nil {
		h.connected <- c.ClientID
	}
}
func (h *recordingHook) OnClientDisconnected(ctx context.Context, c *ClientCtx) {
	if h.disconnected != nil {
		h.disconnected <- c.ClientID
	}
}
func (h *recordingHook) OnSessionSubscribed(ctx context.Context, c *SubscriptionCtx) {
	if h.subscribed != nil {
		h.subscribed <- c.Topic
	}
}
func (h *recordingHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	if h.published != nil {
		h.published <- c.Topic
	}
	return &MessagePublishResult{Allow: true}, nil
}

// decisionHook is a decision test hook that unconditionally returns a fixed verdict.
type decisionHook struct {
	BaseHook
	name   string
	prio   int
	allow  bool
	reason string
}

func (h *decisionHook) Name() string  { return h.name }
func (h *decisionHook) Priority() int { return h.prio }
func (h *decisionHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	return &MessagePublishResult{Allow: h.allow, Reason: h.reason}, nil
}
func (h *decisionHook) OnClientConnect(ctx context.Context, c *ClientConnectCtx) (*Decision, error) {
	return &Decision{Allow: h.allow, Reason: h.reason}, nil
}

func TestManager_PriorityOrder(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	low := &recordingHook{name: "low", prio: 50}
	high := &recordingHook{name: "high", prio: 1}
	mid := &recordingHook{name: "mid", prio: 25}

	m.Register(low)
	m.Register(high)
	m.Register(mid)

	infos := m.List()
	require.Len(t, infos, 3)
	assert.Equal(t, "high", infos[0].Name)
	assert.Equal(t, "mid", infos[1].Name)
	assert.Equal(t, "low", infos[2].Name)

	// Re-registering the same name replaces rather than duplicates.
	m.Register(&recordingHook{name: "mid", prio: 25})
	assert.Len(t, m.List(), 3)

	// Get + Unregister.
	h, ok := m.Get("high")
	require.True(t, ok)
	assert.Equal(t, "high", h.Name())
	m.Unregister("high")
	_, ok = m.Get("high")
	assert.False(t, ok)
}

func TestManager_OnMessagePublishDeny(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	m.Register(&decisionHook{name: "allowfirst", prio: 1, allow: true})
	m.Register(&decisionHook{name: "denysecond", prio: 2, allow: false, reason: "nope"})

	res, err := m.OnMessagePublish(context.Background(), &MessagePublishCtx{Topic: "x"})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.False(t, res.Allow)
	assert.Equal(t, "nope", res.Reason)
}

func TestManager_OnMessagePublishAllow(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	m.Register(&decisionHook{name: "a", prio: 1, allow: true})
	m.Register(&decisionHook{name: "b", prio: 2, allow: true})

	res, err := m.OnMessagePublish(context.Background(), &MessagePublishCtx{Topic: "x"})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.Allow)
}

func TestManager_OnClientConnectDeny(t *testing.T) {
	m := NewManager()
	defer m.Stop()
	m.Register(&decisionHook{name: "deny", prio: 1, allow: false, reason: "blocked"})

	dec, err := m.OnClientConnect(context.Background(), &ClientConnectCtx{ClientID: "c"})
	require.NoError(t, err)
	require.NotNil(t, dec)
	assert.False(t, dec.Allow)
	assert.Equal(t, "blocked", dec.Reason)
}

func TestManager_NotificationAsync(t *testing.T) {
	m := NewManager()
	defer m.Stop()

	h := &recordingHook{
		name:         "rec",
		prio:         0,
		connected:    make(chan string, 4),
		disconnected: make(chan string, 4),
		subscribed:   make(chan string, 4),
		published:    make(chan string, 4),
	}
	m.Register(h)

	m.ClientConnected(context.Background(), &ClientCtx{ClientID: "c1"})
	m.ClientDisconnected(context.Background(), &ClientCtx{ClientID: "c1"})
	m.SessionSubscribed(context.Background(), &SubscriptionCtx{ClientID: "c1", Topic: "t/1"})
	m.SessionUnsubscribed(context.Background(), &SubscriptionCtx{ClientID: "c1", Topic: "t/1"})

	require.Eventually(t, func() bool {
		return len(h.connected) == 1 && len(h.disconnected) == 1 &&
			len(h.subscribed) == 1
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, "c1", <-h.connected)
	assert.Equal(t, "c1", <-h.disconnected)
	assert.Equal(t, "t/1", <-h.subscribed)
}

func TestAuditHook_ToggleAndLog(t *testing.T) {
	var buf bytes.Buffer
	h := NewAuditHook()
	h.SetLogger(log.New(&buf, "", 0))

	// Default: off -> no output.
	h.OnClientConnected(context.Background(), &ClientCtx{ClientID: "c", Username: "u"})
	assert.Empty(t, buf.String())

	// Enable the point -> output appears.
	h.Enable(OnClientConnectedPoint)
	h.OnClientConnected(context.Background(), &ClientCtx{ClientID: "c", Username: "u"})
	assert.Contains(t, buf.String(), "[AUDIT] CONNECT")
	assert.Contains(t, buf.String(), "client=c")

	// Disable again -> no further output.
	buf.Reset()
	h.Disable(OnClientConnectedPoint)
	h.OnClientConnected(context.Background(), &ClientCtx{ClientID: "c"})
	assert.Empty(t, buf.String())

	// Points() reflects enabled state.
	assert.Empty(t, h.Points())
	h.Enable(OnMessagePublishPoint)
	assert.Equal(t, []string{OnMessagePublishPoint}, h.Points())
}

func TestRateLimitHook_UnlimitedByDefault(t *testing.T) {
	h := NewRateLimitHook()
	// Default rate 0 => unlimited: many rapid publishes all allowed.
	for i := 0; i < 20; i++ {
		res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "x"})
		require.NoError(t, err)
		assert.True(t, res.Allow, "expected allow when rate limiting is off")
	}
}

func TestRateLimitHook_DenyWhenExceeded(t *testing.T) {
	h := NewRateLimitHook()
	h.SetDefaultRate(2, 2) // 2 tokens/sec, burst of 2.

	// Burst capacity is 2: first two allowed, third denied (no refill yet).
	for i := 0; i < 2; i++ {
		res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "x"})
		require.NoError(t, err)
		assert.True(t, res.Allow, "call %d within burst should be allowed", i)
	}
	res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "x"})
	require.NoError(t, err)
	assert.False(t, res.Allow)
	assert.Equal(t, rateLimitReason, res.Reason)

	// Per-client override: a different client is unaffected (still has its burst).
	res, err = h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "other", Topic: "x"})
	require.NoError(t, err)
	assert.True(t, res.Allow)
}

func TestForwardHook_RepublishesMatching(t *testing.T) {
	var mu sync.Mutex
	var forwarded []string
	h := NewForwardHook(func(topic string, qos byte, payload []byte, retain bool) error {
		mu.Lock()
		forwarded = append(forwarded, topic)
		mu.Unlock()
		return nil
	})

	// No rules => inert: nothing forwarded, publish allowed.
	res, err := h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "sensors/temp", Payload: []byte("v")})
	require.NoError(t, err)
	assert.True(t, res.Allow)
	mu.Lock()
	assert.Empty(t, forwarded)
	mu.Unlock()

	// Add a rule sensors/# -> fwd/all and publish a matching topic.
	h.AddRule(ForwardRule{TopicFilter: "sensors/#", TargetTopic: "fwd/all", QoS: 1})
	res, err = h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "sensors/temp", Payload: []byte("v")})
	require.NoError(t, err)
	assert.True(t, res.Allow)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(forwarded) == 1
	}, time.Second, 10*time.Millisecond)
	mu.Lock()
	assert.Equal(t, "fwd/all", forwarded[0])
	mu.Unlock()

	// A non-matching topic is not forwarded.
	forwarded = nil
	h.OnMessagePublish(context.Background(), &MessagePublishCtx{ClientID: "c", Topic: "other/x"})
	mu.Lock()
	assert.Empty(t, forwarded)
	mu.Unlock()
}
