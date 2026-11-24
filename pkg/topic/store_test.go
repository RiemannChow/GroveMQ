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

package topic

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/actor"
)

func TestStore(t *testing.T) {
	s := NewStore()
	assert.NotNil(t, s)

	mb1 := actor.NewMailbox(10)
	mb2 := actor.NewMailbox(10)

	// Test Subscribe
	s.Subscribe("test/topic", "client1", mb1, 1)
	s.Subscribe("test/topic", "client2", mb2, 2)

	subs := s.GetSubscribers("test/topic")
	assert.Len(t, subs, 2)

	// Check that we have the correct mailboxes and QoS levels
	var foundMB1, foundMB2 bool
	for _, sub := range subs {
		if sub.Mailbox == mb1 {
			foundMB1 = true
			assert.Equal(t, byte(1), sub.QoS)
		} else if sub.Mailbox == mb2 {
			foundMB2 = true
			assert.Equal(t, byte(2), sub.QoS)
		}
	}
	assert.True(t, foundMB1)
	assert.True(t, foundMB2)

	// Test GetSubscribers for unknown topic
	subs = s.GetSubscribers("unknown/topic")
	assert.Len(t, subs, 0)

	// Test Unsubscribe
	s.Unsubscribe("test/topic", mb1)
	subs = s.GetSubscribers("test/topic")
	assert.Len(t, subs, 1)
	assert.Equal(t, mb2, subs[0].Mailbox)

	// Test Unsubscribe last subscriber
	s.Unsubscribe("test/topic", mb2)
	subs = s.GetSubscribers("test/topic")
	assert.Len(t, subs, 0)
	_, ok := s.subscriptions["test/topic"]
	assert.False(t, ok)

	// Test Unsubscribe from non-existent topic
	s.Unsubscribe("non/existent", mb1)
}

// TestSharedSubscriptionConcurrentSelection is a -race regression test for the
// round-robin cursor: GetSubscribers holds only an RLock, so concurrent readers
// used to perform a plain read/write of NextIndex and raced with each other.
// The cursor must be advanced atomically so every publish is distributed
// deterministically without a data race.
func TestSharedSubscriptionConcurrentSelection(t *testing.T) {
	const (
		subscribers = 4
		goroutines  = 8
		iterations  = 500
	)

	s := NewStore()
	mailboxes := make([]*actor.Mailbox, subscribers)
	for i := range mailboxes {
		mailboxes[i] = actor.NewMailbox(16)
		s.Subscribe("$share/group/topic", "client-"+string(rune('a'+i)), mailboxes[i], 1)
	}

	counts := make([]int64, subscribers)
	var unexpected int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				subs := s.GetSubscribers("topic")
				if len(subs) != 1 {
					atomic.AddInt64(&unexpected, 1)
					continue
				}
				for idx, mb := range mailboxes {
					if subs[0].Mailbox == mb {
						atomic.AddInt64(&counts[idx], 1)
						break
					}
				}
			}
		}()
	}
	wg.Wait()

	require.Zero(t, unexpected, "shared subscription must select exactly one subscriber")
	var total int64
	for i, c := range counts {
		assert.NotZero(t, c, "subscriber %d never received a shared message", i)
		total += c
	}
	assert.Equal(t, int64(goroutines*iterations), total, "every publish must select exactly one subscriber")
}

func TestMatchesTopicFilterReservedTopics(t *testing.T) {
	tests := []struct {
		topic  string
		filter string
		want   bool
	}{
		{"$SYS/broker/uptime", "#", false},
		{"$SYS/broker/uptime", "+/broker/+", false},
		{"$SYS/broker/uptime", "$SYS/#", true},
		{"$a2a/v1/discovery/org/unit/agent", "#", false},
		{"$a2a/v1/discovery/org/unit/agent", "$a2a/v1/#", true},
		{"sensors/room1/temp", "#", true},
		{"sensors/room1/temp", "sensors/+/temp", true},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, matchesTopicFilter(tt.topic, tt.filter),
			"matchesTopicFilter(%q, %q)", tt.topic, tt.filter)
	}
}
