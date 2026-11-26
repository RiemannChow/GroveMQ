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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/broker"
)

// TestA2ASharedSubscriptionWorkPool proves that A2A worker pools scale out:
// two workers subscribe to the same shared request filter and N published
// task requests are distributed across them, each request delivered exactly
// once. This is the transport primitive behind broker-neutral A2A scheduling.
func TestA2ASharedSubscriptionWorkPool(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := broker.New("a2a-shared-pool", nil)
	go func() { _ = b.StartServer(ctx, addr) }()
	waitForTCP(t, addr)

	const agentAddress = "org/unit/agent"
	filter, err := task.SharedRequestTopic("pool", agentAddress)
	require.NoError(t, err)
	requestTopic, err := task.RequestTopic(agentAddress)
	require.NoError(t, err)

	const total = 6
	var (
		mu       sync.Mutex
		received = map[string]int{}
		ready    = make(chan struct{}, 2)
		errCh    = make(chan error, 2)
	)

	// Two workers share the pool filter. Each consumes whatever it is handed.
	for _, id := range []string{"worker-1", "worker-2"} {
		go func(clientID string) {
			conn, err := netDialWithTimeout(addr, time.Second)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
			reader := bufio.NewReader(conn)
			writeCONNECT(t, conn, clientID)
			if _, err := p2ReadPacketSafe(reader); err != nil { // CONNACK
				errCh <- err
				return
			}
			writeSUBSCRIBE(t, conn, filter)
			suback, err := p2ReadPacketSafe(reader)
			if err != nil || suback.FixedHeader.Type != packets.Suback {
				errCh <- fmt.Errorf("%s: subscribe failed: %v", clientID, err)
				return
			}
			ready <- struct{}{}
			for i := 0; i < total; i++ {
				pk, err := p2ReadPacketSafe(reader)
				if err != nil {
					return // deadline reached once all requests were distributed
				}
				if pk.FixedHeader.Type != packets.Publish {
					continue
				}
				if pk.FixedHeader.Qos == 1 {
					ack := &packets.Packet{
						FixedHeader:     packets.FixedHeader{Type: packets.Puback},
						ProtocolVersion: 5, PacketID: pk.PacketID, ReasonCode: task.ReasonSuccess,
					}
					_ = p2WritePacketSafe(conn, ack.PubackEncode)
				}
				mu.Lock()
				received[clientID]++
				mu.Unlock()
			}
		}(id)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case err := <-errCh:
			t.Fatalf("worker setup failed: %v", err)
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not subscribe to shared filter")
		}
	}

	// Publisher emits total QoS 1 requests and waits for each PUBACK.
	pub, err := netDialWithTimeout(addr, time.Second)
	require.NoError(t, err)
	defer pub.Close()
	_ = pub.SetDeadline(time.Now().Add(8 * time.Second))
	pubReader := bufio.NewReader(pub)
	writeCONNECT(t, pub, "a2a-publisher")
	_, err = p2ReadPacketSafe(pubReader)
	require.NoError(t, err)
	for i := 0; i < total; i++ {
		payload := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":"%d","method":"message/send"}`, i))
		err = p2WritePacketSafe(pub, (&packets.Packet{
			FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 1},
			ProtocolVersion: 5, PacketID: uint16(i + 1), TopicName: requestTopic, Payload: payload,
		}).PublishEncode)
		require.NoError(t, err)
		ack, err := p2ReadPacketSafe(pubReader)
		require.NoError(t, err)
		require.Equal(t, packets.Puback, ack.FixedHeader.Type)
	}

	// Wait until every published request has been delivered to exactly one
	// worker. The shared subscription must not duplicate a message to both.
	deadline := time.Now().Add(4 * time.Second)
	for {
		mu.Lock()
		sum := received["worker-1"] + received["worker-2"]
		mu.Unlock()
		if sum >= total || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	sum := received["worker-1"] + received["worker-2"]
	snapshot := map[string]int{"worker-1": received["worker-1"], "worker-2": received["worker-2"]}
	mu.Unlock()

	assert.Equal(t, total, sum, "each request must be delivered exactly once; got %v", snapshot)
	assert.Greater(t, snapshot["worker-1"], 0, "worker-1 should receive some requests")
	assert.Greater(t, snapshot["worker-2"], 0, "worker-2 should receive some requests")
}
