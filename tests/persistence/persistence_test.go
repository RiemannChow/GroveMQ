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

// Package persistence contains integration tests for GroveMQ's durable
// persistence and cross-node high-availability replication.
package persistence

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/cluster"
	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
)

// newClient creates a paho MQTT client connected to the given broker address.
func newClient(t *testing.T, addr, clientID string, clean bool) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + addr)
	opts.SetClientID(clientID)
	opts.SetUsername("test")
	opts.SetPassword("test")
	opts.SetCleanSession(clean)
	opts.SetConnectTimeout(5 * time.Second)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(5*time.Second), "connect %s", clientID)
	require.NoError(t, tok.Error(), "connect error %s", clientID)
	return c
}

// TestPersistenceAcrossRestart verifies that retained messages and offline
// message queues survive a broker restart when GROVEMQ_DATA_DIR is set.
func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Setenv("GROVEMQ_DATA_DIR", dir))
	defer os.Unsetenv("GROVEMQ_DATA_DIR")

	const addr = "localhost:1899"
	const nodeID = "restart-node"

	// --- First broker instance ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	b1 := broker.New(nodeID, nil)
	b1.SetupDefaultAuth()
	go func() { _ = b1.StartServer(ctx1, addr) }()
	time.Sleep(200 * time.Millisecond)

	// Publish a retained message on the first instance.
	pub := newClient(t, addr, "pub-1", true)
	defer pub.Disconnect(250)
	tok := pub.Publish("persist/retained", 1, true, "hello-retained")
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())

	// A persistent (clean=false) subscriber that goes offline, then we publish a
	// non-retained message that must be queued for offline delivery.
	sub := newClient(t, addr, "offline-1", false)
	defer sub.Disconnect(250)
	_ = sub.Subscribe("persist/offline", 1, func(mqtt.Client, mqtt.Message) {})
	time.Sleep(200 * time.Millisecond)
	sub.Disconnect(250)
	time.Sleep(300 * time.Millisecond)

	pub2 := newClient(t, addr, "pub-2", true)
	defer pub2.Disconnect(250)
	tok = pub2.Publish("persist/offline", 1, false, "hello-offline")
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())
	time.Sleep(300 * time.Millisecond) // allow offline queue + disk flush

	// Shut the first instance down (releases the bbolt lock).
	cancel1()
	_ = b1.Close()
	time.Sleep(500 * time.Millisecond)

	// --- Second broker instance, same node ID and data dir ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	b2 := broker.New(nodeID, nil)
	b2.SetupDefaultAuth()
	go func() { _ = b2.StartServer(ctx2, addr) }()
	time.Sleep(200 * time.Millisecond)

	// Retained message must survive the restart.
	sub2 := newClient(t, addr, "sub-2", true)
	defer sub2.Disconnect(250)
	gotRetained := make(chan string, 1)
	_ = sub2.Subscribe("persist/retained", 1, func(_ mqtt.Client, m mqtt.Message) {
		gotRetained <- string(m.Payload())
	})
	select {
	case p := <-gotRetained:
		assert.Equal(t, "hello-retained", p, "retained message must survive restart")
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive retained message after restart")
	}

	// Offline message must survive the restart and be delivered on resume.
	off := newClient(t, addr, "offline-1", false)
	defer off.Disconnect(250)
	gotOffline := make(chan string, 1)
	_ = off.Subscribe("persist/offline", 1, func(_ mqtt.Client, m mqtt.Message) {
		gotOffline <- string(m.Payload())
	})
	select {
	case p := <-gotOffline:
		assert.Equal(t, "hello-offline", p, "offline message must survive restart")
	case <-time.After(6 * time.Second):
		t.Fatal("did not receive offline message after restart")
	}
}

// haNode bundles a broker that participates in a cluster for HA testing.
type haNode struct {
	broker     *broker.Broker
	clusterMgr *cluster.Manager
	grpcServer *grpc.Server
	cancel     context.CancelFunc
}

// startHANode starts a broker with clustering enabled on the given ports and
// connects it to peerNodes.
func startHANode(t *testing.T, parent context.Context, nodeID, mqttPort, grpcPort, peerNodes string) *haNode {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	addr := fmt.Sprintf("localhost%s", grpcPort)

	var b *broker.Broker
	mgr := cluster.NewManager(nodeID, addr, func(topic string, payload []byte, qos byte) {
		if b != nil {
			b.RouteToLocalSubscribersWithQoS(topic, payload, qos)
		}
	})
	b = broker.New(nodeID, mgr)
	b.SetupDefaultAuth()

	go func() { _ = b.StartServer(ctx, mqttPort) }()

	grpcServer := grpc.NewServer()
	clusterServer := cluster.NewServer(nodeID, mgr)
	clusterpb.RegisterClusterServiceServer(grpcServer, clusterServer)
	lis, err := net.Listen("tcp", grpcPort)
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(lis) }()

	if peerNodes != "" {
		go func() {
			// Retry until each peer is connected (peers may start later).
			for attempt := 0; attempt < 30; attempt++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				for _, p := range splitPeers(peerNodes) {
					mgr.AddPeer(ctx, peerID(p), p)
				}
				if mgr.PeerCount() >= len(splitPeers(peerNodes)) {
					return
				}
			}
		}()
	}

	return &haNode{broker: b, clusterMgr: mgr, grpcServer: grpcServer, cancel: cancel}
}

func splitPeers(s string) []string {
	out := make([]string, 0)
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func peerID(addr string) string {
	// The peer key only needs to be unique per peer; the address is unique.
	return addr
}

// waitForPeers blocks until the manager reports at least want connected peers or
// the test times out.
func waitForPeers(t *testing.T, mgr *cluster.Manager, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for mgr.PeerCount() < want {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d peers, have %d", want, mgr.PeerCount())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestHAReplication verifies that retained messages published on one node are
// replicated to a peer and served from the peer's store.
func TestHAReplication(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Setenv("GROVEMQ_DATA_DIR", dir))
	defer os.Unsetenv("GROVEMQ_DATA_DIR")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n1 := startHANode(t, ctx, "ha-node1", "localhost:1901", ":18091", "")
	startHANode(t, ctx, "ha-node2", "localhost:1902", ":18092", "localhost:18091")
	waitForPeers(t, n1.clusterMgr, 1)

	// Publish a retained message on node1.
	pub := newClient(t, "localhost:1901", "ha-pub", true)
	defer pub.Disconnect(250)
	tok := pub.Publish("ha/retained", 1, true, "ha-payload")
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())
	time.Sleep(2 * time.Second) // allow replication to node2

	// Subscribe on node2; the retained message must come from the replicated store.
	sub := newClient(t, "localhost:1902", "ha-sub", true)
	defer sub.Disconnect(250)
	got := make(chan string, 1)
	_ = sub.Subscribe("ha/retained", 1, func(_ mqtt.Client, m mqtt.Message) {
		got <- string(m.Payload())
	})
	select {
	case p := <-got:
		assert.Equal(t, "ha-payload", p, "retained message must be replicated to peer")
	case <-time.After(5 * time.Second):
		t.Fatal("node2 did not receive replicated retained message")
	}
}

// TestHASessionFailover verifies that an offline message queued on one node for a
// persistent session is replicated and delivered when the client reconnects to a
// different node.
func TestHASessionFailover(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Setenv("GROVEMQ_DATA_DIR", dir))
	defer os.Unsetenv("GROVEMQ_DATA_DIR")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n1 := startHANode(t, ctx, "ha-fail-node1", "localhost:1911", ":18095", "")
	startHANode(t, ctx, "ha-fail-node2", "localhost:1912", ":18096", "localhost:18095")
	waitForPeers(t, n1.clusterMgr, 1)

	// Client connects to node1 with a persistent session and subscribes.
	c1 := newClient(t, "localhost:1911", "fail-client", false)
	_ = c1.Subscribe("ha/failover", 1, func(mqtt.Client, mqtt.Message) {})
	time.Sleep(300 * time.Millisecond)
	c1.Disconnect(250)
	time.Sleep(400 * time.Millisecond)

	// Publish to node1 while the client is offline -> queued + replicated.
	pub := newClient(t, "localhost:1911", "fail-pub", true)
	defer pub.Disconnect(250)
	tok := pub.Publish("ha/failover", 1, false, "failover-payload")
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())
	time.Sleep(2 * time.Second) // allow replication to node2

	// Client reconnects to node2; the offline message must be delivered from the
	// replicated session state.
	c2 := newClient(t, "localhost:1912", "fail-client", false)
	defer c2.Disconnect(250)
	got := make(chan string, 1)
	_ = c2.Subscribe("ha/failover", 1, func(_ mqtt.Client, m mqtt.Message) {
		got <- string(m.Payload())
	})
	select {
	case p := <-got:
		assert.Equal(t, "failover-payload", p, "offline message must fail over to peer")
	case <-time.After(8 * time.Second):
		t.Fatal("did not receive fail-over offline message on node2")
	}
}
