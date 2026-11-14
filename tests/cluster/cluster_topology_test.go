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

// Package topotest contains integration tests for the Core/Replicant cluster
// topology: scalable wildcard-aware routing, single-leader election and
// Replicant graceful degradation.
package topotest

import (
	"context"
	"net"
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

// peerSpec identifies a peer node to connect to (id = node ID, addr = gRPC addr).
type peerSpec struct {
	id   string
	addr string
}

// topoNode is a running broker + cluster manager used by the topology tests.
type topoNode struct {
	b       *broker.Broker
	mgr     *cluster.Manager
	grpcSrv *grpc.Server
	cancel  context.CancelFunc
}

// startTopoNode boots a single cluster node: a broker, a gRPC cluster server, the
// configured role/core topology, leader election, and peer connections.
func startTopoNode(t *testing.T, parent context.Context, nodeID string, role cluster.Role, mqttPort, grpcPort string, coreIDs []string, coreAddrs map[string]string, peers []peerSpec) *topoNode {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)

	nodeAddr := "localhost" + grpcPort
	var b *broker.Broker
	mgr := cluster.NewManager(nodeID, nodeAddr, func(topic string, payload []byte, qos byte) {
		if b != nil {
			b.RouteToLocalSubscribersWithQoS(topic, payload, qos)
		}
	})
	mgr.SetRole(role)
	mgr.SetCoreNodes(coreIDs, coreAddrs)
	mgr.StartLeaderElection()

	b = broker.New(nodeID, mgr)
	b.SetupDefaultAuth()

	go func() { _ = b.StartServer(ctx, mqttPort) }()

	grpcSrv := grpc.NewServer()
	srv := cluster.NewServer(nodeID, mgr)
	clusterpb.RegisterClusterServiceServer(grpcSrv, srv)
	lis, err := net.Listen("tcp", grpcPort)
	require.NoError(t, err)
	go func() { _ = grpcSrv.Serve(lis) }()
	// Stop the gRPC server when the node's context is cancelled so discarded
	// nodes do not leak their listener port.
	go func() {
		<-ctx.Done()
		grpcSrv.Stop()
	}()

	if len(peers) > 0 {
		go func() {
			for attempt := 0; attempt < 40; attempt++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				for _, p := range peers {
					mgr.AddPeer(ctx, p.id, p.addr)
				}
				if mgr.PeerCount() >= len(peers) {
					return
				}
			}
		}()
	}

	return &topoNode{b: b, mgr: mgr, grpcSrv: grpcSrv, cancel: cancel}
}

// Kill stops the node (broker, gRPC server and election) to simulate a failure.
func (n *topoNode) Kill() {
	n.cancel()
	n.grpcSrv.Stop()
}

// newClient connects a paho MQTT client to the given broker address.
func newClient(t *testing.T, addr, clientID string, clean bool) mqtt.Client {
	t.Helper()
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + addr)
	opts.SetClientID(clientID)
	opts.SetUsername("test")
	opts.SetPassword("test")
	opts.SetCleanSession(clean)
	opts.SetAutoReconnect(false)
	opts.SetConnectTimeout(5 * time.Second)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(5*time.Second), "connect %s", clientID)
	require.NoError(t, tok.Error(), "connect error %s", clientID)
	return c
}

// waitForRoute blocks until node's routing table maps topic to nodeID, or times out.
func waitForRoute(t *testing.T, mgr *cluster.Manager, topic, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		found := false
		for _, id := range mgr.MatchRemoteSubscribers(topic) {
			if id == nodeID {
				found = true
				break
			}
		}
		if found || time.Now().After(deadline) {
			if !found {
				t.Fatalf("timed out waiting for route %s -> %s", topic, nodeID)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestReplicantToReplicant verifies that, with 1 Core + 2 Replicants, a message
// published on one Replicant is delivered to a subscriber on the other Replicant
// through the Core (the Replicants do not connect to each other).
func TestReplicantToReplicant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreAddrs := map[string]string{"core1": "localhost:1931"}
	core := startTopoNode(t, ctx, "core1", cluster.CoreNode, "localhost:1921", ":1931",
		[]string{"core1"}, coreAddrs, nil)
	_ = startTopoNode(t, ctx, "replA", cluster.ReplicantNode, "localhost:1922", ":1932",
		[]string{"core1"}, coreAddrs, []peerSpec{{id: "core1", addr: "localhost:1931"}})
	replB := startTopoNode(t, ctx, "replB", cluster.ReplicantNode, "localhost:1923", ":1933",
		[]string{"core1"}, coreAddrs, []peerSpec{{id: "core1", addr: "localhost:1931"}})

	// Wait until both Replicants are connected to the Core.
	deadline := time.Now().Add(20 * time.Second)
	for core.mgr.PeerCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for Core peers, have %d", core.mgr.PeerCount())
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Replicant A subscribes (handler registered AFTER connect).
	subA := newClient(t, "localhost:1922", "subA", true)
	defer subA.Disconnect(250)
	received := make(chan string, 1)
	tok := subA.Subscribe("t/a", 1, func(_ mqtt.Client, m mqtt.Message) {
		received <- string(m.Payload())
	})
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())

	// Wait for the route to propagate to Replicant B through the Core.
	waitForRoute(t, replB.mgr, "t/a", "replA")

	// Replicant B publishes; it must reach Replicant A via the Core.
	pubB := newClient(t, "localhost:1923", "pubB", true)
	defer pubB.Disconnect(250)
	pubTok := pubB.Publish("t/a", 1, false, "hello-from-b")
	require.True(t, pubTok.WaitTimeout(5*time.Second))
	require.NoError(t, pubTok.Error())

	select {
	case p := <-received:
		assert.Equal(t, "hello-from-b", p)
	case <-time.After(8 * time.Second):
		t.Fatal("Replicant A did not receive message published by Replicant B")
	}
}

// TestCrossNodeWildcard verifies that a wildcard subscription on one Replicant is
// matched cross-node by a concrete publish from another Replicant.
func TestCrossNodeWildcard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreAddrs := map[string]string{"core1": "localhost:1941"}
	core := startTopoNode(t, ctx, "core1", cluster.CoreNode, "localhost:1934", ":1941",
		[]string{"core1"}, coreAddrs, nil)
	_ = startTopoNode(t, ctx, "replA", cluster.ReplicantNode, "localhost:1935", ":1942",
		[]string{"core1"}, coreAddrs, []peerSpec{{id: "core1", addr: "localhost:1941"}})
	replB := startTopoNode(t, ctx, "replB", cluster.ReplicantNode, "localhost:1936", ":1943",
		[]string{"core1"}, coreAddrs, []peerSpec{{id: "core1", addr: "localhost:1941"}})

	deadline := time.Now().Add(20 * time.Second)
	for core.mgr.PeerCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for Core peers, have %d", core.mgr.PeerCount())
		}
		time.Sleep(200 * time.Millisecond)
	}

	subA := newClient(t, "localhost:1935", "subA", true)
	defer subA.Disconnect(250)
	received := make(chan string, 1)
	tok := subA.Subscribe("t/#", 1, func(_ mqtt.Client, m mqtt.Message) {
		received <- string(m.Payload())
	})
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())

	// The concrete topic t/x/y must match the wildcard subscription cross-node.
	waitForRoute(t, replB.mgr, "t/x/y", "replA")

	pubB := newClient(t, "localhost:1936", "pubB", true)
	defer pubB.Disconnect(250)
	pubTok := pubB.Publish("t/x/y", 1, false, "wildcard-payload")
	require.True(t, pubTok.WaitTimeout(5*time.Second))
	require.NoError(t, pubTok.Error())

	select {
	case p := <-received:
		assert.Equal(t, "wildcard-payload", p)
	case <-time.After(8 * time.Second):
		t.Fatal("Replicant A did not receive wildcard-matched message")
	}
}

// TestLeaderElection verifies that among 3 Cores exactly one leader is elected,
// and that when the leader is killed a new leader is elected among the survivors.
func TestLeaderElection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreIDs := []string{"c1", "c2", "c3"}
	coreAddrs := map[string]string{
		"c1": "localhost:1951",
		"c2": "localhost:1952",
		"c3": "localhost:1953",
	}
	c1 := startTopoNode(t, ctx, "c1", cluster.CoreNode, "localhost:1937", ":1951", coreIDs, coreAddrs,
		[]peerSpec{{id: "c2", addr: "localhost:1952"}, {id: "c3", addr: "localhost:1953"}})
	c2 := startTopoNode(t, ctx, "c2", cluster.CoreNode, "localhost:1938", ":1952", coreIDs, coreAddrs,
		[]peerSpec{{id: "c1", addr: "localhost:1951"}, {id: "c3", addr: "localhost:1953"}})
	c3 := startTopoNode(t, ctx, "c3", cluster.CoreNode, "localhost:1939", ":1953", coreIDs, coreAddrs,
		[]peerSpec{{id: "c1", addr: "localhost:1951"}, {id: "c2", addr: "localhost:1952"}})

	nodes := []*topoNode{c1, c2, c3}

	// Wait until exactly one leader is elected.
	leader := waitForUniqueLeader(t, nodes)

	// Kill the leader and verify a new leader emerges among the survivors.
	leader.Kill()
	deadline := time.Now().Add(20 * time.Second)
	for {
		count, _ := countLeaders(nodes)
		if count == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected exactly one leader after failover, got %d", count)
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, newLeaderID := countLeaders(nodes)
	assert.NotEqual(t, "", newLeaderID, "a new leader must be elected after failover")
}

// countLeaders returns the number of nodes currently reporting leadership and the
// ID of one of them ("" if none).
func countLeaders(nodes []*topoNode) (int, string) {
	count := 0
	id := ""
	for _, n := range nodes {
		if n.mgr.IsLeader() {
			count++
			id = n.mgr.NodeID
		}
	}
	return count, id
}

// waitForUniqueLeader polls until exactly one of the nodes is leader and returns it.
func waitForUniqueLeader(t *testing.T, nodes []*topoNode) *topoNode {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		count, id := countLeaders(nodes)
		if count == 1 {
			for _, n := range nodes {
				if n.mgr.NodeID == id {
					return n
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for a unique leader, current count=%d", count)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestReplicantDegradation verifies that a Replicant continues to serve local
// publish/subscribe after losing its Core connection (graceful degradation).
func TestReplicantDegradation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coreAddrs := map[string]string{"core1": "localhost:1961"}
	core := startTopoNode(t, ctx, "core1", cluster.CoreNode, "localhost:1944", ":1961",
		[]string{"core1"}, coreAddrs, nil)
	replA := startTopoNode(t, ctx, "replA", cluster.ReplicantNode, "localhost:1945", ":1962",
		[]string{"core1"}, coreAddrs, []peerSpec{{id: "core1", addr: "localhost:1961"}})

	// Wait until the Replicant is connected to the Core (degraded becomes false
	// once the election loop observes the Core peer).
	deadline := time.Now().Add(20 * time.Second)
	for replA.mgr.IsDegraded() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for Replicant->Core connection")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Kill the Core.
	core.Kill()

	// Give the Replicant time to detect the lost Core.
	degDeadline := time.Now().Add(10 * time.Second)
	for !replA.mgr.IsDegraded() {
		if time.Now().After(degDeadline) {
			t.Fatalf("Replicant did not become degraded after Core was killed")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Local publish/subscribe must still work without a Core.
	sub := newClient(t, "localhost:1945", "localSub", true)
	defer sub.Disconnect(250)
	received := make(chan string, 1)
	tok := sub.Subscribe("local/topic", 1, func(_ mqtt.Client, m mqtt.Message) {
		received <- string(m.Payload())
	})
	require.True(t, tok.WaitTimeout(5*time.Second))
	require.NoError(t, tok.Error())

	pub := newClient(t, "localhost:1945", "localPub", true)
	defer pub.Disconnect(250)
	pubTok := pub.Publish("local/topic", 1, false, "local-only")
	require.True(t, pubTok.WaitTimeout(5*time.Second))
	require.NoError(t, pubTok.Error())

	select {
	case p := <-received:
		assert.Equal(t, "local-only", p)
	case <-time.After(5 * time.Second):
		t.Fatal("local publish/subscribe did not work after Core was killed")
	}
}
