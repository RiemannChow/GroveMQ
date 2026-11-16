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

// Package cluster provides the functionality for creating a distributed cluster
// of GroveMQ nodes. It handles peer discovery, inter-node communication via
// gRPC, and routing of messages between nodes.
package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
	"github.com/turtacn/grovemq/pkg/storage"
	"github.com/turtacn/grovemq/pkg/storage/messages"
)

// Role identifies a node's position in the cluster topology. The model mirrors
// EMQX Mria: Core nodes hold authoritative metadata (routing table, membership,
// durable state) and elect a single leader that accepts topology writes;
// Replicant nodes do not interconnect with each other — they register their
// subscriptions with a Core and receive a replicated copy of the routing table
// so they can route locally.
type Role int

const (
	// CoreNode holds authoritative cluster metadata and participates in leader
	// election.
	CoreNode Role = iota
	// ReplicantNode is a leaf node that registers with Cores and routes locally.
	ReplicantNode
)

// Internal tunables for the temporary (stopgap) leader election. These are NOT
// a real Raft implementation; they provide single-writer leadership and basic
// partition degradation until hashicorp/raft replaces them in a later round.
const (
	electionInterval = 1 * time.Second
	pingTimeout      = 500 * time.Millisecond
)

// pendingRoute buffers a route operation that could not be applied because no
// leader was known at the time (e.g. a Replicant with no Core connection, or a
// Core that has not yet been elected).
type pendingRoute struct {
	filter string
	nodeID string
	remove bool
}

// Manager is responsible for managing the state of the cluster from the
// perspective of a single node. It maintains connections to peer nodes, manages
// the distributed routing table, and provides methods for broadcasting and
// forwarding messages to other nodes in the cluster.
type Manager struct {
	NodeID      string
	NodeAddress string
	peers       map[string]*Client
	// remoteRoutes maps a subscription filter to the set of node IDs that have
	// subscribers for it. Keying by filter (rather than exact topic) lets the
	// lookup perform MQTT wildcard matching, fixing cross-node wildcard routing.
	remoteRoutes     map[string]map[string]struct{}
	mu               sync.RWMutex
	LocalPublishFunc func(topic string, payload []byte, qos byte)

	// Load-balancing state (docs/design/LOAD_BALANCING.md). Wired at start-up;
	// only selectorEpoch is mutated on the PUBLISH hot path.
	LocalSharedPublishFunc func(topic string, payload []byte, qos byte, groups map[string]bool)
	LocalLoadFunc          func() NodeLoad
	loadTable              *NodeLoadTable
	selector               NodeSelector
	sharedRoutePolicy      string
	selectorEpoch          atomic.Uint64

	// StateStore and RetainedStore are optional durable stores. When set, the
	// cluster applies replicated state (sessions, retained messages) received
	// from peers into them, providing high availability of durable broker state.
	StateStore    storage.Store
	RetainedStore messages.StorageBackend

	// Role & topology (Core/Replicant model).
	role      Role
	coreNodes []string          // Core node IDs that participate in election.
	coreAddrs map[string]string // Core node ID -> gRPC address, for liveness probes.
	degraded  bool              // Replicant with no reachable Core.

	// Temporary leader-election state (stopgap for hashicorp/raft).
	electionMu sync.RWMutex
	epoch      uint64
	leaderID   string
	isLeader   bool
	electCtx   context.Context
	electCncl  context.CancelFunc

	// Buffered route operations awaiting a leader / Core connection.
	pendingRoutes []pendingRoute
}

// SetStateStores registers the local durable stores that should receive
// replicated state from peers.
func (m *Manager) SetStateStores(state storage.Store, retained messages.StorageBackend) {
	m.mu.Lock()
	m.StateStore = state
	m.RetainedStore = retained
	m.mu.Unlock()
}

// NewManager creates and initializes a new Manager instance. The default role is
// CoreNode so that the historical full-mesh behaviour (every node replicates its
// own routes to all peers) is preserved when no topology is configured.
//
// - nodeID: A unique identifier for the local node.
// - nodeAddress: The gRPC address of the local node.
// - localPublishFunc: A callback function to publish messages to local subscribers.
func NewManager(nodeID, nodeAddress string, localPublishFunc func(topic string, payload []byte, qos byte)) *Manager {
	return &Manager{
		NodeID:            nodeID,
		NodeAddress:       nodeAddress,
		peers:             make(map[string]*Client),
		remoteRoutes:      make(map[string]map[string]struct{}),
		LocalPublishFunc:  localPublishFunc,
		loadTable:         NewNodeLoadTable(),
		selector:          NewNodeSelector("round_robin"),
		sharedRoutePolicy: "fanout",
		role:              CoreNode,
		coreAddrs:         make(map[string]string),
	}
}

// PeerCount returns the number of currently connected peers. It is used by
// callers (e.g. tests) to wait until cluster membership is established.
func (m *Manager) PeerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.peers)
}

// SetRole configures this node's role in the cluster topology.
func (m *Manager) SetRole(r Role) {
	m.mu.Lock()
	m.role = r
	m.mu.Unlock()
}

// IsCore reports whether this node is a Core node.
func (m *Manager) IsCore() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.role == CoreNode
}

// IsReplicant reports whether this node is a Replicant node.
func (m *Manager) IsReplicant() bool { return !m.IsCore() }

// SetCoreNodes records the Core node IDs and their gRPC addresses. The addresses
// are used for liveness probes during leader election; the IDs are used to
// compute the majority and identify which peers are Cores.
func (m *Manager) SetCoreNodes(ids []string, addrs map[string]string) {
	m.mu.Lock()
	m.coreNodes = append([]string(nil), ids...)
	if addrs != nil {
		m.coreAddrs = make(map[string]string, len(addrs))
		for k, v := range addrs {
			m.coreAddrs[k] = v
		}
	}
	m.mu.Unlock()
}

// isCorePeer reports whether the given peer ID is a known Core node.
func (m *Manager) isCorePeer(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.coreNodes {
		if c == id {
			return true
		}
	}
	return false
}

// IsDegraded reports whether a Replicant currently has no reachable Core and is
// therefore serving only its local state.
func (m *Manager) IsDegraded() bool {
	m.electionMu.RLock()
	defer m.electionMu.RUnlock()
	return m.degraded
}

// AddPeer establishes a connection to a peer node and sends a join request.
// If the connection and join are successful, the peer is added to the manager's
// pool of connected peers.
//
// - ctx: A context for managing the connection and join request.
// - peerID: The unique identifier of the peer node to connect to.
// - address: The network address of the peer's gRPC server.
func (m *Manager) AddPeer(ctx context.Context, peerID, address string) {
	m.mu.Lock()
	if _, exists := m.peers[peerID]; exists {
		m.mu.Unlock()
		return // Already connected
	}
	m.mu.Unlock()

	log.Printf("Cluster Manager: Attempting to connect to peer %s at %s", peerID, address)
	client := NewClient(m.NodeID)
	if err := client.Connect(ctx, address); err != nil {
		log.Printf("Failed to connect to peer %s: %v", peerID, err)
		return
	}

	joinReq := &clusterpb.JoinRequest{
		Node: &clusterpb.NodeInfo{
			NodeId:  m.NodeID,
			Address: m.NodeAddress,
			Version: "0.1.0-poc",
		},
		Timestamp: time.Now().Unix(),
	}

	resp, err := client.Join(ctx, joinReq)
	if err != nil {
		log.Printf("Failed to join cluster via peer %s: %v", peerID, err)
		client.Close()
		return
	}

	if resp.Success {
		log.Printf("Successfully joined cluster via peer %s. Cluster ID: %s", peerID, resp.ClusterId)
		m.mu.Lock()
		m.peers[peerID] = client
		m.mu.Unlock()
		// Now that we have a (Core) peer, replay any routes that were buffered
		// while we were disconnected.
		m.flushBufferedRoutes()
	} else {
		log.Printf("Peer %s rejected join request: %s", peerID, resp.Message)
		client.Close()
	}
}

// ---------------------------------------------------------------------------
// Routing table
// ---------------------------------------------------------------------------

// AddRemoteRoute adds a new entry to the distributed routing table. It is kept
// for backward compatibility and is used by the gRPC server when a peer sends a
// "register" route update.
func (m *Manager) AddRemoteRoute(topic, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.remoteRoutes[topic] == nil {
		m.remoteRoutes[topic] = make(map[string]struct{})
	}
	m.remoteRoutes[topic][nodeID] = struct{}{}
}

// RemoveRemoteRoute removes a node from a filter's subscriber set.
func (m *Manager) RemoveRemoteRoute(topic, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if set, ok := m.remoteRoutes[topic]; ok {
		delete(set, nodeID)
		if len(set) == 0 {
			delete(m.remoteRoutes, topic)
		}
	}
}

// GetRemoteSubscribers returns the node IDs that have a subscription matching the
// given topic, using MQTT wildcard matching. Retained for backward compatibility.
func (m *Manager) GetRemoteSubscribers(topic string) []string {
	return m.MatchRemoteSubscribers(topic)
}

// MatchRemoteSubscribers returns the de-duplicated set of node IDs that have a
// subscription whose filter matches the published topic. This is the
// wildcard-aware replacement for the old exact-topic lookup and fixes
// cross-node wildcard routing (e.g. a publish to "a/b/c" now reaches a node
// that subscribed to "a/#").
func (m *Manager) MatchRemoteSubscribers(topic string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]string, 0)
	for filter, set := range m.remoteRoutes {
		if !routeTopicMatches(topic, filter) {
			continue
		}
		for nodeID := range set {
			if _, ok := seen[nodeID]; ok {
				continue
			}
			seen[nodeID] = struct{}{}
			out = append(out, nodeID)
		}
	}
	return out
}

// GetRemoteRoutes returns the global routing table (topic filter -> node IDs)
// used to forward publishes across the cluster. It is the read-side counterpart
// of RegisterRoute/UnregisterRoute and powers the observability "routes" view.
func (m *Manager) GetRemoteRoutes() map[string][]string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string][]string, len(m.remoteRoutes))
	for filter, set := range m.remoteRoutes {
		ids := make([]string, 0, len(set))
		for nodeID := range set {
			ids = append(ids, nodeID)
		}
		out[filter] = ids
	}
	return out
}

// CoreNodeIDs returns a copy of the configured Core node IDs participating in
// leader election. Used by the management plane to enumerate cluster members.
func (m *Manager) CoreNodeIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, len(m.coreNodes))
	copy(ids, m.coreNodes)
	return ids
}

// RouteRemote forwards a locally published message to every remote node that has
// a matching subscription. When a target node is not a direct peer (e.g. a
// Replicant publishing to another Replicant that only connects to Cores), the
// message is relayed through a Core.
func (m *Manager) RouteRemote(topic string, payload []byte, publishQoS byte) {
	m.mu.RLock()
	policy := m.sharedRoutePolicy
	m.mu.RUnlock()

	if policy == "single" {
		m.routeRemoteSingle(topic, payload, publishQoS)
		return
	}
	// Default "fanout" preserves the baseline behaviour byte-for-byte during
	// rolling upgrades (design §3 G5).
	for _, nodeID := range m.MatchRemoteSubscribers(topic) {
		m.forwardToNode(topic, payload, nodeID, publishQoS)
	}
}

// routeTopicMatches implements MQTT topic-filter matching supporting the '#'
// (multi-level) and '+' (single-level) wildcards, mirroring the matching used
// elsewhere in the broker.
func routeTopicMatches(topic, filter string) bool {
	if topic == filter {
		return true
	}
	if filter == "#" {
		return true
	}

	tLevels := strings.Split(topic, "/")
	fLevels := strings.Split(filter, "/")

	if len(fLevels) > 0 && fLevels[len(fLevels)-1] == "#" {
		for i := 0; i < len(fLevels)-1; i++ {
			if i >= len(tLevels) {
				return false
			}
			if fLevels[i] != "+" && fLevels[i] != tLevels[i] {
				return false
			}
		}
		return true
	}

	if len(tLevels) != len(fLevels) {
		return false
	}
	for i := 0; i < len(fLevels); i++ {
		if fLevels[i] == "+" {
			continue
		}
		if fLevels[i] != tLevels[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Route registration (the new, role-aware entry point)
// ---------------------------------------------------------------------------

// RegisterRoute records that nodeID subscribes to filter. Replicants forward the
// registration to a Core; Cores apply it locally only when they are the leader,
// otherwise they forward it to the current leader.
func (m *Manager) RegisterRoute(filter, nodeID string) {
	m.mu.RLock()
	role := m.role
	m.mu.RUnlock()

	if role == ReplicantNode {
		m.registerRouteWithCore(filter, nodeID, false)
		return
	}
	m.applyRouteRegistration(filter, nodeID, false)
}

// UnregisterRoute removes a node's subscription for filter, mirrored across the
// cluster the same way RegisterRoute is.
func (m *Manager) UnregisterRoute(filter, nodeID string) {
	m.mu.RLock()
	role := m.role
	m.mu.RUnlock()

	if role == ReplicantNode {
		m.registerRouteWithCore(filter, nodeID, true)
		return
	}
	m.applyRouteRegistration(filter, nodeID, true)
}

// registerRouteWithCore sends a route operation to a reachable Core (Replicants
// only connect to Cores, so any peer qualifies). If no Core is connected, the
// operation is buffered until a peer appears.
func (m *Manager) registerRouteWithCore(filter, nodeID string, remove bool) {
	m.mu.RLock()
	var corePeer *Client
	for id, c := range m.peers {
		if m.isCorePeer(id) {
			corePeer = c
			break
		}
	}
	if corePeer == nil {
		for _, c := range m.peers {
			corePeer = c
			break
		}
	}
	m.mu.RUnlock()

	if corePeer == nil {
		m.bufferRoute(filter, nodeID, remove)
		return
	}
	m.sendRouteOp(corePeer, filter, nodeID, remove)
}

// applyRouteRegistration is the Core-side handler. The leader updates the
// authoritative table and replicates it; followers forward to the leader; if no
// leader is known yet, the operation is buffered. An unconfigured topology
// (no Core list) behaves as a standalone leader so the historical full-mesh
// behaviour — every Core replicates its own routes to all peers — is preserved.
func (m *Manager) applyRouteRegistration(filter, nodeID string, remove bool) {
	if m.actsAsLeader() {
		if remove {
			m.RemoveRemoteRoute(filter, nodeID)
		} else {
			m.AddRemoteRoute(filter, nodeID)
		}
		m.replicateRoutes()
		return
	}

	m.electionMu.RLock()
	leader := m.leaderID
	m.electionMu.RUnlock()
	if leader != "" && leader != m.NodeID {
		m.forwardRouteToLeader(filter, nodeID, remove)
		return
	}
	m.bufferRoute(filter, nodeID, remove)
}

// forwardRouteToLeader sends a route operation to the current leader.
func (m *Manager) forwardRouteToLeader(filter, nodeID string, remove bool) {
	m.electionMu.RLock()
	leader := m.leaderID
	m.electionMu.RUnlock()
	if leader == "" || leader == m.NodeID {
		m.bufferRoute(filter, nodeID, remove)
		return
	}

	m.mu.RLock()
	peer, ok := m.peers[leader]
	m.mu.RUnlock()
	if !ok {
		m.bufferRoute(filter, nodeID, remove)
		return
	}
	m.sendRouteOp(peer, filter, nodeID, remove)
}

// sendRouteOp delivers a single route operation to a Core peer via
// BatchUpdateRoutes.
func (m *Manager) sendRouteOp(peer *Client, filter, nodeID string, remove bool) {
	op := "register"
	if remove {
		op = "unregister"
	}
	req := &clusterpb.BatchUpdateRoutesRequest{
		Routes:   []*clusterpb.Route{{Topic: filter, NodeIds: []string{nodeID}}},
		OpType:   op,
		FromNode: m.NodeID,
	}
	go func(c *Client) {
		if _, err := c.BatchUpdateRoutes(context.Background(), req); err != nil {
			log.Printf("Failed to send route op to core: %v", err)
		}
	}(peer)
}

// bufferRoute stores a route operation that could not be applied yet.
func (m *Manager) bufferRoute(filter, nodeID string, remove bool) {
	m.mu.Lock()
	m.pendingRoutes = append(m.pendingRoutes, pendingRoute{filter: filter, nodeID: nodeID, remove: remove})
	m.mu.Unlock()
}

// flushBufferedRoutes replays buffered route operations. It is called when this
// node becomes a leader or gains a Core peer.
func (m *Manager) flushBufferedRoutes() {
	m.mu.Lock()
	pending := m.pendingRoutes
	m.pendingRoutes = nil
	m.mu.Unlock()

	for _, pr := range pending {
		if m.IsCore() {
			m.applyRouteRegistration(pr.filter, pr.nodeID, pr.remove)
		} else {
			m.registerRouteWithCore(pr.filter, pr.nodeID, pr.remove)
		}
	}
}

// replicateRoutes ships the entire routing table to every peer as a single
// "routes" state entry. The leader calls this after every authoritative change
// so all nodes (Cores and Replicants) hold a local copy and can route locally.
func (m *Manager) replicateRoutes() {
	m.mu.RLock()
	table := make(map[string]map[string]struct{}, len(m.remoteRoutes))
	for f, set := range m.remoteRoutes {
		ns := make(map[string]struct{}, len(set))
		for n := range set {
			ns[n] = struct{}{}
		}
		table[f] = ns
	}
	m.mu.RUnlock()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(table); err != nil {
		log.Printf("Failed to encode routes table: %v", err)
		return
	}
	m.ReplicateState("routes", "routes:global", buf.Bytes())
}

// ---------------------------------------------------------------------------
// Forwarding
// ---------------------------------------------------------------------------

// sendForward sends a PublishForward to a specific peer client. When toNode is
// empty the message is a direct delivery to that peer; when set, it is a relay
// destined for toNode and the receiving node forwards it onward.
func (m *Manager) sendForward(peer *Client, topic string, payload []byte, toNode string, qos byte) {
	m.sendForwardEx(peer, topic, payload, toNode, qos, nil, 0)
}

// sendForwardEx is sendForward with group-scoped forwarding metadata. It is
// used by the single-target shared-subscription path and must survive relay
// hops (ToNode set) unchanged.
func (m *Manager) sendForwardEx(peer *Client, topic string, payload []byte, toNode string, qos byte, sharedGroups []string, epoch uint64) {
	req := &clusterpb.PublishForward{
		Topic:         topic,
		Payload:       payload,
		FromNode:      m.NodeID,
		ToNode:        toNode,
		Qos:           int32(qos),
		SharedGroups:  sharedGroups,
		SelectorEpoch: epoch,
	}
	go func(c *Client) {
		if _, err := c.ForwardPublish(context.Background(), req); err != nil {
			log.Printf("Failed to forward publish: %v", err)
		}
	}(peer)
}

// ForwardPublish sends a PUBLISH message directly to a specific peer node. It is
// used for direct (full-mesh) delivery where both nodes share a peer link.
func (m *Manager) ForwardPublish(topic string, payload []byte, nodeID string, qos byte) {
	m.mu.RLock()
	peer, ok := m.peers[nodeID]
	m.mu.RUnlock()

	if !ok {
		log.Printf("Cannot forward publish: no peer client for node %s", nodeID)
		return
	}
	m.sendForward(peer, topic, payload, "", qos)
}

// forwardToNode delivers a publish to nodeID. If nodeID is a direct peer it is a
// direct delivery; otherwise the message is relayed through a Core (or any peer)
// towards nodeID. exclude names a node that must not be used as a relay (to
// prevent loops). qos is the originating publish's QoS, preserved end-to-end.
func (m *Manager) forwardToNode(topic string, payload []byte, nodeID string, qos byte) {
	m.forwardToNodeFrom(topic, payload, nodeID, "", qos)
}

func (m *Manager) forwardToNodeFrom(topic string, payload []byte, nodeID, exclude string, qos byte) {
	m.forwardToNodeFromEx(topic, payload, nodeID, exclude, qos, nil, 0)
}

// ---------------------------------------------------------------------------
// Durable-state replication (sessions / retained / routes)
// ---------------------------------------------------------------------------

// ReplicateState ships a single durable-state entry to every connected peer so
// that the entry survives the failure of this node. Replication is best-effort
// and asynchronous; a failure to reach a peer is logged but does not block the
// caller. The kind value ("session", "retained" or "routes") tells peers which
// store to apply the entry to.
func (m *Manager) ReplicateState(kind, key string, value []byte) {
	m.replicateEntries([]*clusterpb.StateEntry{{Kind: kind, Key: key, Value: value, Deleted: false}})
}

// ReplicateDelete ships a deletion of a durable-state entry to every peer.
func (m *Manager) ReplicateDelete(kind, key string) {
	m.replicateEntries([]*clusterpb.StateEntry{{Kind: kind, Key: key, Deleted: true}})
}

func (m *Manager) replicateEntries(entries []*clusterpb.StateEntry) {
	m.mu.RLock()
	peers := make(map[string]*Client, len(m.peers))
	for id, c := range m.peers {
		peers[id] = c
	}
	m.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	req := &clusterpb.ReplicateStateRequest{FromNode: m.NodeID, Entries: entries}
	for id, peer := range peers {
		go func(peerID string, c *Client) {
			if _, err := c.ReplicateState(context.Background(), req); err != nil {
				log.Printf("Failed to replicate state to peer %s: %v", peerID, err)
			}
		}(id, peer)
	}
}

// ApplyStateEntries applies replicated state entries into the local durable
// stores. This is invoked by the gRPC server when a peer sends ReplicateState,
// and by RequestStateSync when backfilling from a peer.
func (m *Manager) ApplyStateEntries(ctx context.Context, entries []*clusterpb.StateEntry) int {
	m.mu.RLock()
	stateStore := m.StateStore
	retainedStore := m.RetainedStore
	m.mu.RUnlock()

	applied := 0
	for _, e := range entries {
		switch e.Kind {
		case "session":
			if stateStore == nil {
				continue
			}
			if e.Deleted {
				if err := stateStore.Delete(e.Key); err != nil {
					log.Printf("Failed to apply session delete for %s: %v", e.Key, err)
					continue
				}
			} else {
				var value any
				if err := gob.NewDecoder(bytes.NewReader(e.Value)).Decode(&value); err != nil {
					// Fall back to storing the raw bytes so the entry is not lost.
					value = e.Value
				}
				if err := stateStore.Set(e.Key, value); err != nil {
					log.Printf("Failed to apply session state for %s: %v", e.Key, err)
					continue
				}
			}
			applied++
		case "retained":
			if retainedStore == nil {
				continue
			}
			if e.Deleted {
				if err := retainedStore.DeleteRetained(ctx, e.Key); err != nil {
					log.Printf("Failed to apply retained delete for %s: %v", e.Key, err)
					continue
				}
			} else {
				var msg messages.Message
				if err := gob.NewDecoder(bytes.NewReader(e.Value)).Decode(&msg); err != nil {
					log.Printf("Failed to decode retained message for %s: %v", e.Key, err)
					continue
				}
				if err := retainedStore.StoreRetained(ctx, e.Key, msg); err != nil {
					log.Printf("Failed to apply retained state for %s: %v", e.Key, err)
					continue
				}
			}
			applied++
		case "routes":
			var table map[string]map[string]struct{}
			if err := gob.NewDecoder(bytes.NewReader(e.Value)).Decode(&table); err != nil {
				log.Printf("Failed to decode routes table for %s: %v", e.Key, err)
				continue
			}
			// Merge (union) the received routes into the local table rather than
			// replacing it, so a node never loses routes it learned from other
			// sources. The leader's authoritative table is a superset, so
			// followers converge to it.
			m.mu.Lock()
			for f, set := range table {
				if m.remoteRoutes[f] == nil {
					m.remoteRoutes[f] = make(map[string]struct{})
				}
				for n := range set {
					m.remoteRoutes[f][n] = struct{}{}
				}
			}
			m.mu.Unlock()
			applied++
		default:
			log.Printf("Ignoring replicated state of unknown kind %q", e.Kind)
		}
	}
	return applied
}

// BuildSnapshot gathers the full durable state from the local stores into state
// entries. It returns an empty slice when no durable stores are configured. The
// routing table is included so a joining Replicant can backfill it.
func (m *Manager) BuildSnapshot(ctx context.Context) []*clusterpb.StateEntry {
	m.mu.RLock()
	stateStore := m.StateStore
	retainedStore := m.RetainedStore
	table := m.remoteRoutes
	m.mu.RUnlock()

	entries := make([]*clusterpb.StateEntry, 0)
	if stateStore != nil {
		type forEacher interface {
			ForEach(func(key string, value []byte) error) error
		}
		if fe, ok := stateStore.(forEacher); ok {
			_ = fe.ForEach(func(key string, value []byte) error {
				entries = append(entries, &clusterpb.StateEntry{Kind: "session", Key: key, Value: value})
				return nil
			})
		}
	}
	if retainedStore != nil {
		type allRetaineder interface {
			AllRetained(context.Context) ([]messages.Message, error)
		}
		if ar, ok := retainedStore.(allRetaineder); ok {
			msgs, err := ar.AllRetained(ctx)
			if err == nil {
				for _, msg := range msgs {
					var buf bytes.Buffer
					if err := gob.NewEncoder(&buf).Encode(msg); err != nil {
						continue
					}
					entries = append(entries, &clusterpb.StateEntry{Kind: "retained", Key: msg.Topic, Value: buf.Bytes()})
				}
			}
		}
	}

	if len(table) > 0 {
		clone := make(map[string]map[string]struct{}, len(table))
		for f, set := range table {
			ns := make(map[string]struct{}, len(set))
			for n := range set {
				ns[n] = struct{}{}
			}
			clone[f] = ns
		}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(clone); err == nil {
			entries = append(entries, &clusterpb.StateEntry{Kind: "routes", Key: "routes:global", Value: buf.Bytes()})
		}
	}
	return entries
}

// RequestStateSync asks one connected peer for a full durable-state snapshot and
// applies it locally. It is used after joining a cluster so a cold-started node
// backfills sessions, retained messages and the routing table. It is a no-op
// when there are no peers or no local durable stores.
func (m *Manager) RequestStateSync(ctx context.Context) {
	m.mu.RLock()
	peers := make(map[string]*Client, len(m.peers))
	for id, c := range m.peers {
		peers[id] = c
	}
	stateStore := m.StateStore
	retainedStore := m.RetainedStore
	m.mu.RUnlock()

	if len(peers) == 0 || (stateStore == nil && retainedStore == nil) {
		return
	}

	for id, peer := range peers {
		resp, err := peer.SyncState(ctx, &clusterpb.SyncStateRequest{FromNode: m.NodeID})
		if err != nil {
			log.Printf("Failed to sync state from peer %s: %v", id, err)
			continue
		}
		applied := m.ApplyStateEntries(ctx, resp.Entries)
		log.Printf("State sync from peer %s applied %d entries", id, applied)
		return
	}
}

// ---------------------------------------------------------------------------
// Temporary leader election (stopgap for hashicorp/raft)
// ---------------------------------------------------------------------------

// StartLeaderElection begins the epoch-based single-leader election loop. It is
// a no-op (returns a no-op cancel) if already running. Replicants participate
// only to track degradation; only Cores campaign for leadership. The returned
// function cancels the loop.
func (m *Manager) StartLeaderElection() context.CancelFunc {
	m.electionMu.Lock()
	if m.electCtx != nil {
		m.electionMu.Unlock()
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.electCtx = ctx
	m.electCncl = cancel
	m.electionMu.Unlock()

	go m.leaderElectionLoop(ctx)
	return cancel
}

func (m *Manager) leaderElectionLoop(ctx context.Context) {
	m.runElection()
	ticker := time.NewTicker(electionInterval)
	defer ticker.Stop()
	round := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runElection()
			round++
			// Piggyback load probing on the existing 1s loop: every 5s,
			// refresh every peer's NodeLoad snapshot over the existing
			// SyncNodeStatus RPC (no new RPC, design §5.3).
			if round%5 == 0 {
				go m.probeLoads(ctx)
			}
		}
	}
}

// runElection performs one election round. Core nodes probe the liveness of
// every configured Core; the lexicographically smallest reachable Core that can
// form a majority becomes leader. When a majority cannot be formed the node
// steps down, refusing topology writes to avoid split-brain divergence.
func (m *Manager) runElection() {
	m.mu.RLock()
	role := m.role
	cores := m.coreNodes
	addrs := m.coreAddrs
	selfID := m.NodeID
	m.mu.RUnlock()

	if role != CoreNode {
		m.updateDegraded()
		return
	}

	if len(cores) == 0 {
		// No topology configured: behave as a standalone leader (the historical
		// full-mesh default where every Core replicates its own routes).
		m.setLeader(selfID)
		return
	}

	reachable := make([]string, 0, len(cores))
	for _, id := range cores {
		if id == selfID {
			reachable = append(reachable, id)
			continue
		}
		addr, ok := addrs[id]
		if !ok {
			continue
		}
		if m.ping(addr) {
			reachable = append(reachable, id)
		}
	}

	majority := len(cores)/2 + 1
	if len(reachable) < majority {
		m.stepDown()
		return
	}

	candidate := reachable[0]
	for _, id := range reachable[1:] {
		if id < candidate {
			candidate = id
		}
	}
	if candidate == selfID {
		m.setLeader(selfID)
	} else {
		m.setFollower(candidate)
	}
}

func (m *Manager) setLeader(id string) {
	m.electionMu.Lock()
	wasLeader := m.isLeader
	m.isLeader = true
	m.leaderID = id
	m.epoch++
	m.electionMu.Unlock()
	if !wasLeader {
		log.Printf("Became leader (epoch %d)", m.epoch)
		m.flushBufferedRoutes()
	}
}

func (m *Manager) setFollower(leaderID string) {
	m.electionMu.Lock()
	m.isLeader = false
	m.leaderID = leaderID
	m.electionMu.Unlock()
}

func (m *Manager) stepDown() {
	m.electionMu.Lock()
	if m.isLeader {
		log.Printf("Stepping down as leader (lost majority)")
	}
	m.isLeader = false
	m.leaderID = ""
	m.electionMu.Unlock()
}

// IsLeader reports whether this node currently holds leadership.
func (m *Manager) IsLeader() bool {
	m.electionMu.RLock()
	defer m.electionMu.RUnlock()
	return m.isLeader
}

// LeaderID returns the current leader's node ID ("" if none is known).
func (m *Manager) LeaderID() string {
	m.electionMu.RLock()
	defer m.electionMu.RUnlock()
	return m.leaderID
}

// actsAsLeader reports whether this node should apply topology writes locally
// and replicate them. A node acts as leader when it holds leadership, or when no
// Core topology is configured (the historical full-mesh default where every Core
// is authoritative for its own routes).
func (m *Manager) actsAsLeader() bool {
	if m.IsLeader() {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.coreNodes) == 0
}

// Epoch returns the current election epoch (monotonic while leader).
func (m *Manager) Epoch() uint64 {
	m.electionMu.RLock()
	defer m.electionMu.RUnlock()
	return m.epoch
}

// ping probes a Core node's gRPC endpoint for liveness.
func (m *Manager) ping(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	c := NewClient(m.NodeID)
	if err := c.Connect(ctx, addr); err != nil {
		return false
	}
	defer c.Close()
	resp, err := c.SyncNodeStatus(ctx, &clusterpb.SyncNodeStatusRequest{
		Node: &clusterpb.NodeInfo{NodeId: m.NodeID, Address: m.NodeAddress},
	})
	if err == nil && resp.GetLoad() != nil {
		m.recordRemoteLoad(nodeLoadFromProto(resp.GetLoad()))
	}
	return err == nil
}

// probeLoads refreshes the local NodeLoadTable with a snapshot from every
// connected peer plus this node's own counters. It reuses SyncNodeStatus so no
// new RPC is added; failures are ignored (a stale entry simply disables
// single-target selection for that node, falling back to fan-out).
func (m *Manager) probeLoads(ctx context.Context) {
	m.mu.RLock()
	peers := make(map[string]*Client, len(m.peers))
	for id, c := range m.peers {
		peers[id] = c
	}
	m.mu.RUnlock()

	for id, c := range peers {
		cctx, cancel := context.WithTimeout(ctx, pingTimeout)
		resp, err := c.SyncNodeStatus(cctx, &clusterpb.SyncNodeStatusRequest{
			Node: &clusterpb.NodeInfo{NodeId: m.NodeID, Address: m.NodeAddress},
		})
		cancel()
		if err == nil && resp.GetLoad() != nil {
			l := nodeLoadFromProto(resp.GetLoad())
			if l.NodeId == "" {
				l.NodeId = id
			}
			m.recordRemoteLoad(l)
		}
	}
	m.recordRemoteLoad(m.LocalLoad())
}

// recordRemoteLoad stores a load snapshot if a load table is wired.
func (m *Manager) recordRemoteLoad(l NodeLoad) {
	m.mu.RLock()
	table := m.loadTable
	m.mu.RUnlock()
	if table != nil {
		table.Update(l)
	}
}

// BroadcastRouteUpdate is deprecated. New code should call RegisterRoute, which
// performs role-aware, wildcard-aware registration. This method remains only to
// preserve legacy callers/tests: it blindcasts the given routes to every peer
// with op_type "add".
func (m *Manager) BroadcastRouteUpdate(routes []*clusterpb.Route) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	req := &clusterpb.BatchUpdateRoutesRequest{
		Routes:   routes,
		OpType:   "add",
		FromNode: m.NodeID,
	}

	for id, peer := range m.peers {
		go func(peerID string, c *Client) {
			if _, err := c.BatchUpdateRoutes(context.Background(), req); err != nil {
				log.Printf("Failed to send route update to peer %s: %v", peerID, err)
			}
		}(id, peer)
	}
}

// updateDegraded records whether a Replicant currently has a reachable Core. A
// Core peer is only considered healthy when it responds to a liveness probe, so a
// dead Core (whose stale peer entry lingers) is correctly reported as lost.
func (m *Manager) updateDegraded() {
	m.mu.RLock()
	hasLiveCore := false
	for id := range m.peers {
		if m.isCorePeer(id) {
			if addr, ok := m.coreAddrs[id]; ok && m.ping(addr) {
				hasLiveCore = true
				break
			}
		}
	}
	m.mu.RUnlock()

	m.electionMu.Lock()
	m.degraded = !hasLiveCore
	m.electionMu.Unlock()
}
