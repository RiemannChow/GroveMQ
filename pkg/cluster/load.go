package cluster

import (
	"sync"
	"time"

	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
)

// NodeLoad is a lock-free, in-memory load snapshot. It intentionally mirrors
// the fields of clusterpb.NodeLoad (the wire message) but is a plain struct:
// the generated protobuf type embeds protoimpl.MessageState (a sync.Mutex),
// and copying it by value on the PUBLISH hot path would copy a live lock and
// trip `go vet`. Conversion to/from the wire type is explicit at the RPC edge.
type NodeLoad struct {
	NodeId         string
	Connections    int64
	Sessions       int64
	Subscriptions  int64
	MaxConnections int64
	UnixNano       int64
}

// nodeLoadFromProto converts a wire message into the lock-free value type.
func nodeLoadFromProto(p *clusterpb.NodeLoad) NodeLoad {
	if p == nil {
		return NodeLoad{}
	}
	return NodeLoad{
		NodeId:         p.GetNodeId(),
		Connections:    p.GetConnections(),
		Sessions:       p.GetSessions(),
		Subscriptions:  p.GetSubscriptions(),
		MaxConnections: p.GetMaxConnections(),
		UnixNano:       p.GetUnixNano(),
	}
}

// toNodeLoadProto converts the local value into a wire message pointer.
func toNodeLoadProto(l NodeLoad) *clusterpb.NodeLoad {
	return &clusterpb.NodeLoad{
		NodeId:         l.NodeId,
		Connections:    l.Connections,
		Sessions:       l.Sessions,
		Subscriptions:  l.Subscriptions,
		MaxConnections: l.MaxConnections,
		UnixNano:       l.UnixNano,
	}
}

// NodeLoadTable stores the most recent load snapshot for every node.
//
// Writes arrive on the control plane: SyncNodeStatus liveness responses carry
// a NodeLoad (~5s), and the local node refreshes its own entry from the
// broker's connection counters. Readers on the PUBLISH hot path take an
// immutable Snapshot() and never hold the lock while selecting a node.
type NodeLoadTable struct {
	mu      sync.RWMutex
	entries map[string]NodeLoad
}

// NewNodeLoadTable creates an empty load table.
func NewNodeLoadTable() *NodeLoadTable {
	return &NodeLoadTable{entries: make(map[string]NodeLoad)}
}

// Update replaces the snapshot for l.NodeId. A zero UnixNano is filled with
// the local clock so callers that only have counters still get freshness.
func (t *NodeLoadTable) Update(l NodeLoad) {
	if l.NodeId == "" {
		return
	}
	if l.UnixNano == 0 {
		l.UnixNano = time.Now().UnixNano()
	}
	t.mu.Lock()
	t.entries[l.NodeId] = l
	t.mu.Unlock()
}

// Get returns one node's snapshot without copying the whole table.
func (t *NodeLoadTable) Get(nodeID string) (NodeLoad, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	l, ok := t.entries[nodeID]
	return l, ok
}

// Snapshot returns a read-only copy. Callers may iterate it without holding
// any lock; mutating the returned map does not affect the table.
func (t *NodeLoadTable) Snapshot() map[string]NodeLoad {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]NodeLoad, len(t.entries))
	for k, v := range t.entries {
		out[k] = v
	}
	return out
}

// IsStale reports whether the newest snapshot for nodeID is older than
// maxAge. A missing entry counts as stale: we never route to a node whose
// load we have never seen (it may already be offline).
func (t *NodeLoadTable) IsStale(nodeID string, maxAge time.Duration) bool {
	l, ok := t.Get(nodeID)
	if !ok || l.UnixNano <= 0 {
		return true
	}
	return time.Since(time.Unix(0, l.UnixNano)) > maxAge
}

// FreshNodes filters candidates down to those with a non-stale snapshot.
// The returned slice preserves the input order.
func (t *NodeLoadTable) FreshNodes(candidates []string, maxAge time.Duration) []string {
	if len(candidates) == 0 {
		return nil
	}
	out := make([]string, 0, len(candidates))
	for _, id := range candidates {
		if !t.IsStale(id, maxAge) {
			out = append(out, id)
		}
	}
	return out
}

// NodeLoadSnapshot is an immutable copy handed to NodeSelector.Select.
type NodeLoadSnapshot map[string]NodeLoad
