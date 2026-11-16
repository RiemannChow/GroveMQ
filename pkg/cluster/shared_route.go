package cluster

import (
	"log"
	"sort"
	"time"

	"github.com/turtacn/grovemq/pkg/topic"
)

// loadStaleAfter is 2x the load-report interval (design §5.3). A node whose
// snapshot is older than this is excluded from single-target selection and
// the group falls back to fan-out rather than being routed to a possibly dead
// node.
const loadStaleAfter = 10 * time.Second

// Selection helpers live in selector.go; matchWithGroups below produces one.

// matchWithGroups splits the replicated route index into ordinary fan-out
// targets and, per shared-subscription group, the candidate nodes owning a
// member of that group.
//
// Crucially this does NOT change the routing table layout: shared routes are
// still stored under their original "$share/<group>/<topic>" key. We only
// strip the prefix here, in memory, before wildcard matching. That keeps the
// gob-encoded "routes" state entry byte-compatible with older nodes during a
// rolling upgrade (design §5.2.1 compatibility note) and avoids the recursive
// RLock / table-type churn the composite-key design would require.
func (m *Manager) matchWithGroups(topicName string) Selection {
	sel := Selection{Groups: make(map[string][]string)}

	m.mu.RLock()
	for filter, set := range m.remoteRoutes {
		group, realFilter, err := topic.ParseSharedFilter(filter)
		matched := false
		if err == nil && group != "" {
			matched = routeTopicMatches(topicName, realFilter)
		} else {
			// Ordinary filter, or a malformed $share key from an older node:
			// use the historical matching behaviour verbatim.
			group = ""
			matched = routeTopicMatches(topicName, filter)
		}
		if !matched {
			continue
		}
		nodes := make([]string, 0, len(set))
		for nodeID := range set {
			nodes = append(nodes, nodeID)
		}
		if group == "" {
			sel.Nodes = append(sel.Nodes, nodes...)
		} else {
			sel.Groups[group] = append(sel.Groups[group], nodes...)
		}
	}
	m.mu.RUnlock()

	// Normalise outside the lock: sorted + de-duplicated candidates make every
	// selector's tie-break deterministic.
	sel.Nodes = sortedCandidates(sel.Nodes)
	for g, ns := range sel.Groups {
		sel.Groups[g] = sortedCandidates(ns)
	}
	if len(sel.Groups) == 1 {
		for _, ns := range sel.Groups {
			if len(ns) > 0 {
				sel.SharedTarget = ns[0]
			}
		}
	}
	return sel
}

// routeRemoteSingle is GROVEMQ_SHARED_ROUTE_POLICY=single: ordinary
// subscriptions keep fan-out; each shared group is delivered to exactly one
// node. Node selection is a pure in-memory operation performed only at the
// publishing node, after the route-table lock has been released.
func (m *Manager) routeRemoteSingle(topicName string, payload []byte, publishQoS byte) {
	sel := m.matchWithGroups(topicName)

	m.mu.RLock()
	selector := m.selector
	loadTable := m.loadTable
	m.mu.RUnlock()
	if selector == nil {
		selector = NewNodeSelector("round_robin")
	}
	if loadTable == nil {
		loadTable = NewNodeLoadTable()
	}
	load := loadTable.Snapshot()
	epoch := m.selectorEpoch.Add(1)

	// nodeID -> set of shared groups this node was selected for. An empty set
	// means the node is an ordinary fan-out target only. Aggregating by node is
	// what prevents duplicate ordinary deliveries when one node is chosen by
	// several groups.
	plans := make(map[string]map[string]bool)
	ensure := func(nodeID string) map[string]bool {
		g, ok := plans[nodeID]
		if !ok {
			g = make(map[string]bool)
			plans[nodeID] = g
		}
		return g
	}
	for _, nodeID := range sel.Nodes {
		ensure(nodeID)
	}

	for group, candidates := range sel.Groups {
		eligible := loadTable.FreshNodes(candidates, loadStaleAfter)
		if len(eligible) == 0 || len(eligible) != len(candidates) {
			// Missing/stale load data: loss-avoiding fallback (design §5.5).
			log.Printf("Shared group %q: stale load data, falling back to fan-out", group)
			for _, nodeID := range candidates {
				ensure(nodeID)[group] = true
			}
			continue
		}
		target, err := selector.Select(group, topicName, eligible, load)
		if err != nil || target == "" {
			log.Printf("Shared group %q: selection failed (%v), falling back to fan-out", group, err)
			for _, nodeID := range candidates {
				ensure(nodeID)[group] = true
			}
			continue
		}
		ensure(target)[group] = true
	}

	nodeIDs := make([]string, 0, len(plans))
	for nodeID := range plans {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		groups := plans[nodeID]
		if len(groups) == 0 {
			// This node has no shared-group member for this topic, so the
			// historical LocalPublishFunc cannot deliver a stray group member.
			m.forwardToNode(topicName, payload, nodeID, publishQoS)
			continue
		}
		groupList := make([]string, 0, len(groups))
		for g := range groups {
			groupList = append(groupList, g)
		}
		sort.Strings(groupList)
		m.forwardToNodeShared(topicName, payload, nodeID, groupList, epoch, publishQoS)
	}
}

// forwardToNodeShared sends a group-scoped forward. The receiving node
// delivers to exactly these groups (plus ordinary subscribers) and must not
// re-broadcast to other nodes.
func (m *Manager) forwardToNodeShared(topicName string, payload []byte, nodeID string, groups []string, epoch uint64, qos byte) {
	if nodeID == m.NodeID {
		m.deliverSharedLocally(topicName, payload, groups, qos)
		return
	}
	m.forwardToNodeFromEx(topicName, payload, nodeID, "", qos, groups, epoch)
}

// deliverSharedLocally is used when the publishing node itself is selected.
func (m *Manager) deliverSharedLocally(topicName string, payload []byte, groups []string, qos byte) {
	set := make(map[string]bool, len(groups))
	for _, g := range groups {
		set[g] = true
	}
	if m.LocalSharedPublishFunc != nil {
		m.LocalSharedPublishFunc(topicName, payload, qos, set)
		return
	}
	// No group-scoped callback wired (e.g. a minimal test manager): degrade to
	// the historical local delivery rather than dropping the message.
	if m.LocalPublishFunc != nil {
		m.LocalPublishFunc(topicName, payload, qos)
	}
}

// forwardToNodeFromEx is forwardToNodeFrom with group-scoped forwarding
// metadata that must survive a relay hop.
func (m *Manager) forwardToNodeFromEx(topicName string, payload []byte, nodeID, exclude string, qos byte, groups []string, epoch uint64) {
	if nodeID == m.NodeID {
		return
	}

	m.mu.RLock()
	peer, ok := m.peers[nodeID]
	var relay *Client
	if !ok {
		for id, c := range m.peers {
			if id == exclude {
				continue
			}
			// isCorePeer takes m.mu.RLock itself; do NOT call it while holding
			// the same RLock (Go RWMutex forbids recursive read locking).
			if m.isCorePeerUnsafe(id) {
				relay = c
				break
			}
		}
		if relay == nil {
			for id, c := range m.peers {
				if id == exclude {
					continue
				}
				relay = c
				break
			}
		}
	}
	m.mu.RUnlock()

	if ok {
		m.sendForwardEx(peer, topicName, payload, "", qos, groups, epoch)
	} else if relay != nil {
		m.sendForwardEx(relay, topicName, payload, nodeID, qos, groups, epoch)
	} else {
		log.Printf("Cannot forward publish for %s: no route to node %s", topicName, nodeID)
	}
}

// isCorePeerUnsafe is isCorePeer's lock-free read for callers already holding
// m.mu.RLock. It exists to avoid the recursive read lock in the original
// forwardToNodeFrom (see the code-survey report §9.4).
func (m *Manager) isCorePeerUnsafe(nodeID string) bool {
	for _, id := range m.coreNodes {
		if id == nodeID {
			return true
		}
	}
	return false
}

// SetBalanceConfig wires the forwarding-plane policy and selector strategy.
// policy is "fanout" (baseline, default during rolling upgrades) or "single"
// (cross-node shared-subscription semantics). strategy names a NodeSelector.
func (m *Manager) SetBalanceConfig(policy, strategy string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sharedRoutePolicy = policy
	if strategy != "" {
		m.selector = NewNodeSelector(strategy)
	}
}

// LoadTable exposes the node load table so the access layer (admission
// controller) and diagnostics can read the same snapshots as the selector.
func (m *Manager) LoadTable() *NodeLoadTable {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.loadTable
}

// LocalLoad returns this node's load snapshot for the SyncNodeStatus probe.
func (m *Manager) LocalLoad() NodeLoad {
	f := m.LocalLoadFunc
	if f == nil {
		return NodeLoad{NodeId: m.NodeID, UnixNano: time.Now().UnixNano()}
	}
	l := f()
	if l.NodeId == "" {
		l.NodeId = m.NodeID
	}
	if l.UnixNano == 0 {
		l.UnixNano = time.Now().UnixNano()
	}
	return l
}
