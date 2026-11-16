package cluster

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrNoCandidates is returned when a selector has no reachable node to pick.
var ErrNoCandidates = errors.New("cluster: no candidate nodes")

// Selection is the outcome of matching a published topic against the
// replicated route index. Nodes holds ordinary-subscription fan-out targets;
// Groups holds, per shared-subscription group, the candidate nodes that own
// at least one member of that group.
type Selection struct {
	Nodes  []string
	Groups map[string][]string

	// SharedTarget is filled when exactly one shared group matched. It is
	// diagnostic/compat sugar: RouteRemote selects per group via Groups.
	SharedTarget string
}

// NodeSelector picks exactly one node out of a de-duplicated candidate set.
//
// Implementations must be pure in-memory operations: no RPC, no lock held
// across network I/O, and deterministic tie-breaking. Selection happens at
// the publishing node only (remote nodes merely receive the forward), so a
// local cursor cannot produce cross-node disagreement.
type NodeSelector interface {
	Select(group, topic string, candidates []string, load NodeLoadSnapshot) (string, error)
	Name() string
}

// sortedCandidates returns a sorted, de-duplicated copy. Sorting is what makes
// ties deterministic (lexicographic node ID) across every node in the cluster.
func sortedCandidates(candidates []string) []string {
	if len(candidates) == 0 {
		return nil
	}
	out := append([]string(nil), candidates...)
	sort.Strings(out)
	uniq := out[:0]
	for _, c := range out {
		if c == "" {
			continue
		}
		if len(uniq) == 0 || uniq[len(uniq)-1] != c {
			uniq = append(uniq, c)
		}
	}
	return uniq
}

// RoundRobinSelector keeps one atomic cursor per shared group. Bucketing by
// group (rather than one global cursor) keeps independent groups from shifting
// each other's target.
type RoundRobinSelector struct {
	cursors sync.Map // group -> *atomic.Uint64
}

// Select rotates within the group's own bucket.
func (s *RoundRobinSelector) Select(group, topic string, candidates []string, _ NodeLoadSnapshot) (string, error) {
	list := sortedCandidates(candidates)
	if len(list) == 0 {
		return "", ErrNoCandidates
	}
	v, _ := s.cursors.LoadOrStore(group, &atomic.Uint64{})
	cursor := v.(*atomic.Uint64)
	idx := cursor.Add(1) - 1
	return list[int(idx%uint64(len(list)))], nil
}

// Name implements NodeSelector.
func (s *RoundRobinSelector) Name() string { return "round_robin" }

// LeastConnectionsSelector picks argmin(connections / max_connections).
// A node with no load record is treated as infinitely loaded (worst), so
// load-less nodes only win when every candidate lacks data. Ties are broken
// by lexicographic node ID because candidates are sorted and only a strictly
// smaller ratio replaces the current best.
type LeastConnectionsSelector struct{}

// Select implements NodeSelector.
func (s *LeastConnectionsSelector) Select(group, topic string, candidates []string, load NodeLoadSnapshot) (string, error) {
	list := sortedCandidates(candidates)
	if len(list) == 0 {
		return "", ErrNoCandidates
	}
	best := ""
	bestRatio := math.Inf(1)
	for _, nodeID := range list {
		ratio := math.Inf(1)
		if l, ok := load[nodeID]; ok {
			if l.MaxConnections > 0 {
				ratio = float64(l.Connections) / float64(l.MaxConnections)
			} else {
				ratio = float64(l.Connections)
			}
		}
		if best == "" || ratio < bestRatio {
			best, bestRatio = nodeID, ratio
		}
	}
	return best, nil
}

// Name implements NodeSelector.
func (s *LeastConnectionsSelector) Name() string { return "least_connections" }

// ConsistentHashSelector maps a group to a node with FNV-1a. The key is the
// group alone (not group+topic): every topic of one shared group must land on
// the same node, otherwise the group's message order would be scattered.
type ConsistentHashSelector struct{}

// Select implements NodeSelector.
func (s *ConsistentHashSelector) Select(group, topic string, candidates []string, _ NodeLoadSnapshot) (string, error) {
	list := sortedCandidates(candidates)
	if len(list) == 0 {
		return "", ErrNoCandidates
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(group))
	return list[int(h.Sum64()%uint64(len(list)))], nil
}

// Name implements NodeSelector.
func (s *ConsistentHashSelector) Name() string { return "consistent_hash" }

// NewNodeSelector builds a selector by configuration name. Unknown names fall
// back to round_robin so a typo can never disable forwarding.
func NewNodeSelector(name string) NodeSelector {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "round_robin", "rr":
		return &RoundRobinSelector{}
	case "least_connections", "leastconn", "least-connections":
		return &LeastConnectionsSelector{}
	case "consistent_hash", "hash", "consistent-hash":
		return &ConsistentHashSelector{}
	default:
		return &RoundRobinSelector{}
	}
}

// selectorNames is used by config validation to report the accepted values.
func selectorNames() string {
	return fmt.Sprintf("%s, %s, %s",
		(&RoundRobinSelector{}).Name(),
		(&LeastConnectionsSelector{}).Name(),
		(&ConsistentHashSelector{}).Name())
}
