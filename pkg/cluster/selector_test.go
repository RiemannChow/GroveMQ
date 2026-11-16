package cluster

import "testing"

func TestSelectorsRejectEmptyCandidates(t *testing.T) {
	for _, name := range []string{"round_robin", "least_connections", "consistent_hash"} {
		s := NewNodeSelector(name)
		if _, err := s.Select("g", "t", nil, nil); err != ErrNoCandidates {
			t.Errorf("%s: empty candidates err = %v, want ErrNoCandidates", name, err)
		}
		if _, err := s.Select("g", "t", []string{"", ""}, nil); err != ErrNoCandidates {
			t.Errorf("%s: blank candidates err = %v, want ErrNoCandidates", name, err)
		}
	}
}

func TestSelectorsDeterministicAndInRange(t *testing.T) {
	candidates := []string{"n3", "n1", "n2", "n1"} // unsorted + duplicate
	load := NodeLoadSnapshot{
		"n1": {NodeId: "n1", Connections: 5, MaxConnections: 10},
		"n2": {NodeId: "n2", Connections: 1, MaxConnections: 10},
		"n3": {NodeId: "n3", Connections: 9, MaxConnections: 10},
	}
	for _, name := range []string{"round_robin", "least_connections", "consistent_hash"} {
		s := NewNodeSelector(name)
		first, err := s.Select("g", "sensors/temp", candidates, load)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if first != "n1" && first != "n2" && first != "n3" {
			t.Fatalf("%s: selected unknown node %q", name, first)
		}
		// Round-robin is the one stateful strategy: its per-group cursor
		// advances on every call by design. Determinism (no random tie-break)
		// is required of the two stateless strategies.
		if name == "round_robin" {
			continue
		}
		for i := 0; i < 20; i++ {
			got, err := s.Select("g", "sensors/temp", candidates, load)
			if err != nil || got != first {
				t.Fatalf("%s: non-deterministic: %q vs %q (err=%v)", name, got, first, err)
			}
		}
	}
}

func TestRoundRobinIndependentGroupBuckets(t *testing.T) {
	s := NewNodeSelector("round_robin")
	candidates := []string{"n1", "n2"}
	// Each group has its own cursor; group "a" must not shift group "b".
	a1, _ := s.Select("a", "t", candidates, nil)
	b1, _ := s.Select("b", "t", candidates, nil)
	a2, _ := s.Select("a", "t", candidates, nil)
	b2, _ := s.Select("b", "t", candidates, nil)
	if a2 == a1 || b2 == b1 {
		t.Fatalf("round-robin did not advance: a=%s,%s b=%s,%s", a1, a2, b1, b2)
	}
	if a1 != b1 || a2 != b2 {
		t.Fatalf("group buckets are coupled: a=%s,%s b=%s,%s", a1, a2, b1, b2)
	}
}

func TestLeastConnectionsPicksLeastLoaded(t *testing.T) {
	s := NewNodeSelector("least_connections")
	load := NodeLoadSnapshot{
		"n1": {NodeId: "n1", Connections: 90, MaxConnections: 100},
		"n2": {NodeId: "n2", Connections: 10, MaxConnections: 100},
		"n3": {NodeId: "n3", Connections: 50, MaxConnections: 100},
	}
	got, err := s.Select("g", "t", []string{"n3", "n1", "n2"}, load)
	if err != nil || got != "n2" {
		t.Fatalf("got %q err=%v, want n2", got, err)
	}
}

func TestLeastConnectionsTieBreakByNodeID(t *testing.T) {
	s := NewNodeSelector("least_connections")
	load := NodeLoadSnapshot{
		"n1": {NodeId: "n1", Connections: 5, MaxConnections: 10},
		"n2": {NodeId: "n2", Connections: 5, MaxConnections: 10},
	}
	// Unsorted input proves the tie is broken lexicographically, not by input order.
	got, err := s.Select("g", "t", []string{"n2", "n1"}, load)
	if err != nil || got != "n1" {
		t.Fatalf("got %q err=%v, want n1 (lexicographic tie-break)", got, err)
	}
}

func TestLeastConnectionsMissingLoadIsWorst(t *testing.T) {
	s := NewNodeSelector("least_connections")
	load := NodeLoadSnapshot{
		"known": {NodeId: "known", Connections: 9, MaxConnections: 10},
	}
	got, err := s.Select("g", "t", []string{"unknown", "known"}, load)
	if err != nil || got != "known" {
		t.Fatalf("got %q err=%v, want known (unknown has no load data)", got, err)
	}
	// All-missing still resolves deterministically to the lexicographic first.
	got, err = s.Select("g", "t", []string{"z", "a"}, nil)
	if err != nil || got != "a" {
		t.Fatalf("got %q err=%v, want a", got, err)
	}
}

func TestConsistentHashGroupSticky(t *testing.T) {
	s := NewNodeSelector("consistent_hash")
	candidates := []string{"n1", "n2", "n3"}
	g1t1, _ := s.Select("g1", "a/b", candidates, nil)
	g1t2, _ := s.Select("g1", "totally/different/topic", candidates, nil)
	g2, _ := s.Select("g2", "a/b", candidates, nil)
	// Key is the group alone: all topics of one group land on the same node.
	if g1t1 != g1t2 {
		t.Fatalf("group not sticky: %q vs %q", g1t1, g1t2)
	}
	if g1t1 != g2 {
		t.Logf("note: g1 and g2 hash to the same node (%q); valid with %d candidates", g1t1, len(candidates))
	}
}

func TestNewNodeSelectorFallback(t *testing.T) {
	if got := NewNodeSelector("typo").Name(); got != "round_robin" {
		t.Fatalf("unknown strategy must fall back, got %q", got)
	}
	if got := NewNodeSelector("").Name(); got != "round_robin" {
		t.Fatalf("default must be round_robin, got %q", got)
	}
	for _, name := range []string{"least_connections", "leastconn", "consistent_hash", "hash"} {
		if NewNodeSelector(name).Name() == "round_robin" && name != "hash" {
			t.Errorf("alias %q not recognised", name)
		}
	}
}
