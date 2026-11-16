package cluster

import (
	"reflect"
	"testing"
)

func TestMatchWithGroups(t *testing.T) {
	m := NewManager("n0", "localhost:8081", nil)
	// Ordinary subscriptions must fan-out.
	m.AddRemoteRoute("sensors/#", "n1")
	m.AddRemoteRoute("sensors/temp", "n2")
	// Shared groups keep their $share/... route key; matchWithGroups strips the
	// prefix in memory before matching, leaving the replicated table untouched.
	m.AddRemoteRoute("$share/g1/sensors/#", "n1")
	m.AddRemoteRoute("$share/g1/sensors/#", "n2")
	m.AddRemoteRoute("$share/g2/sensors/#", "n3")

	sel := m.matchWithGroups("sensors/temp")
	if !reflect.DeepEqual(sel.Nodes, []string{"n1", "n2"}) {
		t.Fatalf("Nodes = %v, want [n1 n2]", sel.Nodes)
	}
	if !reflect.DeepEqual(sel.Groups["g1"], []string{"n1", "n2"}) {
		t.Fatalf("g1 = %v, want [n1 n2]", sel.Groups["g1"])
	}
	if !reflect.DeepEqual(sel.Groups["g2"], []string{"n3"}) {
		t.Fatalf("g2 = %v, want [n3]", sel.Groups["g2"])
	}

	// A topic that only matches the wildcard filter keeps g1/g2 but drops the
	// exact-topic ordinary route.
	sel2 := m.matchWithGroups("sensors/humidity")
	if !reflect.DeepEqual(sel2.Nodes, []string{"n1"}) {
		t.Fatalf("Nodes = %v, want [n1]", sel2.Nodes)
	}
	if len(sel2.Groups) != 2 {
		t.Fatalf("Groups = %v, want g1 and g2", sel2.Groups)
	}
}

func TestMatchWithGroupsBaselineUnchanged(t *testing.T) {
	m := NewManager("n0", "localhost:8081", nil)
	m.AddRemoteRoute("$share/g1/sensors/#", "n1")
	m.AddRemoteRoute("$share/g1/sensors/#", "n2")

	// The historical matcher has no $share awareness: default "fanout" policy
	// must therefore keep producing no remote targets (baseline behaviour,
	// design §3 G5). The fix lives only in the opt-in single policy.
	if got := m.MatchRemoteSubscribers("sensors/temp"); len(got) != 0 {
		t.Fatalf("baseline MatchRemoteSubscribers = %v, want empty", got)
	}
	if sel := m.matchWithGroups("sensors/temp"); len(sel.Groups["g1"]) != 2 {
		t.Fatalf("single-policy matcher lost the shared group: %v", sel.Groups)
	}
}

func TestMatchWithGroupsNoGroups(t *testing.T) {
	m := NewManager("n0", "localhost:8081", nil)
	m.AddRemoteRoute("a/+/c", "n1")
	m.AddRemoteRoute("a/#", "n1")
	m.AddRemoteRoute("a/#", "n2")
	sel := m.matchWithGroups("a/b/c")
	if !reflect.DeepEqual(sel.Nodes, []string{"n1", "n2"}) {
		t.Fatalf("Nodes = %v, want [n1 n2]", sel.Nodes)
	}
	if len(sel.Groups) != 0 {
		t.Fatalf("Groups = %v, want empty", sel.Groups)
	}
}

func TestSetBalanceConfig(t *testing.T) {
	m := NewManager("n0", "localhost:8081", nil)
	m.SetBalanceConfig("single", "consistent_hash")
	m.mu.RLock()
	policy := m.sharedRoutePolicy
	sel := m.selector
	m.mu.RUnlock()
	if policy != "single" || sel.Name() != "consistent_hash" {
		t.Fatalf("policy=%q selector=%q", policy, sel.Name())
	}
	// Unknown strategy never disables forwarding.
	m.SetBalanceConfig("single", "nope")
	m.mu.RLock()
	sel = m.selector
	m.mu.RUnlock()
	if sel.Name() != "round_robin" {
		t.Fatalf("unknown strategy = %q, want round_robin fallback", sel.Name())
	}
}

func TestLocalLoadDefaults(t *testing.T) {
	m := NewManager("n0", "localhost:8081", nil)
	l := m.LocalLoad()
	if l.NodeId != "n0" || l.UnixNano <= 0 {
		t.Fatalf("LocalLoad = %+v", l)
	}
	m.LocalLoadFunc = func() NodeLoad {
		return NodeLoad{Connections: 42, MaxConnections: 100}
	}
	l = m.LocalLoad()
	if l.NodeId != "n0" || l.Connections != 42 || l.UnixNano <= 0 {
		t.Fatalf("LocalLoad with func = %+v", l)
	}
}
