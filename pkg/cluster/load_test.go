package cluster

import (
	"sync"
	"testing"
	"time"
)

func TestNodeLoadTableUpdateAndSnapshot(t *testing.T) {
	table := NewNodeLoadTable()
	table.Update(NodeLoad{NodeId: "n1", Connections: 10, MaxConnections: 100, UnixNano: time.Now().UnixNano()})
	table.Update(NodeLoad{NodeId: "n2", Connections: 90, MaxConnections: 100, UnixNano: time.Now().UnixNano()})

	snap := table.Snapshot()
	if len(snap) != 2 || snap["n1"].Connections != 10 || snap["n2"].MaxConnections != 100 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// A returned snapshot must be an immutable copy: mutating it cannot
	// corrupt the table (critical on the PUBLISH hot path).
	snap["n3"] = NodeLoad{NodeId: "n3", Connections: 1}
	if _, ok := table.Get("n3"); ok {
		t.Fatal("Snapshot() leaked a mutable reference into the table")
	}
}

func TestNodeLoadTableIsStale(t *testing.T) {
	table := NewNodeLoadTable()
	table.Update(NodeLoad{NodeId: "fresh", UnixNano: time.Now().UnixNano()})
	table.Update(NodeLoad{NodeId: "old", UnixNano: time.Now().Add(-1 * time.Minute).UnixNano()})

	if table.IsStale("missing", time.Second) != true {
		t.Fatal("a node with no snapshot must be stale")
	}
	if table.IsStale("fresh", 10*time.Second) {
		t.Fatal("fresh node marked stale")
	}
	if !table.IsStale("old", 10*time.Second) {
		t.Fatal("old node marked fresh")
	}
}

func TestNodeLoadTableFreshNodes(t *testing.T) {
	table := NewNodeLoadTable()
	now := time.Now().UnixNano()
	table.Update(NodeLoad{NodeId: "a", UnixNano: now})
	table.Update(NodeLoad{NodeId: "b", UnixNano: now - int64(time.Minute)})
	got := table.FreshNodes([]string{"a", "b", "c"}, 10*time.Second)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("FreshNodes = %v, want [a]", got)
	}
}

func TestNodeLoadTableConcurrent(t *testing.T) {
	table := NewNodeLoadTable()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				table.Update(NodeLoad{NodeId: "n", Connections: int64(i*200 + j), UnixNano: time.Now().UnixNano()})
				_ = table.Snapshot()
				_ = table.IsStale("n", time.Second)
			}
		}(i)
	}
	wg.Wait()
	if _, ok := table.Get("n"); !ok {
		t.Fatal("entry lost under concurrent writes")
	}
}
