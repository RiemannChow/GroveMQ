package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
)

func collectForwards(t *testing.T, ch <-chan *clusterpb.PublishForward, want int) []*clusterpb.PublishForward {
	t.Helper()
	got := make([]*clusterpb.PublishForward, 0, want)
	deadline := time.After(5 * time.Second)
	for len(got) < want {
		select {
		case r := <-ch:
			got = append(got, r)
		case <-deadline:
			t.Fatalf("timed out: got %d forwards, want %d", len(got), want)
		}
	}
	return got
}

// TestRouteRemoteSingleSharedGroupOnce is G1: 3 nodes each own one member of
// the same shared group; publishing N messages must produce exactly N
// group-scoped forwards across the cluster (no duplicates, no loss), while
// ordinary subscriptions keep fanning out.
func TestRouteRemoteSingleSharedGroupOnce(t *testing.T) {
	ch := make(chan *clusterpb.PublishForward, 256)
	mockSrv := &mockClusterServer{
		ForwardPublishFunc: func(ctx context.Context, req *clusterpb.PublishForward) (*clusterpb.ForwardAck, error) {
			ch <- req
			return &clusterpb.ForwardAck{Success: true}, nil
		},
	}
	m, _ := setupTestManager(t, mockSrv)
	for _, id := range []string{"n1", "n2", "n3"} {
		m.AddPeer(context.Background(), id, "bufnet")
	}

	// Ordinary subscribers on n1 and n2 must fan-out.
	m.AddRemoteRoute("sensors/temp", "n1")
	m.AddRemoteRoute("sensors/#", "n2")
	// One shared group, three candidate nodes.
	for _, id := range []string{"n1", "n2", "n3"} {
		m.AddRemoteRoute("$share/g/sensors/temp", id)
	}

	now := time.Now().UnixNano()
	for _, id := range []string{"n1", "n2", "n3"} {
		m.loadTable.Update(NodeLoad{NodeId: id, Connections: 1, MaxConnections: 100, UnixNano: now})
	}
	m.SetBalanceConfig("single", "consistent_hash")

	target, err := NewNodeSelector("consistent_hash").Select("g", "sensors/temp", []string{"n1", "n2", "n3"}, nil)
	require.NoError(t, err)
	perMsg := 2 // ordinary fan-out targets n1, n2
	if target != "n1" && target != "n2" {
		perMsg = 3 // group selected a third node
	}

	const msgs = 20
	for i := 0; i < msgs; i++ {
		m.RouteRemote("sensors/temp", []byte("p"), 0)
	}

	got := collectForwards(t, ch, msgs*perMsg)
	scoped := 0
	for _, r := range got {
		if len(r.SharedGroups) > 0 {
			scoped++
			assert.Equal(t, []string{"g"}, r.SharedGroups)
			assert.NotZero(t, r.SelectorEpoch, "selector_epoch must be stamped")
		}
	}
	assert.Equal(t, msgs, scoped, "each message must produce exactly one group-scoped forward")
}

// TestRouteRemoteSingleFallsBackOnStaleLoad is the loss-avoiding degradation
// (design §5.5): with no load data the group fans out to every candidate
// instead of being routed to a possibly dead node.
func TestRouteRemoteSingleFallsBackOnStaleLoad(t *testing.T) {
	ch := make(chan *clusterpb.PublishForward, 64)
	mockSrv := &mockClusterServer{
		ForwardPublishFunc: func(ctx context.Context, req *clusterpb.PublishForward) (*clusterpb.ForwardAck, error) {
			ch <- req
			return &clusterpb.ForwardAck{Success: true}, nil
		},
	}
	m, _ := setupTestManager(t, mockSrv)
	for _, id := range []string{"n1", "n2", "n3"} {
		m.AddPeer(context.Background(), id, "bufnet")
		m.AddRemoteRoute("$share/g/t", id)
	}
	m.SetBalanceConfig("single", "round_robin")

	m.RouteRemote("t", []byte("p"), 0)

	got := collectForwards(t, ch, 3)
	scoped := 0
	for _, r := range got {
		if len(r.SharedGroups) > 0 {
			scoped++
			assert.Equal(t, []string{"g"}, r.SharedGroups)
		}
	}
	assert.Equal(t, 3, scoped, "stale load must fan the group out to all 3 candidates")
}

// TestRouteRemoteDefaultFanoutUnchanged is G5: the default policy reproduces
// baseline fan-out byte-for-byte (no group metadata on the wire).
func TestRouteRemoteDefaultFanoutUnchanged(t *testing.T) {
	ch := make(chan *clusterpb.PublishForward, 16)
	mockSrv := &mockClusterServer{
		ForwardPublishFunc: func(ctx context.Context, req *clusterpb.PublishForward) (*clusterpb.ForwardAck, error) {
			ch <- req
			return &clusterpb.ForwardAck{Success: true}, nil
		},
	}
	m, _ := setupTestManager(t, mockSrv)
	for _, id := range []string{"n1", "n2"} {
		m.AddPeer(context.Background(), id, "bufnet")
		m.AddRemoteRoute("a/#", id)
	}

	m.RouteRemote("a/b", []byte("p"), 0)

	got := collectForwards(t, ch, 2)
	for _, r := range got {
		assert.Empty(t, r.SharedGroups)
		assert.Zero(t, r.SelectorEpoch)
	}
}
