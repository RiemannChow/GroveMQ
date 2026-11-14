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

package cluster

import (
	"context"
	"log"

	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
)

// Server implements the gRPC handlers for the ClusterService. It receives
// requests from peer nodes and uses the cluster Manager to update the local
// cluster state, such as adding new peers or updating routing information.
type Server struct {
	clusterpb.UnimplementedClusterServiceServer
	// NodeID is the unique identifier of the local node.
	NodeID  string
	manager *Manager
}

// NewServer creates and initializes a new Server instance.
//
// - nodeID: The unique identifier of the local node.
// - manager: A pointer to the cluster Manager that the server will interact with.
func NewServer(nodeID string, manager *Manager) *Server {
	return &Server{
		NodeID:  nodeID,
		manager: manager,
	}
}

// Join is the gRPC handler for a JoinRequest from a peer. It processes the
// request and, if successful, allows the peer to join the cluster.
//
// - ctx: The context for the gRPC call.
// - req: The join request from the peer.
//
// Returns a response indicating success or failure.
func (s *Server) Join(ctx context.Context, req *clusterpb.JoinRequest) (*clusterpb.JoinResponse, error) {
	log.Printf("Received Join request from node %s at %s", req.Node.NodeId, req.Node.Address)

	// Add the joining peer to our peer list using background context (not the RPC request context)
	// The request context gets canceled when the RPC completes, but peer connection should persist
	go s.manager.AddPeer(context.Background(), req.Node.NodeId, req.Node.Address)

	// In a real implementation, we would add the node to our peer list.
	return &clusterpb.JoinResponse{
		Success:      true,
		ClusterId:    "cluster-1", // Add a cluster ID
		Message:      "Welcome to the cluster!",
		ClusterNodes: []*clusterpb.NodeInfo{{NodeId: s.NodeID}},
	}, nil
}

// BatchUpdateRoutes is the gRPC handler for a request to add or remove routing
// entries. A "register" (or legacy "add") operation records that the given node
// subscribes to a filter; an "unregister" removes it. Registrations arriving at
// a Core are funnelled through the leader election rules (leader applies and
// replicates, followers forward to the leader).
//
// - ctx: The context for the gRPC call.
// - req: The request containing the route operations.
//
// Returns a response indicating the number of routes updated.
func (s *Server) BatchUpdateRoutes(ctx context.Context, req *clusterpb.BatchUpdateRoutesRequest) (*clusterpb.BatchUpdateRoutesResponse, error) {
	log.Printf("Received BatchUpdateRoutes request from node %s (op=%s)", req.FromNode, req.OpType)
	remove := req.OpType == "unregister"
	for _, route := range req.Routes {
		for _, nodeID := range route.NodeIds {
			s.manager.applyRouteRegistration(route.Topic, nodeID, remove)
		}
	}
	return &clusterpb.BatchUpdateRoutesResponse{
		Success:      true,
		UpdatedCount: int32(len(req.Routes)),
	}, nil
}

// ForwardPublish is the gRPC handler for a forwarded PUBLISH message from a
// peer. It delivers the message to local subscribers, and — when the message is
// a relay (ToNode set) — forwards it onward toward the final destination. This
// lets a Replicant that has no direct link to the subscriber deliver through a
// Core without the Core re-broadcasting to every matching subscriber.
//
// - ctx: The context for the gRPC call.
// - req: The forwarded message.
//
// Returns an acknowledgment of receipt.
func (s *Server) ForwardPublish(ctx context.Context, req *clusterpb.PublishForward) (*clusterpb.ForwardAck, error) {
	log.Printf("Received ForwardPublish request for topic '%s' from node %s (to=%s, qos=%d)", req.Topic, req.FromNode, req.ToNode, req.Qos)
	if s.manager.LocalPublishFunc != nil {
		s.manager.LocalPublishFunc(req.Topic, req.Payload, byte(req.Qos))
	}
	if req.ToNode != "" && req.ToNode != s.NodeID {
		// Relay toward the final destination, never sending back to the node we
		// received it from (prevents loops).
		s.manager.forwardToNodeFrom(req.Topic, req.Payload, req.ToNode, req.FromNode, byte(req.Qos))
	}
	return &clusterpb.ForwardAck{Success: true}, nil
}

// ReplicateState is the gRPC handler for durable-state replication. It applies
// the received entries into the local durable stores so that state survives the
// failure of the originating node.
func (s *Server) ReplicateState(ctx context.Context, req *clusterpb.ReplicateStateRequest) (*clusterpb.ReplicateStateResponse, error) {
	applied := s.manager.ApplyStateEntries(ctx, req.Entries)
	return &clusterpb.ReplicateStateResponse{
		Success:      true,
		AppliedCount: int32(applied),
	}, nil
}

// SyncState is the gRPC handler that returns a full snapshot of this node's
// durable state to a joining peer for backfill.
func (s *Server) SyncState(ctx context.Context, req *clusterpb.SyncStateRequest) (*clusterpb.SyncStateResponse, error) {
	entries := s.manager.BuildSnapshot(ctx)
	return &clusterpb.SyncStateResponse{
		Success: true,
		Entries: entries,
	}, nil
}

// SyncNodeStatus is the gRPC handler used as a lightweight liveness probe during
// leader election. It simply acknowledges, letting a probing Core confirm the
// peer's gRPC endpoint is reachable.
func (s *Server) SyncNodeStatus(ctx context.Context, req *clusterpb.SyncNodeStatusRequest) (*clusterpb.SyncNodeStatusResponse, error) {
	return &clusterpb.SyncNodeStatusResponse{
		Success:  true,
		AllNodes: []*clusterpb.NodeInfo{{NodeId: s.NodeID, Address: s.manager.NodeAddress}},
	}, nil
}
