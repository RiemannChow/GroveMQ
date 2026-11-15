package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveNodeAdvertiseAddr(t *testing.T) {
	// Local development fallback: the gRPC port with a localhost host.
	t.Setenv("NODE_ADVERTISE_ADDR", "")
	assert.Equal(t, "localhost:8081", resolveNodeAdvertiseAddr(":8081"))
	assert.Equal(t, "localhost:19082", resolveNodeAdvertiseAddr(":19082"))

	// Deployments must be able to advertise a peer-reachable address; using
	// "localhost" inside a container would make peers dial themselves.
	t.Setenv("NODE_ADVERTISE_ADDR", "grovemq-node2:8081")
	assert.Equal(t, "grovemq-node2:8081", resolveNodeAdvertiseAddr(":8081"))

	t.Setenv("NODE_ADVERTISE_ADDR", "10.0.0.7:8081")
	assert.Equal(t, "10.0.0.7:8081", resolveNodeAdvertiseAddr(":18081"))
}
