package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/admin"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/config"
	"github.com/turtacn/grovemq/pkg/metrics"
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

func TestA2AMountpointRisk(t *testing.T) {
	cases := []struct {
		url  string
		want bool
	}{
		{"mqtt://broker:1883", false},
		{"mqtts://broker:8883/", false},
		{"mqtt://broker:1883/factory", false},
		{"mqtt://broker:1883/factory/a2a", true},
		{"mqtt://broker:1883/org/unit/agent", true},
		{"://bad", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, a2aMountpointRisk(tc.url), tc.url)
	}
}

func TestA2ARegistryDisabledLeavesBrokerUnchanged(t *testing.T) {
	require.False(t, config.DefaultConfig().Broker.A2ARegistry.Enabled, "A2A Registry must be off by default")

	b := broker.New("a2a-disabled-node", nil)
	api := admin.NewAPIServer(metrics.NewMetricsManager(), b)
	before := len(b.GetHooks().List())

	reg, err := configureA2ARegistry(b, api, config.DefaultConfig().Broker.A2ARegistry)
	require.NoError(t, err)
	require.Nil(t, reg)
	assert.Equal(t, before, len(b.GetHooks().List()), "disabled Registry must not install hooks")
	assert.Nil(t, b.RetainedAnnotationFunc)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v5/a2a/agents", nil))
	assert.Equal(t, http.StatusNotImplemented, rr.Code)
}
