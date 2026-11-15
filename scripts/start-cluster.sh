#!/bin/bash
# Start a 3-node GroveMQ cluster locally
#
# Build is delegated to ./scripts/run.sh, which handles the Go >= 1.25 toolchain
# and CGO_ENABLED=0 automatically. Per-node ports/identity come from the env vars
# that cmd/grovemq/main.go reads (NODE_ID, *_PORT, PEER_NODES).
# GROVEMQ_DATA_DIR enables the durable session/retained store and cluster state
# replication; each node writes to ./data/<NODE_ID>.

set -e

# Always operate from the repo root, regardless of CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# Build the application (shared one-click script)
echo "Building grovemq..."
./scripts/run.sh build

# Create log directory
mkdir -p ./logs

# Kill existing processes
echo "Stopping existing cluster nodes..."
pkill -f "grovemq" || true
sleep 1

# Start node 1
echo "Starting node1..."
NODE_ID=node1 \
MQTT_PORT=:1883 \
GRPC_PORT=:8081 \
METRICS_PORT=:8082 \
DASHBOARD_PORT=:18083 \
GROVEMQ_DATA_DIR=./data \
./bin/grovemq > ./logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node1 started with PID: $NODE1_PID"

# Wait for node1 to start
sleep 2

# Start node 2
echo "Starting node2..."
NODE_ID=node2 \
MQTT_PORT=:1884 \
GRPC_PORT=:8083 \
METRICS_PORT=:8084 \
DASHBOARD_PORT=:18084 \
PEER_NODES=localhost:8081 \
GROVEMQ_DATA_DIR=./data \
./bin/grovemq > ./logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node2 started with PID: $NODE2_PID"

# Wait for node2 to start
sleep 2

# Start node 3
echo "Starting node3..."
NODE_ID=node3 \
MQTT_PORT=:1885 \
GRPC_PORT=:8085 \
METRICS_PORT=:8086 \
DASHBOARD_PORT=:18085 \
PEER_NODES=localhost:8081,localhost:8083 \
GROVEMQ_DATA_DIR=./data \
./bin/grovemq > ./logs/node3.log 2>&1 &
NODE3_PID=$!
echo "Node3 started with PID: $NODE3_PID"

echo ""
echo "Cluster started successfully!"
echo "Node1 PID: $NODE1_PID (MQTT: 1883, gRPC: 8081, Metrics: 8082, Dashboard: 18083)"
echo "Node2 PID: $NODE2_PID (MQTT: 1884, gRPC: 8083, Metrics: 8084, Dashboard: 18084)"
echo "Node3 PID: $NODE3_PID (MQTT: 1885, gRPC: 8085, Metrics: 8086, Dashboard: 18085)"
echo ""
echo "Log files:"
echo "  - ./logs/node1.log"
echo "  - ./logs/node2.log"
echo "  - ./logs/node3.log"
echo ""
echo "Durable state: ./data/node1, ./data/node2, ./data/node3"
echo ""
echo "To stop the cluster, run: ./scripts/stop-cluster.sh"
