#!/bin/bash
# Stop the 3-node GroveMQ cluster

echo "Stopping cluster nodes..."
pkill -f "grovemq" || true
echo "Cluster stopped."
