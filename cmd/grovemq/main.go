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

// Package main serves as the entry point for the GroveMQ application. It is
// responsible for initializing and orchestrating all the major components of the
// broker, including the MQTT server, the gRPC server for clustering, the
// metrics server, and the peer discovery mechanism. It also handles graceful
// shutdown on receiving termination signals.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/turtacn/grovemq/pkg/admin"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/cluster"
	"github.com/turtacn/grovemq/pkg/config"
	"github.com/turtacn/grovemq/pkg/connector"
	"github.com/turtacn/grovemq/pkg/dashboard"
	"github.com/turtacn/grovemq/pkg/discovery"
	"github.com/turtacn/grovemq/pkg/integration"
	"github.com/turtacn/grovemq/pkg/metrics"
	"github.com/turtacn/grovemq/pkg/monitor"
	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
	"github.com/turtacn/grovemq/pkg/rules"
	"github.com/turtacn/grovemq/pkg/tls"
	"google.golang.org/grpc"
)

const (
	// grpcPort is the port on which the gRPC server listens for inter-node
	// communication.
	grpcPort = ":8081"
	// metricsPort is the port on which the Prometheus metrics server listens.
	metricsPort = ":8082"
)

// main is the primary function that starts the GroveMQ broker.
// It follows these steps:
//  1. Parses command line arguments and loads configuration.
//  2. Sets up a unique node ID.
//  3. Initializes the main context for graceful shutdown.
//  4. Creates the cluster manager and the main broker, linking them together.
//  5. Configures authentication from the configuration file.
//  6. Starts the MQTT broker server in a goroutine.
//  7. Starts the gRPC server for cluster communication in a goroutine.
//  8. Starts the Prometheus metrics server in a goroutine.
//  9. Starts the peer discovery process in a goroutine.
//  10. Blocks and waits for a shutdown signal (SIGINT or SIGTERM) to gracefully
//     terminate the application.
func main() {
	// Parse command line flags
	var configPath = flag.String("config", "", "Path to configuration file (YAML or JSON)")
	var generateConfig = flag.String("generate-config", "", "Generate a sample configuration file at the specified path")
	flag.Parse()

	// Handle config generation
	if *generateConfig != "" {
		err := config.SaveConfig(config.DefaultConfig(), *generateConfig)
		if err != nil {
			log.Fatalf("Failed to generate config file: %v", err)
		}
		log.Printf("Sample configuration saved to %s", *generateConfig)
		return
	}

	log.Println("Starting GroveMQ Broker PoC (Phase 3)...")

	// Load configuration
	configFilePath := *configPath
	cfg, err := config.LoadConfig(configFilePath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override config with environment variables if set
	if envNodeID := os.Getenv("NODE_ID"); envNodeID != "" {
		cfg.Broker.NodeID = envNodeID
	}
	if envMQTTPort := os.Getenv("MQTT_PORT"); envMQTTPort != "" {
		cfg.Broker.MQTTPort = envMQTTPort
	}
	if envGRPCPort := os.Getenv("GRPC_PORT"); envGRPCPort != "" {
		cfg.Broker.GRPCPort = envGRPCPort
	}
	if envMetricsPort := os.Getenv("METRICS_PORT"); envMetricsPort != "" {
		cfg.Broker.MetricsPort = envMetricsPort
	}
	if envDashboardPort := os.Getenv("DASHBOARD_PORT"); envDashboardPort != "" {
		cfg.Broker.DashboardPort = envDashboardPort
	}

	nodeID := cfg.Broker.NodeID
	if nodeID == "" {
		nodeID, _ = os.Hostname()
		if nodeID == "" {
			nodeID = "local-node"
		}
	}
	log.Printf("Node ID: %s", nodeID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Setup Broker and Cluster Manager ---
	// The broker needs to be created first to pass its publish function to the manager.
	var b *broker.Broker
	// The address is sent to peers in Join requests so they can dial us back.
	// It must be reachable from other nodes, which "localhost" never is in a
	// container or multi-host deployment; deployments set NODE_ADVERTISE_ADDR.
	nodeAddress := resolveNodeAdvertiseAddr(cfg.Broker.GRPCPort)
	log.Printf("Node advertise address: %s", nodeAddress)
	clusterMgr := cluster.NewManager(nodeID, nodeAddress, func(topic string, payload []byte, qos byte) {
		if b != nil {
			b.RouteToLocalSubscribersWithQoS(topic, payload, qos)
		}
	})

	// --- Cluster role & topology (Core/Replicant model) ---
	// NODE_ROLE selects a Core (default) or Replicant node. CORE_NODES is a comma
	// separated list of "id=addr" entries describing the Core cluster; Replicants
	// connect only to Cores, Cores interconnect with each other. It is also used
	// to compute the election majority and to probe Core liveness.
	role := cluster.CoreNode
	if strings.EqualFold(os.Getenv("NODE_ROLE"), "replicant") {
		role = cluster.ReplicantNode
	}
	clusterMgr.SetRole(role)

	coreIDs, coreAddrMap := parseCoreNodes(os.Getenv("CORE_NODES"))
	if len(coreIDs) > 0 {
		clusterMgr.SetCoreNodes(coreIDs, coreAddrMap)
	}

	// Begin the (stopgap) leader election loop. Cores campaign for leadership;
	// Replicants only track Core reachability for graceful degradation. Real Raft
	// consensus (hashicorp/raft) will replace this in a later round.
	stopElection := clusterMgr.StartLeaderElection()
	defer stopElection()

	// --- Start Local Broker ---
	b = broker.New(nodeID, clusterMgr)

	// Apply operational knobs from config (New() already set safe defaults;
	// only override when the config value is explicitly set).
	if cfg.Broker.MailboxSize > 0 {
		b.MailboxSize = cfg.Broker.MailboxSize
	}
	if cfg.Broker.WriteTimeout > 0 {
		b.WriteTimeout = time.Duration(cfg.Broker.WriteTimeout) * time.Second
	}
	b.MaxConnections = cfg.Broker.MaxConnections

	// --- Configure Authentication from Config File ---
	err = cfg.ConfigureAuth(b.GetAuthChain())
	if err != nil {
		log.Fatalf("Failed to configure authentication: %v", err)
	}

	// --- Configure ACL (publish/subscribe authorization) from Config File ---
	if err = cfg.ConfigureACL(b.GetACLEngine()); err != nil {
		log.Fatalf("Failed to configure ACL: %v", err)
	}

	// --- Configure built-in hooks (rate-limit, audit) from Config File ---
	if err = cfg.ConfigureHooks(b.GetHooks()); err != nil {
		log.Fatalf("Failed to configure hooks: %v", err)
	}

	// --- Config hot-reload closure (shared by SIGHUP and the admin endpoint) ---
	// Re-reads the config file, re-validates, and re-applies the
	// runtime-reconfigurable settings (auth/ACL/hooks) WITHOUT restarting the
	// broker or dropping live connections. Listeners/ports are bound once at
	// startup and are intentionally NOT re-applied (changing them needs a
	// restart). If no config file was supplied, reload is unsupported.
	reload := func() error {
		if configFilePath == "" {
			return fmt.Errorf("no config file path (start with --config to enable hot reload)")
		}
		newCfg, lerr := config.LoadConfig(configFilePath)
		if lerr != nil {
			return lerr
		}
		if lerr = newCfg.ConfigureAuth(b.GetAuthChain()); lerr != nil {
			return fmt.Errorf("auth: %w", lerr)
		}
		if lerr = newCfg.ConfigureACL(b.GetACLEngine()); lerr != nil {
			return fmt.Errorf("acl: %w", lerr)
		}
		if lerr = newCfg.ConfigureHooks(b.GetHooks()); lerr != nil {
			return fmt.Errorf("hooks: %w", lerr)
		}
		// Ports/listeners are deliberately not re-applied here.
		return nil
	}

	// --- Initialize Advanced Components ---
	// Metrics Manager
	metricsManager := metrics.DefaultManager

	// Health Checker
	healthChecker := monitor.NewHealthChecker()

	// Certificate Manager
	certManager := tls.NewCertificateManager()

	// Connector Manager
	connectorManager := connector.NewConnectorManager()

	// Rule Engine
	ruleEngine := rules.NewRuleEngine(connectorManager)

	// Data Integration Engine
	integrationEngine := integration.NewDataIntegrationEngine()

	// Admin API Server
	adminAPI := admin.NewAPIServer(metricsManager, b)
	// Wire the config hot-reload closure (also triggered by SIGHUP) into the
	// admin reload endpoint.
	adminAPI.SetReloadHandler(reload)

	// Dashboard Server
	dashboardConfig := dashboard.DefaultConfig()
	// Use configured dashboard port, falling back to default if not set
	dashboardPort := cfg.Broker.DashboardPort
	if dashboardPort == "" {
		dashboardPort = ":18083"
	}
	// Extract port number from string (remove leading colon if present)
	if strings.HasPrefix(dashboardPort, ":") {
		dashboardPort = dashboardPort[1:]
	}
	if port, err := strconv.Atoi(dashboardPort); err == nil {
		dashboardConfig.Port = port
	} else {
		log.Printf("Invalid dashboard port %s, using default 18083", dashboardPort)
		dashboardConfig.Port = 18083
	}

	dashboardServer, err := dashboard.NewServer(
		dashboardConfig,
		adminAPI,
		metricsManager,
		healthChecker,
		certManager,
		connectorManager,
		ruleEngine,
		integrationEngine,
	)
	if err != nil {
		log.Fatalf("Failed to create dashboard server: %v", err)
	}

	// --- Start MQTT Broker ---
	go func() {
		if err := b.StartServer(ctx, cfg.Broker.MQTTPort); err != nil {
			log.Fatalf("Broker server failed: %v", err)
		}
	}()

	// --- Start MQTT-over-WebSocket Gateway (if configured) ---
	if cfg.Broker.WebSocketPort != "" {
		go func() {
			if err := b.StartWebSocketServer(ctx, cfg.Broker.WebSocketPort); err != nil {
				log.Fatalf("WebSocket broker server failed: %v", err)
			}
		}()
	}

	// --- Start gRPC Server for Clustering ---
	grpcServer := grpc.NewServer()
	clusterServer := cluster.NewServer(nodeID, clusterMgr)
	clusterpb.RegisterClusterServiceServer(grpcServer, clusterServer)

	lis, err := net.Listen("tcp", cfg.Broker.GRPCPort)
	if err != nil {
		log.Fatalf("Failed to listen for gRPC: %v", err)
	}
	go func() {
		log.Printf("gRPC server listening on %s", cfg.Broker.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()
	defer grpcServer.Stop()

	// --- Start Metrics Server ---
	go metrics.Serve(cfg.Broker.MetricsPort)

	// --- Start Dashboard Server ---
	go func() {
		if err := dashboardServer.Start(ctx); err != nil {
			log.Printf("Dashboard server error: %v", err)
		}
	}()

	// --- Start Discovery and Peer Connection ---
	go startDiscovery(ctx, clusterMgr)

	// --- Seed peer connections ---
	// Replicants connect only to Cores; Cores interconnect with the other Cores
	// (and any legacy PEER_NODES). CORE_NODES entries are skipped for the local
	// node itself.
	seedNodes := seedPeerAddresses(clusterMgr, role, coreIDs, coreAddrMap)
	if seedNodes != "" {
		go connectToPeers(ctx, clusterMgr, seedNodes)

		// Backfill durable state (sessions, retained messages) and the routing
		// table from a peer after joining the cluster. Retried for a short window
		// because peer connections are established asynchronously.
		go func() {
			for attempt := 0; attempt < 12; attempt++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(3 * time.Second):
				}
				clusterMgr.RequestStateSync(ctx)
			}
		}()
	}

	// --- Wait for Shutdown Signal ---
	// SIGINT/SIGTERM trigger a graceful shutdown; SIGHUP triggers a runtime
	// config reload (auth/ACL/hooks) without stopping the broker.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range signalChan {
			switch sig {
			case syscall.SIGHUP:
				log.Println("[INFO] SIGHUP received: reloading configuration")
				if err := reload(); err != nil {
					log.Printf("[ERROR] config reload failed: %v", err)
				} else {
					log.Println("[INFO] configuration reloaded successfully")
				}
			default:
				log.Printf("Shutdown signal (%v) received. Shutting down...", sig)
				cancel()
				return
			}
		}
	}()

	// Block until a shutdown signal cancels the context.
	<-ctx.Done()
}

// connectToPeers connects to static peer nodes. Each entry is either "id=addr"
// (preferred, gives a stable node ID) or a bare "host:port" address (the host is
// used as the peer ID, for backward compatibility with the old PEER_NODES
// format).
//
// - ctx: The main application context.
// - mgr: The cluster manager to which peers will be added.
// - peerNodes: Comma-separated list of peer entries.
func connectToPeers(ctx context.Context, mgr *cluster.Manager, peerNodes string) {
	// Wait a bit for local server to start
	time.Sleep(2 * time.Second)

	peers := strings.Split(peerNodes, ",")
	log.Printf("Attempting to connect to %d static peer nodes", len(peers))

	for _, entry := range peers {
		peerID, peerAddr := parsePeerEntry(strings.TrimSpace(entry))
		if peerAddr == "" {
			continue
		}
		log.Printf("Connecting to peer: %s at %s", peerID, peerAddr)
		go mgr.AddPeer(ctx, peerID, peerAddr)
	}

	// Retry connection periodically
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, entry := range peers {
				peerID, peerAddr := parsePeerEntry(strings.TrimSpace(entry))
				if peerAddr == "" {
					continue
				}
				go mgr.AddPeer(ctx, peerID, peerAddr)
			}
		}
	}
}

// resolveNodeAdvertiseAddr returns the address this node advertises to peers in
// Join requests. NODE_ADVERTISE_ADDR must be set to a dialable host:port in any
// container or multi-host deployment; the localhost fallback is only suitable
// for a single-host development cluster.
func resolveNodeAdvertiseAddr(grpcPort string) string {
	if addr := os.Getenv("NODE_ADVERTISE_ADDR"); addr != "" {
		return addr
	}
	return fmt.Sprintf("localhost%s", grpcPort)
}

// parsePeerEntry splits a peer entry of the form "id=addr" or "host:port" into a
// peer ID and address.
func parsePeerEntry(entry string) (id, addr string) {
	if entry == "" {
		return "", ""
	}
	if i := strings.Index(entry, "="); i >= 0 {
		return entry[:i], entry[i+1:]
	}
	return strings.Split(entry, ":")[0], entry
}

// parseCoreNodes parses the CORE_NODES environment variable
// ("id=addr,id=addr") into the list of Core IDs and an ID->address map used for
// leader-election liveness probes.
func parseCoreNodes(s string) ([]string, map[string]string) {
	ids := []string{}
	addrs := map[string]string{}
	if s == "" {
		return ids, addrs
	}
	for _, e := range strings.Split(s, ",") {
		id, addr := parsePeerEntry(strings.TrimSpace(e))
		if addr == "" {
			continue
		}
		ids = append(ids, id)
		addrs[id] = addr
	}
	return ids, addrs
}

// seedPeerAddresses builds the comma-separated peer list for this node. Cores are
// seeded with the other Cores; Replicants are seeded with all Cores. Legacy
// PEER_NODES entries are appended unchanged. The local node never seeds itself.
func seedPeerAddresses(mgr *cluster.Manager, role cluster.Role, coreIDs []string, coreAddrMap map[string]string) string {
	var parts []string
	for _, id := range coreIDs {
		if id == mgr.NodeID {
			continue
		}
		if addr, ok := coreAddrMap[id]; ok {
			parts = append(parts, id+"="+addr)
		}
	}
	if pn := os.Getenv("PEER_NODES"); pn != "" {
		for _, e := range strings.Split(pn, ",") {
			e = strings.TrimSpace(e)
			if e != "" {
				parts = append(parts, e)
			}
		}
	}
	return strings.Join(parts, ",")
}

// startDiscovery initializes and runs the peer discovery process. It is designed
// to work within a Kubernetes environment by default, using a headless service
// to find other broker pods. If not in a Kubernetes environment, it logs this
// fact and does nothing.
//
// The discovery process runs in a loop, periodically querying for peers and
// attempting to add them to the cluster manager.
//
//   - ctx: The main application context. The discovery loop will terminate when this
//     context is canceled.
//   - mgr: The cluster manager to which discovered peers will be added.
func startDiscovery(ctx context.Context, mgr *cluster.Manager) {
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	serviceName := "grovemq-headless"
	portName := "grpc"

	disc, err := discovery.NewKubeDiscovery(namespace, serviceName, portName)
	if err != nil {
		log.Printf("Could not initialize Kubernetes discovery: %v", err)
		if strings.Contains(err.Error(), "in-cluster configuration") {
			log.Println("This is not a Kubernetes environment. Skipping peer discovery.")
		}
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers, err := disc.DiscoverPeers(ctx)
			if err != nil {
				log.Printf("Failed to discover peers: %v", err)
				continue
			}
			log.Printf("Discovered %d peers", len(peers))
			for _, peer := range peers {
				go mgr.AddPeer(ctx, peer.ID, peer.Address)
			}
		}
	}
}
