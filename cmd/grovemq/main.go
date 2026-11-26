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
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/turtacn/grovemq/pkg/a2a/gateway"
	"github.com/turtacn/grovemq/pkg/a2a/registry"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/admin"
	"github.com/turtacn/grovemq/pkg/balancer"
	"github.com/turtacn/grovemq/pkg/broker"
	"github.com/turtacn/grovemq/pkg/cluster"
	"github.com/turtacn/grovemq/pkg/config"
	"github.com/turtacn/grovemq/pkg/dashboard"
	"github.com/turtacn/grovemq/pkg/discovery"
	"github.com/turtacn/grovemq/pkg/hooks"
	"github.com/turtacn/grovemq/pkg/metrics"
	"github.com/turtacn/grovemq/pkg/monitor"
	clusterpb "github.com/turtacn/grovemq/pkg/proto/cluster"
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

	// Load-balancing configuration (docs/design/LOAD_BALANCING.md). Every value
	// can be overridden for container deploys; defaults ("off" / "fanout" /
	// "round_robin") keep rolling upgrades safe (design G5).
	balance := cfg.Broker.Balance
	if v := os.Getenv("GROVEMQ_SHARED_ROUTE_POLICY"); v != "" {
		balance.SharedRoutePolicy = v
	}
	if v := os.Getenv("GROVEMQ_LB_STRATEGY"); v != "" {
		balance.LBStrategy = v
	}
	if v := os.Getenv("GROVEMQ_ACCESS_LB"); v != "" {
		balance.AccessLB = v
	}
	if v := os.Getenv("MQTT_ADVERTISE_ADDR"); v != "" {
		balance.MQTTAdvertiseAddr = v
	}
	if v := os.Getenv("GROVEMQ_LB_SOFT_LIMIT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			balance.SoftLimit = f
		} else {
			log.Printf("[WARN] ignoring invalid GROVEMQ_LB_SOFT_LIMIT %q: %v", v, err)
		}
	}
	b.SetBalanceConfig(balance.SharedRoutePolicy, balance.LBStrategy)

	// Access layer (design §6.2, Mode B): hand the CONNECT decision to the
	// admission controller and validate one-hop redirect credentials on the
	// target node. Both hooks are nil unless redirect mode is enabled, so the
	// default configuration never changes connection behaviour.
	if balance.AccessLB == "redirect" || balance.AccessLB == "both" {
		secret := []byte(os.Getenv("GROVEMQ_LB_SECRET"))
		if len(secret) == 0 {
			log.Printf("[WARN] GROVEMQ_LB_SECRET is unset; redirect tokens fall back to the built-in shared secret. Set the same value on every node in production.")
		}
		if balance.MQTTAdvertiseAddr == "" {
			log.Printf("[WARN] GROVEMQ_ACCESS_LB=%s but MQTT_ADVERTISE_ADDR is empty; access-layer redirect disabled", balance.AccessLB)
		} else {
			loads := func() balancer.LoadSnapshot {
				out := make(balancer.LoadSnapshot)
				for id, l := range clusterMgr.LoadTable().Snapshot() {
					out[id] = balancer.NodeLoad{
						NodeID:         id,
						Connections:    l.Connections,
						MaxConnections: l.MaxConnections,
						UnixNano:       l.UnixNano,
					}
				}
				return out
			}
			local := func() balancer.NodeLoad {
				l := clusterMgr.LocalLoad()
				return balancer.NodeLoad{
					NodeID:         l.NodeId,
					Connections:    l.Connections,
					MaxConnections: l.MaxConnections,
					UnixNano:       l.UnixNano,
				}
			}
			ctrl := balancer.NewAdmissionController(balancer.Config{
				Mode:              balance.AccessLB,
				MQTTAdvertiseAddr: balance.MQTTAdvertiseAddr,
				SoftLimit:         balance.SoftLimit,
				Secret:            secret,
				Loads:             loads,
			})
			b.AdmissionFunc = func(clientID string, protoVer byte) (byte, string) {
				d := ctrl.OnConnect(clientID, protoVer, local())
				if d.Accept {
					return 0, ""
				}
				return d.ReasonCode, d.ServerRef
			}
			b.RedirectTokenFunc = func(clientID string, props map[string]string) bool {
				ts, err := strconv.ParseInt(props[balancer.RedirectTimeProperty], 10, 64)
				if err != nil {
					return false
				}
				return balancer.ValidateRedirectToken(secret, clientID,
					props[balancer.RedirectTokenProperty], ts, props[balancer.RedirectNodeProperty])
			}
			log.Printf("[INFO] Access-layer load balancing enabled: mode=%s mqtt_endpoint=%s soft_limit=%.2f",
				balance.AccessLB, balance.MQTTAdvertiseAddr, balance.SoftLimit)
		}
	}

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

	// Connector / rule / integration engines.
	//
	// These MUST be the same instances the broker uses on its message hot path.
	// Previously this entrypoint created fresh instances here and handed them to
	// the dashboard, while broker.New() created its own copies. As a result rules
	// and connectors configured from the dashboard were written to one set of
	// objects but the broker's PUBLISH path read from another, so configuration
	// silently had no effect. A2A Registry wiring depends on a single shared
	// instance as well.
	connectorManager := b.GetConnectorManager()
	ruleEngine := b.GetRuleEngine()
	integrationEngine := b.GetIntegrationEngine()

	// Admin API Server
	adminAPI := admin.NewAPIServer(metricsManager, b)
	// Wire the config hot-reload closure (also triggered by SIGHUP) into the
	// admin reload endpoint.
	adminAPI.SetReloadHandler(reload)

	// --- Configure the optional A2A over MQTT Registry ---
	// The Registry observes retained Agent Cards on $a2a/v1/discovery/#,
	// indexes them for the admin API, and annotates retained cards with the
	// current a2a-status at delivery time. It is completely off by default.
	a2aRegistry, err := configureA2ARegistry(b, adminAPI, cfg.Broker.A2ARegistry)
	if err != nil {
		log.Fatalf("Failed to configure A2A Registry: %v", err)
	}
	configureA2ARequestAuth(b, cfg.Broker.A2ARequestAuth)
	if a2aRegistry != nil {
		warmUpA2ARegistry(context.Background(), b, a2aRegistry)
	}

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
	configureA2AGateway(dashboardServer, cfg.Broker.A2AGateway, cfg.Broker.MQTTPort, a2aRegistry)

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

// configureA2ARegistry creates and wires the optional A2A Registry. A disabled
// configuration leaves the broker and admin API untouched. A2A settings are
// startup-only: changing them requires a restart, unlike auth/ACL/hooks.
func configureA2ARegistry(b *broker.Broker, api *admin.APIServer, cfg config.A2ARegistryConfig) (*registry.Registry, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	mode, err := schema.ParseMode(cfg.Validate)
	if err != nil {
		return nil, err
	}
	r := registry.New(registry.Config{
		Enabled:       true,
		Validate:      mode,
		MaxCardSize:   cfg.MaxCardSize,
		Registrations: cfg.RegistrationRate,
		BindingURL:    cfg.BindingURL,
		Security: &schema.SecurityConfig{
			RequireMetadata:    cfg.RequireSecurityMetadata,
			AllowedJKUPrefixes: cfg.AllowedJKUPrefixes,
		},
	})
	b.GetHooks().Register(r)
	b.RetainedAnnotationFunc = r.AnnotateRetained
	api.SetA2AProvider(admin.NewA2AProvider(r))
	bindingURL := effectiveBindingURL(cfg.BindingURL)
	log.Printf("[A2A] Registry enabled: validate=%s max_card_size=%d rate_limit=%d/min binding_url=%s",
		mode, cfg.MaxCardSize, cfg.RegistrationRate, bindingURL)
	if a2aMountpointRisk(bindingURL) {
		log.Printf("[A2A] WARNING: the configured binding URL has a multi-level path. A2A discovery" +
			" uses reserved $a2a/v1/... topics; a broker mountpoint must be exactly one topic level" +
			" (or absent), otherwise registration and discovery will not match.")
	}
	return r, nil
}

// a2aMountpointRisk reports whether bindingURL carries a topic-like path with
// more than one level. EMQX documents that A2A Registry is incompatible with
// mountpoints deeper than one level; GroveMQ has no native mountpoint setting,
// so this detects the equivalent risk on the advertised binding URL.
func a2aMountpointRisk(bindingURL string) bool {
	u, err := url.Parse(strings.TrimSpace(bindingURL))
	if err != nil {
		return false
	}
	path := strings.Trim(u.Path, "/")
	if path == "" {
		return false
	}
	return len(strings.Split(path, "/")) > 1
}

// configureA2AGateway creates the optional HTTP JSON-RPC to MQTT bridge and
// mounts it on the dashboard listener. It is disabled by default and reads a
// snapshot of the configuration at startup; changing it requires a restart.
// A failure to create an explicitly enabled gateway is fatal so operators do
// not silently lose the advertised HTTP endpoint.
func configureA2AGateway(dashboardServer *dashboard.Server, cfg config.A2AGatewayConfig, mqttPort string, a2aRegistry *registry.Registry) {
	if !cfg.Enabled {
		log.Printf("[A2A] HTTP JSON-RPC gateway is disabled (start with broker.a2a_gateway.enabled=true)")
		return
	}
	bridge, err := gateway.NewMQTTBridge(gateway.MQTTConfig{
		BrokerURL: effectiveGatewayBrokerURL(cfg.MQTTBrokerURL, mqttPort),
		ClientID:  "grovemq-gateway",
		Username:  cfg.Username,
		Password:  cfg.Password,
		Timeout:   time.Duration(cfg.TimeoutSeconds) * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to configure A2A gateway: %v", err)
	}
	// With the Registry enabled, unknown agent addresses fail fast with the
	// TaskNotFoundError semantic error instead of waiting for a bridge timeout.
	var lookup gateway.AgentLookup
	if a2aRegistry != nil {
		lookup = func(address string) bool {
			_, ok := a2aRegistry.Get(address)
			return ok
		}
	}
	auth := &gateway.AuthConfig{Tokens: cfg.Tokens}
	streamBridge, err := gateway.NewMQTTStreamBridge(gateway.MQTTConfig{
		BrokerURL: effectiveGatewayBrokerURL(cfg.MQTTBrokerURL, mqttPort),
		ClientID:  "grovemq-gateway-stream",
		Username:  cfg.Username,
		Password:  cfg.Password,
		Timeout:   time.Duration(cfg.TimeoutSeconds) * time.Second,
	}, "main")
	if err != nil {
		log.Fatalf("Failed to configure A2A streaming gateway: %v", err)
	}
	_ = streamBridge.Hub().StartJanitor(time.Minute)
	var handler http.Handler = gateway.NewStreamHandler(bridge, streamBridge, lookup, auth)
	if len(cfg.Tokens) == 0 {
		log.Printf("[A2A] WARNING: HTTP JSON-RPC gateway has no bearer tokens configured; " +
			"all callers are anonymous. Set broker.a2a_gateway.tokens or restrict network access.")
	} else {
		log.Printf("[A2A] HTTP JSON-RPC gateway bearer authentication enabled for %d token(s)", len(cfg.Tokens))
	}
	dashboardServer.SetA2AGateway(handler)
	log.Printf("[A2A] HTTP JSON-RPC gateway enabled at %s{org}/{unit}/{agent} via %s",
		gateway.PathPrefix, effectiveGatewayBrokerURL(cfg.MQTTBrokerURL, mqttPort))
}

// warmUpA2ARegistry rebuilds the Registry index from durable retained Agent
// Cards so admin/CLI discovery works immediately after a broker restart,
// before agents republish. Failures are logged but never fatal: live publishes
// will continue to populate the index through the hook.
func warmUpA2ARegistry(ctx context.Context, b *broker.Broker, reg *registry.Registry) {
	msgs, err := b.GetRetainedMessages(ctx, hooks.A2ADiscoveryPrefix+"#")
	if err != nil {
		log.Printf("[A2A] WARNING: failed to load retained cards for registry warm-up: %v", err)
		return
	}
	restored := 0
	for _, msg := range msgs {
		address := strings.TrimPrefix(msg.Topic, hooks.A2ADiscoveryPrefix)
		if reg.RestoreCard(address, msg.Payload) {
			restored++
		}
	}
	log.Printf("[A2A] registry warm-up restored %d agent card(s) from retained messages", restored)
}

// configureA2ARequestAuth installs the broker-side identity policy for A2A task
// requests. Policy "off" is a no-op so existing deployments are unchanged.
func configureA2ARequestAuth(b *broker.Broker, cfg config.A2ARequestAuthConfig) {
	policy := hooks.NormalizeA2ARequestAuthPolicy(cfg.Policy)
	if policy == hooks.A2ARequestAuthPolicyOff {
		return
	}
	b.GetHooks().Register(hooks.NewA2ARequestAuthHook(hooks.A2ARequestAuthConfig{
		Policy:           policy,
		AllowedClients:   cfg.AllowedClients,
		AllowedUsernames: cfg.AllowedUsernames,
	}))
	log.Printf("[A2A] task request identity policy enabled: %s (allowed_clients=%d allowed_usernames=%d)",
		policy, len(cfg.AllowedClients), len(cfg.AllowedUsernames))
}

// effectiveGatewayBrokerURL resolves an empty gateway URL to the local MQTT
// listener. The bridge accepts both tcp://host:port and host:port forms.
func effectiveGatewayBrokerURL(configured, mqttPort string) string {
	if strings.TrimSpace(configured) != "" {
		return configured
	}
	if strings.TrimSpace(mqttPort) == "" {
		return "127.0.0.1:1883"
	}
	if strings.Contains(mqttPort, "://") {
		return mqttPort
	}
	if strings.HasPrefix(mqttPort, ":") {
		return "127.0.0.1" + mqttPort
	}
	return mqttPort
}

// effectiveBindingURL mirrors registry.New's default so startup logs show the
// value agents will actually normalize into their Agent Cards.
func effectiveBindingURL(configured string) string {
	if strings.TrimSpace(configured) == "" {
		return registry.DefaultBindingURL
	}
	return configured
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
