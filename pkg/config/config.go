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

// Package config provides configuration management for grovemq,
// including user authentication configuration and other broker settings.
package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"

	"github.com/turtacn/grovemq/pkg/acl"
	"github.com/turtacn/grovemq/pkg/auth"
	"github.com/turtacn/grovemq/pkg/hooks"
	"gopkg.in/yaml.v2"
)

// UserConfig represents a user configuration entry
type UserConfig struct {
	Username  string `yaml:"username" json:"username"`
	Password  string `yaml:"password" json:"password"`
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	Enabled   bool   `yaml:"enabled" json:"enabled"`
}

// AuthConfig represents the authentication configuration
type AuthConfig struct {
	Enabled        bool         `yaml:"enabled" json:"enabled"`
	AllowAnonymous bool         `yaml:"allow_anonymous" json:"allow_anonymous"`
	Users          []UserConfig `yaml:"users" json:"users"`
}

// ACLRuleConfig represents a single ACL rule in configuration
type ACLRuleConfig struct {
	ID              string `yaml:"id" json:"id"`
	Permission      string `yaml:"permission" json:"permission"` // allow | deny
	Action          string `yaml:"action" json:"action"`         // publish | subscribe | pubsub | all
	Username        string `yaml:"username" json:"username"`     // exact; "" = any; "anonymous" = empty username
	UsernamePattern string `yaml:"username_pattern" json:"username_pattern,omitempty"`
	ClientID        string `yaml:"clientid" json:"clientid"` // exact; "" = any
	ClientIDPattern string `yaml:"clientid_pattern" json:"clientid_pattern,omitempty"`
	Topic           string `yaml:"topic" json:"topic"` // MQTT topic filter with +/#; "" = any
	Enabled         bool   `yaml:"enabled" json:"enabled"`
	Priority        int    `yaml:"priority" json:"priority"`
	Description     string `yaml:"description" json:"description,omitempty"`
}

// ACLConfig represents the authorization (ACL) configuration
type ACLConfig struct {
	Enabled       bool            `yaml:"enabled" json:"enabled"`
	DefaultAction string          `yaml:"default_action" json:"default_action"` // allow | deny
	Rules         []ACLRuleConfig `yaml:"rules" json:"rules"`
}

// RateLimitHookConfig configures the built-in rate-limit hook. When Enabled is
// false the hook stays inert (rate 0 = unlimited).
type RateLimitHookConfig struct {
	Enabled      bool    `yaml:"enabled" json:"enabled"`
	PublishRate  float64 `yaml:"publish_rate" json:"publish_rate"`   // tokens/sec; 0 = unlimited
	PublishBurst int     `yaml:"publish_burst" json:"publish_burst"` // bucket capacity
}

// AuditHookConfig configures the built-in audit hook. When Enabled is false no
// auditing occurs. If Points is empty, a sensible default set is enabled.
type AuditHookConfig struct {
	Enabled bool     `yaml:"enabled" json:"enabled"`
	Points  []string `yaml:"points" json:"points,omitempty"` // empty = default set
}

// HookConfig groups the built-in extension-point hook settings.
type HookConfig struct {
	RateLimit RateLimitHookConfig `yaml:"rate_limit" json:"rate_limit"`
	Audit     AuditHookConfig     `yaml:"audit" json:"audit"`
}

// A2ARegistryConfig configures the optional A2A over MQTT Agent Registry.
// When Enabled is false (the default) the Registry is not created and no A2A
// hooks are registered, so ordinary MQTT behaviour is unchanged.
type A2ARegistryConfig struct {
	Enabled          bool   `yaml:"enabled" json:"enabled"`
	Validate         string `yaml:"validate_schema" json:"validate_schema"` // strict | loose
	MaxCardSize      int    `yaml:"max_card_size" json:"max_card_size"`
	RegistrationRate int    `yaml:"registration_rate_limit" json:"registration_rate_limit"` // per agent per minute; 0 = unlimited
	BindingURL       string `yaml:"binding_url" json:"binding_url"`
}

// A2AGatewayConfig configures the optional HTTP JSON-RPC to MQTT gateway.
// It lets standard A2A SDKs call agents over /a2a/v1/... without changing
// their JSON-RPC binding code.
type A2AGatewayConfig struct {
	Enabled        bool   `yaml:"enabled" json:"enabled"`
	MQTTBrokerURL  string `yaml:"mqtt_broker_url" json:"mqtt_broker_url"` // empty = local broker
	Username       string `yaml:"username" json:"username"`               // MQTT client user for the bridge
	Password       string `yaml:"password" json:"password"`               // MQTT client password for the bridge
	TimeoutSeconds int    `yaml:"timeout_seconds" json:"timeout_seconds"` // 0 = 5s
}

// BalanceConfig groups the access-layer (and forwarding-layer) load-balancing
// settings introduced by the Mode B design (docs/design/LOAD_BALANCING.md §6.2).
// All fields are optional: an empty value means "use the built-in default", so
// older configuration files that lack the balance section keep working.
type BalanceConfig struct {
	AccessLB          string  `yaml:"access_lb" json:"access_lb"`
	MQTTAdvertiseAddr string  `yaml:"mqtt_advertise_addr" json:"mqtt_advertise_addr"`
	SoftLimit         float64 `yaml:"soft_limit" json:"soft_limit"`
	LBStrategy        string  `yaml:"lb_strategy" json:"lb_strategy"`
	SharedRoutePolicy string  `yaml:"shared_route_policy" json:"shared_route_policy"`
}

// BrokerConfig represents the overall broker configuration
type BrokerConfig struct {
	NodeID        string            `yaml:"node_id" json:"node_id"`
	MQTTPort      string            `yaml:"mqtt_port" json:"mqtt_port"`
	WebSocketPort string            `yaml:"websocket_port" json:"websocket_port"` // empty = WebSocket gateway disabled
	GRPCPort      string            `yaml:"grpc_port" json:"grpc_port"`
	MetricsPort   string            `yaml:"metrics_port" json:"metrics_port"`
	DashboardPort string            `yaml:"dashboard_port" json:"dashboard_port"`
	Auth          AuthConfig        `yaml:"auth" json:"auth"`
	ACL           ACLConfig         `yaml:"acl" json:"acl"`
	Hooks         HookConfig        `yaml:"hooks" json:"hooks"`
	Balance       BalanceConfig     `yaml:"balance" json:"balance"`
	A2ARegistry   A2ARegistryConfig `yaml:"a2a_registry" json:"a2a_registry"`
	A2AGateway    A2AGatewayConfig  `yaml:"a2a_gateway" json:"a2a_gateway"`

	// MailboxSize is the capacity of each subscriber's delivery mailbox. A
	// slow or stalled subscriber fills its mailbox; once full, best-effort
	// (QoS 0) messages are dropped rather than stalling the router. 0 = default 100.
	MailboxSize int `yaml:"mailbox_size" json:"mailbox_size"`
	// WriteTimeout bounds how long a single outbound write to a client may
	// block (seconds). A stalled connection then fails fast instead of hanging
	// the session actor. 0 = disabled.
	WriteTimeout int `yaml:"write_timeout" json:"write_timeout"`
	// MaxConnections is the hard cap on simultaneous live connections. 0 =
	// unlimited (the off-by-default behaviour, matching RateLimit's 0=unlimited).
	MaxConnections int `yaml:"max_connections" json:"max_connections"`
}

// Config holds the complete configuration
type Config struct {
	Broker BrokerConfig `yaml:"broker" json:"broker"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Broker: BrokerConfig{
			NodeID:        "grovemq-node",
			MQTTPort:      ":1883",
			WebSocketPort: "", // WebSocket gateway disabled by default
			GRPCPort:      ":8081",
			MetricsPort:   ":8082",
			DashboardPort: ":18083",
			Auth: AuthConfig{
				Enabled: true,
				// Keep the built-in default in sync with the shipped config.yaml:
				// the demo broker opts into anonymous access explicitly. Operators
				// can disable it by setting allow_anonymous: false in their config.
				AllowAnonymous: true,
				Users: []UserConfig{
					{
						Username:  "admin",
						Password:  "admin123",
						Algorithm: "bcrypt",
						Enabled:   true,
					},
					{
						Username:  "user1",
						Password:  "password123",
						Algorithm: "sha256",
						Enabled:   true,
					},
					{
						Username:  "test",
						Password:  "test",
						Algorithm: "plain",
						Enabled:   true,
					},
				},
			},
			ACL: ACLConfig{
				Enabled:       false,
				DefaultAction: "allow",
			},
			// Hooks are all OFF by default: rate_limit disabled, audit disabled.
			Hooks: HookConfig{
				RateLimit: RateLimitHookConfig{Enabled: false, PublishRate: 0, PublishBurst: 0},
				Audit:     AuditHookConfig{Enabled: false},
			},
			// A2A Registry is OFF by default so existing deployments are unchanged.
			A2ARegistry: A2ARegistryConfig{
				Enabled:          false,
				Validate:         "strict",
				MaxCardSize:      0, // 0 => package default (65536)
				RegistrationRate: 0, // 0 => package default (10/min)
				BindingURL:       "",
			},
			// A2A JSON-RPC gateway is OFF by default.
			A2AGateway: A2AGatewayConfig{
				Enabled:        false,
				MQTTBrokerURL:  "",
				Username:       "",
				Password:       "",
				TimeoutSeconds: 5,
			},
			// Load balancing defaults are the safe baseline: no new behaviour
			// during rolling upgrades (design §3 G5).
			Balance: BalanceConfig{
				AccessLB:          "off",
				MQTTAdvertiseAddr: "",
				SoftLimit:         0.8,
				LBStrategy:        "round_robin",
				SharedRoutePolicy: "fanout",
			},
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(configPath string) (*Config, error) {
	// If no config file specified, return default config
	if configPath == "" {
		log.Println("[INFO] No config file specified, using default configuration")
		return DefaultConfig(), nil
	}

	// Read config file
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	config := &Config{}
	ext := strings.ToLower(filepath.Ext(configPath))

	switch ext {
	case ".yaml", ".yml":
		err = yaml.Unmarshal(data, config)
	case ".json":
		err = json.Unmarshal(data, config)
	default:
		return nil, fmt.Errorf("unsupported config file format: %s (supported: .yaml, .yml, .json)", ext)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	log.Printf("[INFO] Configuration loaded from %s", configPath)
	return config, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(config *Config, configPath string) error {
	var data []byte
	var err error

	ext := strings.ToLower(filepath.Ext(configPath))
	switch ext {
	case ".yaml", ".yml":
		data, err = yaml.Marshal(config)
	case ".json":
		data, err = json.MarshalIndent(config, "", "  ")
	default:
		return fmt.Errorf("unsupported config file format: %s (supported: .yaml, .yml, .json)", ext)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	err = ioutil.WriteFile(configPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write config file %s: %w", configPath, err)
	}

	log.Printf("[INFO] Configuration saved to %s", configPath)
	return nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if config.Broker.NodeID == "" {
		return fmt.Errorf("node_id cannot be empty")
	}

	if config.Broker.MQTTPort == "" {
		return fmt.Errorf("mqtt_port cannot be empty")
	}

	// Validate users
	usernames := make(map[string]bool)
	for i, user := range config.Broker.Auth.Users {
		if user.Username == "" {
			return fmt.Errorf("user %d: username cannot be empty", i)
		}

		if usernames[user.Username] {
			return fmt.Errorf("duplicate username: %s", user.Username)
		}
		usernames[user.Username] = true

		if user.Password == "" {
			return fmt.Errorf("user %s: password cannot be empty", user.Username)
		}

		// Validate algorithm
		switch user.Algorithm {
		case "plain", "sha256", "bcrypt":
			// Valid algorithms
		default:
			return fmt.Errorf("user %s: unsupported algorithm: %s (supported: plain, sha256, bcrypt)", user.Username, user.Algorithm)
		}
	}

	// Validate ACL configuration
	switch config.Broker.ACL.DefaultAction {
	case "", "allow", "deny":
		// Valid; empty defaults to allow at runtime.
	default:
		return fmt.Errorf("acl.default_action must be \"allow\" or \"deny\", got %q", config.Broker.ACL.DefaultAction)
	}
	for i, rule := range config.Broker.ACL.Rules {
		switch rule.Permission {
		case "", "allow", "deny":
		default:
			return fmt.Errorf("acl rule %d: invalid permission %q", i, rule.Permission)
		}
		switch rule.Action {
		case "", "publish", "subscribe", "pubsub", "all":
		default:
			return fmt.Errorf("acl rule %d: invalid action %q", i, rule.Action)
		}
	}

	// Validate A2A Registry configuration (off by default).
	a2a := config.Broker.A2ARegistry
	switch strings.ToLower(strings.TrimSpace(a2a.Validate)) {
	case "", "strict", "loose":
	default:
		return fmt.Errorf("a2a_registry.validate_schema must be strict or loose, got %q", a2a.Validate)
	}
	if a2a.MaxCardSize < 0 {
		return fmt.Errorf("a2a_registry.max_card_size must be >= 0, got %d", a2a.MaxCardSize)
	}
	if a2a.RegistrationRate < 0 {
		return fmt.Errorf("a2a_registry.registration_rate_limit must be >= 0, got %d", a2a.RegistrationRate)
	}
	if config.Broker.A2AGateway.TimeoutSeconds < 0 {
		return fmt.Errorf("a2a_gateway.timeout_seconds must be >= 0, got %d", config.Broker.A2AGateway.TimeoutSeconds)
	}

	// Validate hooks configuration.
	if config.Broker.Hooks.RateLimit.PublishRate < 0 {
		return fmt.Errorf("hooks.rate_limit.publish_rate must be >= 0, got %v", config.Broker.Hooks.RateLimit.PublishRate)
	}
	if config.Broker.Hooks.RateLimit.PublishBurst < 0 {
		return fmt.Errorf("hooks.rate_limit.publish_burst must be >= 0, got %v", config.Broker.Hooks.RateLimit.PublishBurst)
	}

	// max_connections: 0 means unlimited; a negative value is invalid.
	if config.Broker.MaxConnections < 0 {
		return fmt.Errorf("broker.max_connections must be >= 0, got %v", config.Broker.MaxConnections)
	}

	// Load balancing (docs/design/LOAD_BALANCING.md §6.2). Empty strings and a
	// zero soft_limit mean "unset", so older config files keep working.
	b := config.Broker.Balance
	switch b.AccessLB {
	case "", "off", "l4", "redirect", "both":
	default:
		return fmt.Errorf("broker.balance.access_lb must be one of off|l4|redirect|both, got %q", b.AccessLB)
	}
	if b.SoftLimit < 0 || b.SoftLimit > 1 {
		return fmt.Errorf("broker.balance.soft_limit must be in (0,1], got %v", b.SoftLimit)
	}
	switch b.LBStrategy {
	case "", "round_robin", "least_connections", "consistent_hash":
	default:
		return fmt.Errorf("broker.balance.lb_strategy must be one of round_robin|least_connections|consistent_hash, got %q", b.LBStrategy)
	}
	switch b.SharedRoutePolicy {
	case "", "fanout", "single":
	default:
		return fmt.Errorf("broker.balance.shared_route_policy must be fanout or single, got %q", b.SharedRoutePolicy)
	}

	return nil
}

// ConfigureAuth configures authentication from the config
func (c *Config) ConfigureAuth(authChain *auth.AuthChain) error {
	// Anonymous access is an explicit opt-in, not an accidental side effect of
	// enabling authentication. Apply it on every (re)configure so a hot reload
	// cannot silently re-open an anonymous back door.
	authChain.SetAllowAnonymous(c.Broker.Auth.AllowAnonymous)

	if !c.Broker.Auth.Enabled {
		// Swap in an empty, disabled chain atomically.
		authChain.Replace(nil, false)
		log.Println("[INFO] Authentication disabled by configuration")
		return nil
	}

	// Create memory authenticator
	memAuth := auth.NewMemoryAuthenticator()

	// Add users from configuration
	for _, userConfig := range c.Broker.Auth.Users {
		algorithm := auth.HashAlgorithm(userConfig.Algorithm)
		err := memAuth.AddUser(userConfig.Username, userConfig.Password, algorithm)
		if err != nil {
			return fmt.Errorf("failed to add user %s: %w", userConfig.Username, err)
		}

		// Set user enabled status
		err = memAuth.SetUserEnabled(userConfig.Username, userConfig.Enabled)
		if err != nil {
			return fmt.Errorf("failed to set user %s enabled status: %w", userConfig.Username, err)
		}

		log.Printf("[INFO] Configured user: %s (algorithm: %s, enabled: %t)",
			userConfig.Username, userConfig.Algorithm, userConfig.Enabled)
	}

	// Atomically swap in the rebuilt chain (enabled). This avoids exposing a
	// half-built chain during a runtime reload.
	authChain.Replace([]auth.Authenticator{memAuth}, true)
	log.Printf("[INFO] Authentication configured with %d users", len(c.Broker.Auth.Users))

	return nil
}

// ConfigureACL configures the ACL engine from the config: default action and
// rules are seeded into the engine's manager.
func (c *Config) ConfigureACL(engine *acl.Engine) error {
	if engine == nil {
		return fmt.Errorf("nil ACL engine")
	}

	cfg := &acl.ACLConfig{
		Enabled:       c.Broker.ACL.Enabled,
		DefaultAction: c.Broker.ACL.DefaultAction,
		LogDenials:    true,
	}
	if cfg.DefaultAction == "" {
		cfg.DefaultAction = acl.DefaultActionAllow
	}
	engine.UpdateConfig(cfg)

	manager := engine.GetManager()
	manager.Clear()
	for _, rc := range c.Broker.ACL.Rules {
		rule := &acl.Rule{
			ID:              rc.ID,
			Permission:      rc.Permission,
			Action:          rc.Action,
			Username:        rc.Username,
			UsernamePattern: rc.UsernamePattern,
			ClientID:        rc.ClientID,
			ClientIDPattern: rc.ClientIDPattern,
			Topic:           rc.Topic,
			Enabled:         rc.Enabled,
			Priority:        rc.Priority,
			Description:     rc.Description,
		}
		if rule.Permission == "" {
			rule.Permission = acl.PermissionAllow
		}
		if rule.Action == "" {
			rule.Action = acl.ActionAll
		}
		if err := manager.AddRule(rule); err != nil {
			return fmt.Errorf("failed to add ACL rule %q: %w", rc.ID, err)
		}
	}

	log.Printf("[INFO] ACL configured: enabled=%t, default=%s, rules=%d", cfg.Enabled, cfg.DefaultAction, len(c.Broker.ACL.Rules))
	return nil
}

// ConfigureHooks applies the built-in hook settings to a hooks.Manager that was
// already populated (by broker.New) with the default AuditHook and
// RateLimitHook. It is a no-op for hooks that are disabled, so configurations
// that leave hooks off incur zero behavioural change. The ForwardHook is not
// configurable via static config (it requires a publish callback); it can be
// configured programmatically via the manager.
func (c *Config) ConfigureHooks(mgr *hooks.Manager) error {
	if mgr == nil {
		return fmt.Errorf("nil hooks manager")
	}

	// Rate limit hook.
	if c.Broker.Hooks.RateLimit.Enabled {
		if h, ok := mgr.Get("rate_limit"); ok {
			if rl, ok := h.(*hooks.RateLimitHook); ok {
				rl.SetDefaultRate(c.Broker.Hooks.RateLimit.PublishRate, c.Broker.Hooks.RateLimit.PublishBurst)
				log.Printf("[INFO] Hooks: rate_limit enabled (rate=%.2f tokens/s, burst=%d)",
					c.Broker.Hooks.RateLimit.PublishRate, c.Broker.Hooks.RateLimit.PublishBurst)
			}
		}
	}

	// Audit hook.
	if c.Broker.Hooks.Audit.Enabled {
		if h, ok := mgr.Get("audit"); ok {
			if a, ok := h.(*hooks.AuditHook); ok {
				points := c.Broker.Hooks.Audit.Points
				if len(points) == 0 {
					points = []string{
						hooks.OnClientConnectedPoint,
						hooks.OnClientDisconnectedPoint,
						hooks.OnSessionSubscribedPoint,
						hooks.OnSessionUnsubscribedPoint,
						hooks.OnMessagePublishPoint,
					}
				}
				for _, p := range points {
					a.Enable(p)
				}
				log.Printf("[INFO] Hooks: audit enabled for %d point(s)", len(points))
			}
		}
	}

	return nil
}

// AddUser adds a new user to the configuration
func (c *Config) AddUser(username, password, algorithm string, enabled bool) error {
	// Check if user already exists
	for _, user := range c.Broker.Auth.Users {
		if user.Username == username {
			return fmt.Errorf("user %s already exists", username)
		}
	}

	// Validate algorithm
	switch algorithm {
	case "plain", "sha256", "bcrypt":
		// Valid algorithms
	default:
		return fmt.Errorf("unsupported algorithm: %s (supported: plain, sha256, bcrypt)", algorithm)
	}

	// Add user
	newUser := UserConfig{
		Username:  username,
		Password:  password,
		Algorithm: algorithm,
		Enabled:   enabled,
	}

	c.Broker.Auth.Users = append(c.Broker.Auth.Users, newUser)
	log.Printf("[INFO] Added user to configuration: %s", username)
	return nil
}

// UpdateUser updates an existing user in the configuration
func (c *Config) UpdateUser(username, password, algorithm string, enabled bool) error {
	for i, user := range c.Broker.Auth.Users {
		if user.Username == username {
			// Validate algorithm if provided
			if algorithm != "" {
				switch algorithm {
				case "plain", "sha256", "bcrypt":
					c.Broker.Auth.Users[i].Algorithm = algorithm
				default:
					return fmt.Errorf("unsupported algorithm: %s (supported: plain, sha256, bcrypt)", algorithm)
				}
			}

			// Update password if provided
			if password != "" {
				c.Broker.Auth.Users[i].Password = password
			}

			c.Broker.Auth.Users[i].Enabled = enabled
			log.Printf("[INFO] Updated user in configuration: %s", username)
			return nil
		}
	}

	return fmt.Errorf("user %s not found", username)
}

// RemoveUser removes a user from the configuration
func (c *Config) RemoveUser(username string) error {
	for i, user := range c.Broker.Auth.Users {
		if user.Username == username {
			c.Broker.Auth.Users = append(c.Broker.Auth.Users[:i], c.Broker.Auth.Users[i+1:]...)
			log.Printf("[INFO] Removed user from configuration: %s", username)
			return nil
		}
	}

	return fmt.Errorf("user %s not found", username)
}

// ListUsers returns all users in the configuration
func (c *Config) ListUsers() []UserConfig {
	return c.Broker.Auth.Users
}
