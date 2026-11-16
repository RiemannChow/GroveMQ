// Package balancer implements the access-layer admission controller for
// GroveMQ (docs/design/LOAD_BALANCING.md §6.2, Mode B).
//
// A hot node can answer an MQTT 5.0 CONNECT with CONNACK 0x9C (Use another
// server) plus a Server Reference, handing the reconnect decision back to the
// client. MQTT 3.1.1 has no such reason code, so it is never redirected.
package balancer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultSecret is used only when no secret is configured. Deployments MUST
// set Config.Secret so every node computes the same redirect token.
const defaultSecret = "grovemq-redirect-default-secret"

// loadStaleAfter mirrors cluster.loadStaleAfter: a snapshot older than this is
// unusable for a redirect decision (design §5.3 / §6.2).
const loadStaleAfter = 10 * time.Second

// tokenValidity bounds replay of a redirect token.
const tokenValidity = 5 * time.Minute

// NodeLoad is a point-in-time load snapshot of one node. It deliberately does
// not import pkg/cluster so this package stays usable on its own.
type NodeLoad struct {
	NodeID         string
	Connections    int64
	MaxConnections int64
	UnixNano       int64
}

// LoadSnapshot maps node ID to that node's most recent load snapshot.
type LoadSnapshot map[string]NodeLoad

// Decision is the admission verdict for one CONNECT.
type Decision struct {
	Accept     bool
	ReasonCode byte   // MQTT 5.0: 0x9C Use another server
	ServerRef  string // client-reachable MQTT endpoint, with a one-hop token
}

// AdmissionController decides whether a CONNECT is accepted locally.
type AdmissionController interface {
	OnConnect(clientID string, protoVer byte, local NodeLoad) Decision
}

// Config wires the admission controller. Loads and Now are injectable for
// deterministic tests.
type Config struct {
	Mode              string // off | l4 | redirect | both
	MQTTAdvertiseAddr string // client-reachable MQTT endpoint (NOT the gRPC NODE_ADVERTISE_ADDR)
	SoftLimit         float64
	Secret            []byte
	Cooldown          time.Duration
	Loads             func() LoadSnapshot
	Now               func() time.Time
}

type controller struct {
	cfg  Config
	mu   sync.Mutex
	last map[string]time.Time // clientID -> last time we redirected it (loop guard)
}

// NewAdmissionController builds a thread-safe admission controller.
func NewAdmissionController(cfg Config) AdmissionController {
	if cfg.SoftLimit <= 0 || cfg.SoftLimit > 1 {
		cfg.SoftLimit = 0.8
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if len(cfg.Secret) == 0 {
		cfg.Secret = []byte(defaultSecret)
	}
	return &controller{cfg: cfg, last: make(map[string]time.Time)}
}

// OnConnect implements AdmissionController. All early returns are "accept
// locally": the controller never destabilises a connection it cannot reason
// about (no load data, stale data, MQTT 3.1.1, cooldown, or no better node).
func (c *controller) OnConnect(clientID string, protoVer byte, local NodeLoad) Decision {
	accept := Decision{Accept: true}

	// Mode A (L4) and "off" never use a CONNACK redirect.
	if c.cfg.Mode != "redirect" && c.cfg.Mode != "both" {
		return accept
	}
	// MQTT 3.1.1 has no 0x9C; connackReturnCode would fold it into 0x05.
	if protoVer < 5 {
		return accept
	}
	if c.cfg.MQTTAdvertiseAddr == "" || c.cfg.Loads == nil {
		return accept
	}

	now := c.cfg.Now()

	// One hop + cooldown: a client that keeps coming back here within the
	// window is accepted locally instead of bouncing between nodes.
	c.mu.Lock()
	if t, ok := c.last[clientID]; ok && now.Sub(t) < c.cfg.Cooldown {
		c.mu.Unlock()
		return accept
	}
	c.mu.Unlock()

	snap := c.cfg.Loads()
	if len(snap) == 0 {
		return accept
	}
	// Stale/incomplete load data: prefer a local hot spot over redirecting to
	// a node that may already be offline (design §6.4).
	for _, l := range snap {
		if l.UnixNano <= 0 || now.Sub(time.Unix(0, l.UnixNano)) > loadStaleAfter {
			return accept
		}
	}

	self, ok := snap[local.NodeID]
	if !ok {
		self = local
	}
	if utilisation(self) <= c.cfg.SoftLimit {
		return accept
	}

	// Deterministic least-loaded choice; sorted IDs break ties by node ID.
	ids := make([]string, 0, len(snap))
	for id := range snap {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	best, bestUtil := "", math.Inf(1)
	for _, id := range ids {
		if u := utilisation(snap[id]); best == "" || u < bestUtil {
			best, bestUtil = id, u
		}
	}
	if best == "" || best == local.NodeID || bestUtil >= utilisation(self) {
		return accept
	}

	ts := now.Unix()
	token := signToken(c.cfg.Secret, best, ts, clientID)
	ref := fmt.Sprintf("%s?redirect=%s&ts=%d&node=%s",
		strings.TrimSuffix(c.cfg.MQTTAdvertiseAddr, "/"), token, ts, url.QueryEscape(best))

	c.mu.Lock()
	c.last[clientID] = now
	c.mu.Unlock()

	return Decision{Accept: false, ReasonCode: 0x9C, ServerRef: ref}
}

func utilisation(l NodeLoad) float64 {
	if l.MaxConnections <= 0 {
		return 0
	}
	return float64(l.Connections) / float64(l.MaxConnections)
}

func signToken(secret []byte, nodeID string, ts int64, clientID string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = fmt.Fprintf(mac, "%s|%d|%s", nodeID, ts, clientID)
	return hex.EncodeToString(mac.Sum(nil))
}

// ValidateRedirectToken verifies a one-hop token from a Server Reference.
// The target node calls this (directly, or via a CONNECT user property) and
// must force Accept=true when it succeeds, preventing redirect loops.
func ValidateRedirectToken(secret []byte, clientID, token string, ts int64, nodeID string) bool {
	if len(secret) == 0 {
		secret = []byte(defaultSecret)
	}
	if token == "" || nodeID == "" || ts <= 0 || clientID == "" {
		return false
	}
	if delta := time.Now().Unix() - ts; delta > int64(tokenValidity.Seconds()) || delta < -int64(tokenValidity.Seconds()) {
		return false
	}
	expected := signToken(secret, nodeID, ts, clientID)
	return hmac.Equal([]byte(expected), []byte(token))
}

// ParseServerRef extracts the endpoint and one-hop fields from a Server
// Reference produced by OnConnect. It is provided for the broker-side token
// check on reconnect.
func ParseServerRef(ref string) (endpoint, token, nodeID string, ts int64, ok bool) {
	u, err := url.Parse(ref)
	if err != nil {
		return "", "", "", 0, false
	}
	q := u.Query()
	token = q.Get("redirect")
	nodeID = q.Get("node")
	ts, _ = strconv.ParseInt(q.Get("ts"), 10, 64)
	if token == "" || nodeID == "" || ts <= 0 {
		return "", "", "", 0, false
	}
	q.Del("redirect")
	q.Del("node")
	q.Del("ts")
	u.RawQuery = q.Encode()
	return u.String(), token, nodeID, ts, true
}
