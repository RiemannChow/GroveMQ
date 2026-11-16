package balancer

import (
	"sync"
	"testing"
	"time"
)

func freshSnapshot(now time.Time) LoadSnapshot {
	n := now.UnixNano()
	return LoadSnapshot{
		"n1": {NodeID: "n1", Connections: 95, MaxConnections: 100, UnixNano: n},
		"n2": {NodeID: "n2", Connections: 10, MaxConnections: 100, UnixNano: n},
		"n3": {NodeID: "n3", Connections: 40, MaxConnections: 100, UnixNano: n},
	}
}

func testConfig(mode string, now time.Time) Config {
	return Config{
		Mode:              mode,
		MQTTAdvertiseAddr: "grovemq-mqtt:1883",
		SoftLimit:         0.8,
		Secret:            []byte("test-secret"),
		Cooldown:          30 * time.Second,
		Loads:             func() LoadSnapshot { return freshSnapshot(now) },
		Now:               func() time.Time { return now },
	}
}

func TestOffAndL4NeverRedirect(t *testing.T) {
	now := time.Now()
	for _, mode := range []string{"off", "l4"} {
		c := NewAdmissionController(testConfig(mode, now))
		d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"})
		if !d.Accept || d.ReasonCode != 0 || d.ServerRef != "" {
			t.Fatalf("mode %s: %+v", mode, d)
		}
	}
}

func TestMQTT311NeverRedirect(t *testing.T) {
	now := time.Now()
	c := NewAdmissionController(testConfig("both", now))
	d := c.OnConnect("client-1", 4, NodeLoad{NodeID: "n1"})
	if !d.Accept {
		t.Fatalf("v3.1.1 client must be accepted locally: %+v", d)
	}
}

func TestBelowSoftLimitAccepted(t *testing.T) {
	now := time.Now()
	cfg := testConfig("redirect", now)
	cfg.Loads = func() LoadSnapshot {
		s := freshSnapshot(now)
		s["n1"] = NodeLoad{NodeID: "n1", Connections: 20, MaxConnections: 100, UnixNano: now.UnixNano()}
		return s
	}
	c := NewAdmissionController(cfg)
	if d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"}); !d.Accept {
		t.Fatalf("below soft limit must be accepted: %+v", d)
	}
}

func TestOverSoftLimitRedirects(t *testing.T) {
	now := time.Now()
	c := NewAdmissionController(testConfig("both", now))
	d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"})
	if d.Accept || d.ReasonCode != 0x9C || d.ServerRef == "" {
		t.Fatalf("expected 0x9C redirect, got %+v", d)
	}
	endpoint, token, nodeID, ts, ok := ParseServerRef(d.ServerRef)
	if !ok || endpoint != "grovemq-mqtt:1883" || nodeID != "n2" || ts != now.Unix() {
		t.Fatalf("bad ServerRef %q -> (%q,%q,%q,%d,%v)", d.ServerRef, endpoint, token, nodeID, ts, ok)
	}
	if !ValidateRedirectToken([]byte("test-secret"), "client-1", token, ts, nodeID) {
		t.Fatal("generated token failed validation")
	}
}

func TestStaleLoadDisablesRedirect(t *testing.T) {
	now := time.Now()
	cfg := testConfig("redirect", now)
	cfg.Loads = func() LoadSnapshot {
		s := freshSnapshot(now)
		s["n2"] = NodeLoad{NodeID: "n2", Connections: 1, MaxConnections: 100, UnixNano: now.Add(-time.Minute).UnixNano()}
		return s
	}
	c := NewAdmissionController(cfg)
	if d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"}); !d.Accept {
		t.Fatalf("stale load must disable redirect: %+v", d)
	}
}

func TestMissingLoadDataAccepted(t *testing.T) {
	now := time.Now()
	cfg := testConfig("redirect", now)
	cfg.Loads = func() LoadSnapshot { return nil }
	c := NewAdmissionController(cfg)
	if d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"}); !d.Accept {
		t.Fatalf("no load data must accept locally: %+v", d)
	}
}

func TestNoBetterNodeAccepted(t *testing.T) {
	now := time.Now()
	cfg := testConfig("redirect", now)
	cfg.Loads = func() LoadSnapshot {
		return LoadSnapshot{
			"n1": {NodeID: "n1", Connections: 95, MaxConnections: 100, UnixNano: now.UnixNano()},
			"n2": {NodeID: "n2", Connections: 99, MaxConnections: 100, UnixNano: now.UnixNano()},
		}
	}
	c := NewAdmissionController(cfg)
	if d := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"}); !d.Accept {
		t.Fatalf("no better node must accept locally: %+v", d)
	}
}

func TestCooldownAllowsOnlyOneRedirect(t *testing.T) {
	now := time.Now()
	c := NewAdmissionController(testConfig("redirect", now))
	first := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"})
	if first.Accept {
		t.Fatal("first call should redirect")
	}
	second := c.OnConnect("client-1", 5, NodeLoad{NodeID: "n1"})
	if !second.Accept {
		t.Fatalf("second call within cooldown must be accepted: %+v", second)
	}
}

func TestValidateRedirectTokenRejectsTamperingAndReplay(t *testing.T) {
	secret := []byte("s3cr3t")
	ts := time.Now().Unix()
	token := signToken(secret, "n2", ts, "client-1")
	if !ValidateRedirectToken(secret, "client-1", token, ts, "n2") {
		t.Fatal("valid token rejected")
	}
	if ValidateRedirectToken(secret, "other-client", token, ts, "n2") {
		t.Fatal("token accepted for a different client")
	}
	if ValidateRedirectToken(secret, "client-1", token, ts, "n3") {
		t.Fatal("token accepted for a different node")
	}
	if ValidateRedirectToken(secret, "client-1", token+"0", ts, "n2") {
		t.Fatal("tampered token accepted")
	}
	old := time.Now().Add(-time.Hour).Unix()
	if ValidateRedirectToken(secret, "client-1", signToken(secret, "n2", old, "client-1"), old, "n2") {
		t.Fatal("expired token accepted")
	}
}

func TestConcurrentOnConnect(t *testing.T) {
	now := time.Now()
	c := NewAdmissionController(testConfig("both", now))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = c.OnConnect("client", 5, NodeLoad{NodeID: "n1"})
			}
		}()
	}
	wg.Wait()
}
