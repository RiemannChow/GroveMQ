// Command smoke is a self-contained end-to-end smoke test for the grovemq broker.
// It exercises: MQTT auth (valid + invalid), QoS0/QoS1 pub/sub round trips,
// MQTT-over-WebSocket, ACL enforcement (subscribe-deny + publish-deny), and the
// dashboard/admin HTTP APIs. It exits non-zero on any failure.
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

const (
	tcpBroker   = "tcp://localhost:1883"
	wsBroker    = "ws://localhost:8083/mqtt"
	adminBase   = "http://localhost:18083"
	metricsBase = "http://localhost:8082"
)

var failures int

func check(name string, ok bool, detail string) {
	if ok {
		fmt.Printf("PASS  %s\n", name)
	} else {
		failures++
		fmt.Printf("FAIL  %s :: %s\n", name, detail)
	}
}

func newClient(broker, id, user, pass string, useWS bool) (paho.Client, error) {
	opts := paho.NewClientOptions().
		SetClientID(id).
		SetUsername(user).
		SetPassword(pass).
		SetProtocolVersion(5).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(false).
		SetCleanSession(true)
	opts.AddBroker(broker)
	if useWS {
		opts.SetWebsocketOptions(&paho.WebsocketOptions{})
	}
	c := paho.NewClient(opts)
	t := c.Connect()
	t.WaitTimeout(5 * time.Second)
	return c, t.Error()
}

func main() {
	// --- 1. Auth: valid credentials connect ---
	c, err := newClient(tcpBroker, "smoke-valid", "user1", "password123", false)
	check("auth: valid creds connect", err == nil && c.IsConnected(), fmt.Sprintf("err=%v connected=%v", err, c.IsConnected()))
	if err != nil || !c.IsConnected() {
		fmt.Println("cannot continue without a connected client")
		os.Exit(1)
	}
	defer c.Disconnect(100)

	// --- 2. Auth: invalid credentials rejected (broker sends CONNACK 0x84) ---
	bad, errBad := newClient(tcpBroker, "smoke-bad", "user1", "wrongpassword", false)
	rejected := errBad != nil || (bad != nil && !bad.IsConnected())
	check("auth: invalid creds rejected", rejected, fmt.Sprintf("err=%v connected=%v", errBad, bad != nil && bad.IsConnected()))

	// --- 3. QoS0 pub/sub round trip ---
	var mu sync.Mutex
	got := map[string][]byte{}
	c.Subscribe("smoke/q0", 0, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		got[m.Topic()] = m.Payload()
		mu.Unlock()
	})
	time.Sleep(200 * time.Millisecond)
	tok := c.Publish("smoke/q0", 0, false, []byte("hello-q0"))
	tok.WaitTimeout(3 * time.Second)
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	okQ0 := bytes.Equal(got["smoke/q0"], []byte("hello-q0"))
	mu.Unlock()
	check("pubsub: QoS0 round trip", okQ0 && tok.Error() == nil, "payload not received")

	// --- 4. QoS1 pub/sub round trip ---
	c.Subscribe("smoke/q1", 1, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		got[m.Topic()] = m.Payload()
		mu.Unlock()
	})
	time.Sleep(200 * time.Millisecond)
	tok = c.Publish("smoke/q1", 1, false, []byte("hello-q1"))
	okQ1 := tok.WaitTimeout(3*time.Second) && tok.Error() == nil
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	okQ1 = okQ1 && bytes.Equal(got["smoke/q1"], []byte("hello-q1"))
	mu.Unlock()
	check("pubsub: QoS1 round trip", okQ1, "QoS1 payload not received or publish errored")

	// --- 5. ACL: subscribe to denied topic rejected (SUBACK reason >= 0x80) ---
	// The client runs MQTT 3.1.1 (paho v1.5.1 speaks only 3.1.1). A correct
	// broker must send the 3.1.1 failure code 0x80, NOT the MQTT 5.0 code 0x87,
	// which is invalid for 3.1.1. We assert the exact code to catch that gap.
	subTok := c.Subscribe("nosec/#", 0, func(_ paho.Client, _ paho.Message) {})
	subTok.WaitTimeout(3 * time.Second)
	deniedSub := false
	var subReason byte
	if st, ok := subTok.(*paho.SubscribeToken); ok {
		for _, code := range st.Result() {
			if code >= 0x80 { // 0x80 = failure (3.1.1); 0x87 = Not authorized (5.0)
				deniedSub = true
				subReason = code
			}
		}
	}
	check("acl: subscribe to nosec/# denied (3.1.1 code 0x80)", deniedSub && subReason == 0x80,
		fmt.Sprintf("got reason=0x%02x (want 0x80 for 3.1.1)", subReason))

	// --- 6. ACL: publish to denied topic dropped; ALLOWED topic still delivered ---
	// Subscriber allows the topic filters (subscribe is permitted for deny/pub/#).
	c.Subscribe("deny/pub/#", 0, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		got[m.Topic()] = m.Payload()
		mu.Unlock()
	})
	c.Subscribe("allow/pub/#", 0, func(_ paho.Client, m paho.Message) {
		mu.Lock()
		got[m.Topic()] = m.Payload()
		mu.Unlock()
	})
	time.Sleep(300 * time.Millisecond)

	// Publish to DENIED topic (should be silently dropped for QoS0).
	c.Publish("deny/pub/x", 0, false, []byte("should-be-dropped")).WaitTimeout(3 * time.Second)
	// Publish to ALLOWED topic (should be delivered).
	c.Publish("allow/pub/y", 0, false, []byte("delivered")).WaitTimeout(3 * time.Second)
	time.Sleep(700 * time.Millisecond)

	mu.Lock()
	dropOK := got["deny/pub/x"] == nil
	allowOK := bytes.Equal(got["allow/pub/y"], []byte("delivered"))
	mu.Unlock()
	check("acl: publish to deny/pub/# dropped", dropOK, "message on denied topic was delivered")
	check("acl: publish to allowed topic still delivered", allowOK, "allowed message not delivered (session not killed by prior deny)")

	// --- 7. MQTT-over-WebSocket connect + pub/sub ---
	wc, err := newClient(wsBroker, "smoke-ws", "test", "test", true)
	check("websocket: connect", err == nil && wc.IsConnected(), fmt.Sprintf("err=%v connected=%v", err, wc != nil && wc.IsConnected()))
	if err == nil && wc.IsConnected() {
		wsDone := make(chan struct{})
		wsGot := false
		wc.Subscribe("smoke/ws", 0, func(_ paho.Client, m paho.Message) {
			if bytes.Equal(m.Payload(), []byte("ws-hello")) {
				wsGot = true
			}
			select {
			case <-wsDone:
			default:
				close(wsDone)
			}
		})
		time.Sleep(200 * time.Millisecond)
		wc.Publish("smoke/ws", 0, false, []byte("ws-hello")).WaitTimeout(3 * time.Second)
		select {
		case <-wsDone:
		case <-time.After(3 * time.Second):
		}
		check("websocket: pub/sub round trip", wsGot, "ws message not received")
		wc.Disconnect(100)
	}

	// --- 8. Dashboard / Admin HTTP APIs (authenticate via /login form) ---
	jar, _ := cookiejar.New(nil)
	hc := &http.Client{Jar: jar, Timeout: 5 * time.Second}
	// Login sets the dashboard_session cookie on success.
	resp, lerr := hc.PostForm(adminBase+"/login", url.Values{"username": {"admin"}, "password": {"public"}})
	loginOK := false
	if lerr == nil {
		resp.Body.Close()
		loginOK = resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusFound
	}
	check("dashboard: login with admin/public", loginOK, fmt.Sprintf("login err=%v", lerr))

	httpCheckJar(hc, "dashboard: GET / (login page)", adminBase+"/", 200)
	httpCheckJar(hc, "dashboard: GET /api/acl (authed)", adminBase+"/api/acl", 200)
	httpCheckJar(hc, "admin: GET /api/v5/acl (authed)", adminBase+"/api/v5/acl", 200)
	httpCheckJar(hc, "dashboard: GET /api/acl/stats (authed)", adminBase+"/api/acl/stats", 200)
	httpCheck("metrics: GET /metrics", metricsBase+"/metrics", 200)

	if failures == 0 {
		fmt.Println("\nALL SMOKE TESTS PASSED")
	} else {
		fmt.Printf("\n%d SMOKE TEST(S) FAILED\n", failures)
		os.Exit(1)
	}
}

func httpCheck(name, url string, want int) {
	resp, err := http.Get(url)
	if err != nil {
		check(name, false, fmt.Sprintf("request error: %v", err))
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	check(name, resp.StatusCode == want, fmt.Sprintf("status=%d want=%d", resp.StatusCode, want))
}

func httpCheckJar(hc *http.Client, name, url string, want int) {
	resp, err := hc.Get(url)
	if err != nil {
		check(name, false, fmt.Sprintf("request error: %v", err))
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	check(name, resp.StatusCode == want, fmt.Sprintf("status=%d want=%d", resp.StatusCode, want))
}
