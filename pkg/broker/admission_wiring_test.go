package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	packets "github.com/mochi-mqtt/server/v2/packets"
	"github.com/turtacn/grovemq/pkg/balancer"
)

// TestSplitAdmissionRef verifies that the query-string trick used internally
// by the admission controller never leaks into the wire-level Server
// Reference: the client gets a bare endpoint and the credential as user
// properties instead.
func TestSplitAdmissionRef(t *testing.T) {
	ref := "grovemq-mqtt:1883?redirect=deadbeef&ts=1700000000&node=n2"
	endpoint, props := splitAdmissionRef(ref)
	if endpoint != "grovemq-mqtt:1883" {
		t.Fatalf("endpoint must be stripped of the query, got %q", endpoint)
	}
	want := map[string]string{
		balancer.RedirectTokenProperty: "deadbeef",
		balancer.RedirectNodeProperty:  "n2",
		balancer.RedirectTimeProperty:  "1700000000",
	}
	if len(props) != len(want) {
		t.Fatalf("expected %d user properties, got %d", len(want), len(props))
	}
	for _, p := range props {
		if want[p.Key] != p.Val {
			t.Fatalf("property %q = %q, want %q", p.Key, p.Val, want[p.Key])
		}
	}

	// A plain endpoint (or an unrecognised format) must be forwarded verbatim.
	for _, plain := range []string{"10.0.0.2:1883", "mqtts://broker.example.com:8883"} {
		got, extra := splitAdmissionRef(plain)
		if got != plain || len(extra) != 0 {
			t.Fatalf("plain reference %q changed into (%q, %v)", plain, got, extra)
		}
	}
	if got, extra := splitAdmissionRef(""); got != "" || len(extra) != 0 {
		t.Fatalf("empty reference must stay empty, got (%q, %v)", got, extra)
	}
}

// admissionTestRef produces a real redirect Decision from the admission
// controller and returns its Server Reference plus the extracted credential.
func admissionTestRef(t *testing.T, clientID string) (ref, token, nodeID string, ts int64) {
	t.Helper()
	now := time.Now()
	ctrl := balancer.NewAdmissionController(balancer.Config{
		Mode:              "redirect",
		MQTTAdvertiseAddr: "grovemq-mqtt:1883",
		SoftLimit:         0.5,
		Secret:            []byte("wiring-test-secret"),
		Loads: func() balancer.LoadSnapshot {
			n := now.UnixNano()
			return balancer.LoadSnapshot{
				"n1": {NodeID: "n1", Connections: 95, MaxConnections: 100, UnixNano: n},
				"n2": {NodeID: "n2", Connections: 10, MaxConnections: 100, UnixNano: n},
			}
		},
		Now: func() time.Time { return now },
	})
	d := ctrl.OnConnect(clientID, 5, balancer.NodeLoad{NodeID: "n1"})
	if d.Accept || d.ReasonCode != 0x9C {
		t.Fatalf("expected a 0x9C redirect decision, got %+v", d)
	}
	_, token, nodeID, ts, ok := balancer.ParseServerRef(d.ServerRef)
	if !ok {
		t.Fatalf("decision ServerRef is not parseable: %q", d.ServerRef)
	}
	return d.ServerRef, token, nodeID, ts
}

func dialTestBroker(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(addr, "tcp://"), 2*time.Second)
	if err != nil {
		t.Fatalf("dial test broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	return conn
}

func writeConnect(t *testing.T, conn net.Conn, clientID string, props map[string]string) {
	t.Helper()
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			Clean:            true,
			Keepalive:        60,
			ClientIdentifier: clientID,
		},
	}
	for k, v := range props {
		pk.Properties.User = append(pk.Properties.User, packets.UserProperty{Key: k, Val: v})
	}
	var buf bytes.Buffer
	if err := pk.ConnectEncode(&buf); err != nil {
		t.Fatalf("ConnectEncode: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
}

func readConnack(t *testing.T, conn net.Conn) *packets.Packet {
	t.Helper()
	// readPacket is the server-side decoder and deliberately has no CONNACK
	// branch, so decode the response explicitly as an MQTT 5.0 CONNACK.
	r := bufio.NewReader(conn)
	first, err := r.ReadByte()
	if err != nil {
		t.Fatalf("read CONNACK fixed header: %v", err)
	}
	fh := new(packets.FixedHeader)
	if err := fh.Decode(first); err != nil {
		t.Fatalf("decode CONNACK fixed header: %v", err)
	}
	if fh.Type != packets.Connack {
		t.Fatalf("expected CONNACK, got packet type %d", fh.Type)
	}
	rem, _, err := packets.DecodeLength(r)
	if err != nil {
		t.Fatalf("decode CONNACK length: %v", err)
	}
	body := make([]byte, rem)
	if _, err := io.ReadFull(r, body); err != nil {
		t.Fatalf("read CONNACK body: %v", err)
	}
	pk := &packets.Packet{FixedHeader: *fh, ProtocolVersion: 5}
	if err := pk.ConnackDecode(body); err != nil {
		t.Fatalf("decode CONNACK: %v", err)
	}
	return pk
}

// TestConnackRedirectWireContract is the end-to-end counterpart of
// TestConnackRedirectWireFormat (which only checks the encoder in isolation):
// it drives a real CONNECT through handleConnection and inspects the emitted
// CONNACK bytes.
func TestConnackRedirectWireContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, addr := startTestBroker(ctx, t)

	ref, token, nodeID, ts := admissionTestRef(t, "client-redirect-wire")
	b.AdmissionFunc = func(clientID string, protoVer byte) (byte, string) {
		return 0x9C, ref
	}

	conn := dialTestBroker(t, addr)
	writeConnect(t, conn, "client-redirect-wire", nil)

	pk := readConnack(t, conn)
	if pk.ReasonCode != 0x9C {
		t.Fatalf("expected reason code 0x9C, got 0x%02x", pk.ReasonCode)
	}
	if pk.Properties.ServerReference != "grovemq-mqtt:1883" {
		t.Fatalf("Server Reference must be a bare endpoint, got %q", pk.Properties.ServerReference)
	}
	got := make(map[string]string)
	for _, p := range pk.Properties.User {
		got[p.Key] = p.Val
	}
	if got[balancer.RedirectTokenProperty] != token ||
		got[balancer.RedirectNodeProperty] != nodeID ||
		got[balancer.RedirectTimeProperty] != strconv.FormatInt(ts, 10) {
		t.Fatalf("redirect credential missing from CONNACK user properties: %v", got)
	}
}

// TestValidRedirectTokenForcesLocalAccept closes the single-hop loop: a client
// that reconnects carrying the credential must be accepted (0x00) even though
// AdmissionFunc would redirect it again. Without the credential the same
// CONNECT is redirected (0x9C).
func TestValidRedirectTokenForcesLocalAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, addr := startTestBroker(ctx, t)

	ref, token, nodeID, ts := admissionTestRef(t, "client-loop")
	secret := []byte("wiring-test-secret")
	b.AdmissionFunc = func(clientID string, protoVer byte) (byte, string) {
		return 0x9C, ref
	}
	b.RedirectTokenFunc = func(clientID string, props map[string]string) bool {
		parsed, err := strconv.ParseInt(props[balancer.RedirectTimeProperty], 10, 64)
		if err != nil {
			return false
		}
		return balancer.ValidateRedirectToken(secret, clientID,
			props[balancer.RedirectTokenProperty], parsed, props[balancer.RedirectNodeProperty])
	}

	credential := map[string]string{
		balancer.RedirectTokenProperty: token,
		balancer.RedirectNodeProperty:  nodeID,
		balancer.RedirectTimeProperty:  strconv.FormatInt(ts, 10),
	}

	// 1) A fresh CONNECT is redirected.
	conn1 := dialTestBroker(t, addr)
	writeConnect(t, conn1, "client-loop", nil)
	if pk := readConnack(t, conn1); pk.ReasonCode != 0x9C {
		t.Fatalf("fresh CONNECT should be redirected, got 0x%02x", pk.ReasonCode)
	}

	// 2) Reconnect with the credential: force-accepted, no Server Reference.
	conn2 := dialTestBroker(t, addr)
	writeConnect(t, conn2, "client-loop", credential)
	pk := readConnack(t, conn2)
	if pk.ReasonCode != 0x00 {
		t.Fatalf("credentialed reconnect must be accepted, got 0x%02x", pk.ReasonCode)
	}
	if pk.Properties.ServerReference != "" {
		t.Fatalf("force-accepted CONNACK must not carry a Server Reference: %q", pk.Properties.ServerReference)
	}

	// 3) A forged credential must not bypass admission.
	conn3 := dialTestBroker(t, addr)
	bad := map[string]string{
		balancer.RedirectTokenProperty: "forged",
		balancer.RedirectNodeProperty:  nodeID,
		balancer.RedirectTimeProperty:  strconv.FormatInt(ts, 10),
	}
	writeConnect(t, conn3, "client-loop", bad)
	if pk := readConnack(t, conn3); pk.ReasonCode != 0x9C {
		t.Fatalf("forged credential must not force acceptance, got 0x%02x", pk.ReasonCode)
	}
}
