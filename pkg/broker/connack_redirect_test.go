package broker

import (
	"bytes"
	"testing"

	packets "github.com/mochi-mqtt/server/v2/packets"
)

// TestConnackRedirectWireFormat verifies the MQTT 5.0 encoder emits reason
// code 0x9C (Use another server) together with the Server Reference property
// (identifier 28). Both CONNACK paths in broker.go construct exactly this
// packet when the access-layer admission hook decides to redirect.
func TestConnackRedirectWireFormat(t *testing.T) {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connack},
		ReasonCode:      0x9C,
		ProtocolVersion: 5,
	}
	// The broker emits a bare endpoint here; the one-hop credential travels as
	// CONNACK user properties (see admission_wiring_test.go).
	pk.Properties.ServerReference = "grovemq-mqtt:1883"
	pk.Properties.User = []packets.UserProperty{
		{Key: "grovemq-redirect-token", Val: "deadbeef"},
		{Key: "grovemq-redirect-node", Val: "n2"},
		{Key: "grovemq-redirect-ts", Val: "1"},
	}

	var buf bytes.Buffer
	if err := pk.ConnackEncode(&buf); err != nil {
		t.Fatalf("ConnackEncode: %v", err)
	}
	b := buf.Bytes()
	if b[0] != 0x20 {
		t.Fatalf("not a CONNACK fixed header: %x", b)
	}
	if !bytes.Contains(b, []byte{0x9C}) {
		t.Fatalf("reason code 0x9C missing: %x", b)
	}
	if !bytes.Contains(b, []byte{0x1C}) {
		t.Fatalf("Server Reference property id 28 (0x1C) missing: %x", b)
	}
	if !bytes.Contains(b, []byte("grovemq-mqtt:1883")) {
		t.Fatalf("Server Reference value missing: %x", b)
	}
}
