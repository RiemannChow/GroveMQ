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

package broker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// wsNetConn adapts a *websocket.Conn to the net.Conn interface expected by
// handleConnection. MQTT-over-WebSocket carries one MQTT packet per binary
// WebSocket message, so we buffer the current message and serve it as a stream
// via Read, and write each outgoing packet as a single binary message.
//
// This lets the entire existing MQTT packet loop (CONNECT/SUBSCRIBE/PUBLISH/
// DISCONNECT, keep-alive, the hook chain, ACL, auth, …) be reused unchanged for
// WebSocket clients.
type wsNetConn struct {
	*websocket.Conn
	reader io.Reader // current message reader; nil means the next message must be pulled
}

// Read implements net.Conn. It drains the current WebSocket message first, then
// pulls the next binary message as needed.
func (c *wsNetConn) Read(p []byte) (int, error) {
	for {
		if c.reader == nil {
			_, r, err := c.Conn.NextReader()
			if err != nil {
				return 0, err
			}
			c.reader = r
		}
		n, err := c.reader.Read(p)
		if err == io.EOF {
			c.reader = nil
			if n > 0 {
				return n, nil
			}
			// Message fully consumed; loop to pull the next one.
			continue
		}
		return n, err
	}
}

// Write implements net.Conn by emitting a single binary WebSocket message.
func (c *wsNetConn) Write(p []byte) (int, error) {
	if err := c.Conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// SetDeadline implements net.Conn. *websocket.Conn only exposes per-direction
// deadlines, so we set both.
func (c *wsNetConn) SetDeadline(t time.Time) error {
	if err := c.Conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.Conn.SetWriteDeadline(t)
}

// StartWebSocketServer starts an MQTT-over-WebSocket listener on the given
// address. Clients must connect to the "/mqtt" path and speak the "mqtt"
// subprotocol. Each upgraded connection is handed to handleConnection, so all
// MQTT semantics and extension points apply identically to WebSocket clients.
//
// The listener is bound once at startup; changing the port requires a restart
// (config reload does not rebind listeners).
func (b *Broker) StartWebSocketServer(ctx context.Context, addr string) error {
	upgrader := websocket.Upgrader{
		HandshakeTimeout: 10 * time.Second,
		// Only the standard MQTT subprotocol is accepted.
		Subprotocols: []string{"mqtt"},
		// TODO: make origin checking configurable. For now permissive to match
		// the TCP listener behaviour (no origin concept on raw TCP).
		CheckOrigin: func(_ *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/mqtt", func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[WARN] WebSocket upgrade failed from %s: %v", r.RemoteAddr, err)
			return
		}
		go b.handleConnection(ctx, &wsNetConn{Conn: wsConn})
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Gracefully shut the HTTP server down when the broker context is canceled.
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	log.Printf("MQTT WebSocket broker listening on %s (path=/mqtt, subprotocol=mqtt)", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("websocket server on %s failed: %w", addr, err)
	}
	return nil
}
