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

// Package gateway bridges standard A2A JSON-RPC SDK calls to the A2A over
// MQTT topic space. The HTTP surface deliberately speaks JSON-RPC 2.0 so an
// SDK configured for the official JSON-RPC binding needs only a URL change.
package gateway

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	a2ametrics "github.com/turtacn/grovemq/pkg/a2a"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/hooks"
)

// PropertyPrincipal carries the authenticated HTTP principal into the MQTT
// bridge. The broker's A2A request-auth hook can bind it to the request topic.
const PropertyPrincipal = hooks.A2APropertyPrincipal

const (
	// PathPrefix is the HTTP path prefix for the A2A gateway.
	PathPrefix = "/a2a/v1/"
	// MaxRequestBytes bounds a single JSON-RPC request body.
	MaxRequestBytes = 1 << 20
)

// AgentLookup reports whether an agent address is registered. When supplied,
// the gateway rejects unknown addresses with the A2A TaskNotFoundError instead
// of waiting for a transport timeout.
type AgentLookup func(address string) bool

// AuthConfig configures HTTP bearer-token authentication for the gateway.
// Tokens map a raw bearer token to the principal forwarded to the MQTT bridge.
// An empty token map disables authentication, preserving the legacy behaviour.
type AuthConfig struct {
	Tokens map[string]string
}

// Handler serves POST /a2a/v1/{org}/{unit}/{agent} JSON-RPC requests.
type Handler struct {
	bridge Bridge
	lookup AgentLookup
	auth   *AuthConfig
}

// NewHandler creates a gateway HTTP handler backed by bridge.
func NewHandler(bridge Bridge) *Handler {
	return &Handler{bridge: bridge}
}

// NewHandlerWithAgentLookup creates a handler that checks agent registration
// before forwarding a request. A nil lookup keeps the bridge-only behaviour.
func NewHandlerWithAgentLookup(bridge Bridge, lookup AgentLookup) *Handler {
	return &Handler{bridge: bridge, lookup: lookup}
}

// NewHandlerWithAuth creates a handler with both agent lookup and bearer-token
// authentication. A nil config or empty token map leaves authentication off.
func NewHandlerWithAuth(bridge Bridge, lookup AgentLookup, auth *AuthConfig) *Handler {
	return &Handler{bridge: bridge, lookup: lookup, auth: auth}
}

func (h *Handler) authenticate(r *http.Request) (string, bool) {
	if h == nil || h.auth == nil || len(h.auth.Tokens) == 0 {
		return "", true // authentication disabled
	}
	header := r.Header.Get("Authorization")
	if len(header) < 8 || !strings.EqualFold(header[:7], "Bearer ") {
		return "", false
	}
	token := strings.TrimSpace(header[7:])
	for candidate, principal := range h.auth.Tokens {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(token)) == 1 {
			return principal, true
		}
	}
	return "", false
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h == nil || h.bridge == nil {
		a2ametrics.IncGatewayRequest("unknown", "disabled")
		writeRPC(w, http.StatusNotImplemented, task.NewSemanticErrorResponse(nil,
			task.NewError(task.ErrorAgentNotAvailable, "A2A gateway is not enabled")))
		return
	}

	address := strings.Trim(strings.TrimPrefix(r.URL.Path, PathPrefix), "/")
	if !hooks.ValidA2AAgentAddress(address) {
		a2ametrics.IncGatewayRequest("unknown", "invalid_address")
		writeRPC(w, http.StatusBadRequest, task.NewSemanticErrorResponse(nil,
			task.NewError(task.ErrorInvalidParams, "agent address must be {org}/{unit}/{agent}")))
		return
	}

	principal, ok := h.authenticate(r)
	if !ok {
		a2ametrics.IncGatewayRequest("unknown", "unauthorized")
		writeRPC(w, http.StatusUnauthorized, task.NewSemanticErrorResponse(nil,
			task.NewError(task.ErrorUnauthorized, "missing or invalid bearer token")))
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, MaxRequestBytes+1))
	if err != nil || len(body) > MaxRequestBytes {
		a2ametrics.IncGatewayRequest("unknown", "invalid_request")
		writeRPC(w, http.StatusBadRequest, task.NewSemanticErrorResponse(nil,
			task.NewError(task.ErrorInvalidRequest, "request body is unreadable or too large")))
		return
	}
	req, err := task.ParseRequest(body)
	if err != nil {
		a2ametrics.IncGatewayRequest("unknown", "invalid_request")
		writeRPC(w, http.StatusBadRequest, task.NewSemanticErrorResponse(nil,
			task.NewError(task.ErrorInvalidRequest, err.Error())))
		return
	}

	if version := r.Header.Get("A2A-Version"); version != "" && version != schema.ProtocolVersion {
		a2ametrics.IncGatewayRequest(req.Method, "rpc_error")
		writeRPC(w, http.StatusOK, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorVersionNotSupported, "unsupported A2A-Version: "+version)))
		return
	}
	method := task.NormalizeMethod(req.Method)
	if !task.IsSupportedMethod(method) {
		a2ametrics.IncGatewayRequest(req.Method, "rpc_error")
		writeRPC(w, http.StatusOK, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorMethodNotFound, "unsupported method: "+req.Method)))
		return
	}
	req.Method = method

	if h.lookup != nil && !h.lookup(address) {
		a2ametrics.IncGatewayRequest(method, "agent_not_found")
		writeRPC(w, http.StatusOK, task.NewSemanticErrorResponse(req.ID, task.AgentNotFound(address)))
		return
	}

	props := propertiesFromHeaders(r.Header)
	if principal != "" {
		props[PropertyPrincipal] = []byte(principal)
	}
	resp, err := h.bridge.Request(r.Context(), address, req, props)
	if err != nil {
		a2ametrics.IncGatewayRequest(method, "transport_error")
		writeRPC(w, http.StatusBadGateway, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorAgentNotAvailable, err.Error())))
		return
	}
	if resp != nil && resp.Error == nil {
		if err := task.ValidateResultPlacement(resp.Result); err != nil {
			a2ametrics.IncGatewayRequest(method, "invalid_result")
			writeRPC(w, http.StatusBadGateway, task.NewSemanticErrorResponse(req.ID,
				task.NewError(task.ErrorInternal, err.Error())))
			return
		}
	}
	if resp != nil && resp.Error != nil {
		a2ametrics.IncGatewayRequest(method, "rpc_error")
	} else {
		a2ametrics.IncGatewayRequest(method, "success")
	}
	writeRPC(w, http.StatusOK, resp)
}

func writeRPC(w http.ResponseWriter, status int, resp *task.Response) {
	w.Header().Set("Content-Type", task.ContentType)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func propertiesFromHeaders(header http.Header) map[string][]byte {
	props := make(map[string][]byte)
	for _, name := range []string{"A2A-Version", "A2A-Extensions", "Authorization", "Content-Type"} {
		if value := header.Get(name); value != "" {
			props[name] = []byte(value)
		}
	}
	return props
}
