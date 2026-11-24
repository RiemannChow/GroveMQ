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
	"encoding/json"
	"io"
	"net/http"
	"strings"

	a2ametrics "github.com/turtacn/grovemq/pkg/a2a"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/a2a/task"
	"github.com/turtacn/grovemq/pkg/hooks"
)

const (
	// PathPrefix is the HTTP path prefix for the A2A gateway.
	PathPrefix = "/a2a/v1/"
	// MaxRequestBytes bounds a single JSON-RPC request body.
	MaxRequestBytes = 1 << 20
)

// Handler serves POST /a2a/v1/{org}/{unit}/{agent} JSON-RPC requests.
type Handler struct {
	bridge Bridge
}

// NewHandler creates a gateway HTTP handler backed by bridge.
func NewHandler(bridge Bridge) *Handler {
	return &Handler{bridge: bridge}
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
	method := normalizeMethod(req.Method)
	if method == "" {
		a2ametrics.IncGatewayRequest(req.Method, "rpc_error")
		writeRPC(w, http.StatusOK, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorMethodNotFound, "unsupported method: "+req.Method)))
		return
	}
	req.Method = method

	resp, err := h.bridge.Request(r.Context(), address, req, propertiesFromHeaders(r.Header))
	if err != nil {
		a2ametrics.IncGatewayRequest(method, "transport_error")
		writeRPC(w, http.StatusBadGateway, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorAgentNotAvailable, err.Error())))
		return
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

// normalizeMethod maps common SDK aliases onto canonical A2A 1.0 JSON-RPC
// method names. Unknown methods stay empty and are rejected by ServeHTTP.
func normalizeMethod(method string) string {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "message/send", "sendmessage", "send_message":
		return task.MethodMessageSend
	case "message/stream", "sendstreamingmessage", "send_streaming_message":
		return task.MethodMessageStream
	case "tasks/get", "gettask", "get_task":
		return task.MethodTasksGet
	case "tasks/cancel", "canceltask", "cancel_task":
		return task.MethodTasksCancel
	case "tasks/subscribe", "subscribetotask", "subscribe_to_task":
		return task.MethodTasksSubscribe
	default:
		return ""
	}
}
