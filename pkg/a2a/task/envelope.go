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

package task

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// JSONRPCVersion is the envelope version used by the MQTT and HTTP
	// bindings so a standard A2A SDK can reuse its JSON-RPC codec.
	JSONRPCVersion = "2.0"
	// ContentType is the MQTT v5 Content Type and HTTP Content-Type for A2A
	// payloads.
	ContentType = "application/a2a+json"

	MethodMessageSend    = "message/send"
	MethodMessageStream  = "message/stream"
	MethodTasksGet       = "tasks/get"
	MethodTasksCancel    = "tasks/cancel"
	MethodTasksSubscribe = "tasks/subscribe"
)

// Request is a JSON-RPC 2.0 request carrying an A2A abstract operation.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Error is a JSON-RPC 2.0 error object.
type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Response is a JSON-RPC 2.0 response carrying an A2A result or error.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *Error          `json:"error,omitempty"`
}

// NewRequest builds a request and validates that method and ID are present.
func NewRequest(id interface{}, method string, params interface{}) (*Request, error) {
	if method == "" {
		return nil, fmt.Errorf("a2a: request method is required")
	}
	rawID, err := json.Marshal(id)
	if err != nil {
		return nil, fmt.Errorf("a2a: encode request id: %w", err)
	}
	if string(rawID) == "null" {
		return nil, fmt.Errorf("a2a: request id is required")
	}
	var rawParams json.RawMessage
	if params != nil {
		rawParams, err = json.Marshal(params)
		if err != nil {
			return nil, fmt.Errorf("a2a: encode request params: %w", err)
		}
	}
	return &Request{JSONRPC: JSONRPCVersion, ID: rawID, Method: method, Params: rawParams}, nil
}

// ParseRequest decodes and minimally validates a request envelope.
func ParseRequest(data []byte) (*Request, error) {
	var r Request
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("a2a: invalid JSON-RPC request: %w", err)
	}
	if r.JSONRPC != JSONRPCVersion {
		return nil, fmt.Errorf("a2a: unsupported JSON-RPC version %q", r.JSONRPC)
	}
	if r.Method == "" {
		return nil, fmt.Errorf("a2a: request method is required")
	}
	if len(r.ID) == 0 || string(r.ID) == "null" {
		return nil, fmt.Errorf("a2a: request id is required")
	}
	return &r, nil
}

// NormalizeMethod maps common SDK aliases onto canonical A2A 1.0 JSON-RPC
// method names. Unknown methods are returned unchanged so callers can report
// MethodNotFoundError without losing the original value.
func NormalizeMethod(method string) string {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "message/send", "sendmessage", "send_message":
		return MethodMessageSend
	case "message/stream", "sendstreamingmessage", "send_streaming_message":
		return MethodMessageStream
	case "tasks/get", "gettask", "get_task":
		return MethodTasksGet
	case "tasks/cancel", "canceltask", "cancel_task":
		return MethodTasksCancel
	case "tasks/subscribe", "subscribetotask", "subscribe_to_task":
		return MethodTasksSubscribe
	default:
		return method
	}
}

// IsSupportedMethod reports whether method is one of the abstract operations
// GroveMQ can route over MQTT.
func IsSupportedMethod(method string) bool {
	switch NormalizeMethod(method) {
	case MethodMessageSend, MethodMessageStream, MethodTasksGet, MethodTasksCancel, MethodTasksSubscribe:
		return true
	default:
		return false
	}
}

// ParamsInto decodes request params into a caller-provided value.
func (r *Request) ParamsInto(v interface{}) error {
	if len(r.Params) == 0 {
		return fmt.Errorf("a2a: request params are missing")
	}
	if err := json.Unmarshal(r.Params, v); err != nil {
		return fmt.Errorf("a2a: invalid request params: %w", err)
	}
	return nil
}

// NewResponse builds a successful response with the same request ID.
func NewResponse(id json.RawMessage, result interface{}) (*Response, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("a2a: encode response result: %w", err)
	}
	return &Response{JSONRPC: JSONRPCVersion, ID: cloneRaw(id), Result: raw}, nil
}

// NewErrorResponse builds a JSON-RPC error response with the same request ID.
func NewErrorResponse(id json.RawMessage, rpcErr *Error) *Response {
	return &Response{JSONRPC: JSONRPCVersion, ID: cloneRaw(id), Error: rpcErr}
}

// ParseResponse decodes and minimally validates a response envelope.
func ParseResponse(data []byte) (*Response, error) {
	var r Response
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("a2a: invalid JSON-RPC response: %w", err)
	}
	if r.JSONRPC != JSONRPCVersion {
		return nil, fmt.Errorf("a2a: unsupported JSON-RPC version %q", r.JSONRPC)
	}
	if r.Error == nil && len(r.Result) == 0 {
		return nil, fmt.Errorf("a2a: response has neither result nor error")
	}
	return &r, nil
}

// ResultInto decodes a successful response result into a caller-provided value.
func (r *Response) ResultInto(v interface{}) error {
	if r.Error != nil {
		return fmt.Errorf("a2a: response error %d: %s", r.Error.Code, r.Error.Message)
	}
	if len(r.Result) == 0 {
		return fmt.Errorf("a2a: response result is missing")
	}
	if err := json.Unmarshal(r.Result, v); err != nil {
		return fmt.Errorf("a2a: invalid response result: %w", err)
	}
	return nil
}

func cloneRaw(v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(v))
	copy(out, v)
	return out
}
