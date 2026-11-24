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

package gateway

import (
	"context"
	"sync"

	"github.com/turtacn/grovemq/pkg/a2a/task"
)

// Bridge carries a JSON-RPC A2A request from HTTP to an MQTT Agent and back.
// Implementations must preserve the request ID and may attach MQTT v5 user
// properties supplied by the caller.
type Bridge interface {
	Request(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Response, error)
}

// HandlerFunc handles one A2A request in-process. It is used by embedded
// agents and by tests; returning a SemanticError produces a JSON-RPC error
// response rather than a transport error.
type HandlerFunc func(ctx context.Context, address string, req *task.Request, props map[string][]byte) (interface{}, *task.SemanticError)

// MemoryBridge is an in-process Bridge. It is also the extension point for
// agents embedded in the broker process.
type MemoryBridge struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

// NewMemoryBridge creates an empty in-process bridge.
func NewMemoryBridge() *MemoryBridge {
	return &MemoryBridge{handlers: make(map[string]HandlerFunc)}
}

// Register installs a handler for one three-part agent address.
func (b *MemoryBridge) Register(address string, fn HandlerFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[address] = fn
}

// Request implements Bridge.
func (b *MemoryBridge) Request(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Response, error) {
	b.mu.RLock()
	fn, ok := b.handlers[address]
	b.mu.RUnlock()
	if !ok {
		return task.NewSemanticErrorResponse(req.ID, task.NewError(task.ErrorAgentNotAvailable, "agent is not available: "+address)), nil
	}
	result, semErr := fn(ctx, address, req, props)
	if semErr != nil {
		return task.NewSemanticErrorResponse(req.ID, semErr), nil
	}
	return task.NewResponse(req.ID, result)
}
