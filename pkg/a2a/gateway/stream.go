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
	"encoding/json"
	"fmt"

	"github.com/turtacn/grovemq/pkg/a2a/task"
)

// StreamBridge carries a streaming A2A request to an MQTT Agent and exposes
// the resulting task events. Implementations must close the stream when the
// task reaches a terminal state or the context is cancelled.
type StreamBridge interface {
	Stream(ctx context.Context, address string, req *task.Request, props map[string][]byte) (*task.Stream, error)
}

// TaskStream is the streamed task handle returned by StreamBridge.
type TaskStream = task.Stream

// StreamParams are the params accepted by message/stream. TaskId, when set,
// selects an existing task to subscribe to; otherwise the agent creates one.
type StreamParams struct {
	Message  jsonMessage            `json:"message"`
	TaskID   string                 `json:"taskId,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// jsonMessage is a permissive representation of the A2A Message payload. The
// gateway forwards it verbatim to MQTT so SDK-specific fields are preserved.
type jsonMessage map[string]interface{}

// ParseStreamParams decodes message/stream params and validates that either a
// message or an existing taskId is present, as required by A2A 1.0.
func ParseStreamParams(req *task.Request) (*StreamParams, error) {
	if req == nil {
		return nil, fmt.Errorf("a2a: nil stream request")
	}
	if len(req.Params) == 0 {
		return nil, fmt.Errorf("a2a: stream params are missing")
	}
	var params StreamParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return nil, fmt.Errorf("a2a: invalid stream params: %w", err)
	}
	if params.TaskID == "" && len(params.Message) == 0 {
		return nil, fmt.Errorf("a2a: stream params require message or taskId")
	}
	return &params, nil
}
