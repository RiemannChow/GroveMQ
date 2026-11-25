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

package hooks

import (
	"context"
	"strings"

	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/a2a/task"
)

// A2ATaskHook validates A2A JSON-RPC requests published to
// $a2a/v1/{org}/{unit}/{agent}/requests and maps semantic errors to MQTT v5
// PUBACK reason codes plus the a2a-error user property. It is inert for all
// other topics, so ordinary MQTT traffic is unaffected.
type A2ATaskHook struct {
	BaseHook
}

// NewA2ATaskHook creates the A2A task validation hook.
func NewA2ATaskHook() *A2ATaskHook { return &A2ATaskHook{} }

// Name implements Hook.
func (h *A2ATaskHook) Name() string { return "a2a-task-validation" }

// Points reports the single point this hook acts on.
func (h *A2ATaskHook) Points() []string { return []string{OnMessagePublishPoint} }

// OnMessagePublish validates request envelopes. The response itself is not
// inspected here: the gateway/SDK codec parses it after correlation.
func (h *A2ATaskHook) OnMessagePublish(_ context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	if c == nil {
		return &MessagePublishResult{Allow: true}, nil
	}
	address, ok := task.ParseRequestTopic(c.Topic)
	if !ok {
		return &MessagePublishResult{Allow: true}, nil
	}
	_ = address // topic shape is enough; the client identity is checked by ACLs

	if version := string(c.UserProperties["A2A-Version"]); version != "" && version != schema.ProtocolVersion {
		return rejectA2A(task.NewError(task.ErrorVersionNotSupported, "unsupported A2A-Version: "+version)), nil
	}
	req, err := task.ParseRequest(c.Payload)
	if err != nil {
		return rejectA2A(task.NewError(task.ErrorInvalidRequest, err.Error())), nil
	}
	if !task.IsSupportedMethod(req.Method) {
		return rejectA2A(task.NewError(task.ErrorMethodNotFound, "unsupported method: "+req.Method)), nil
	}
	return &MessagePublishResult{Allow: true}, nil
}

func rejectA2A(sem *task.SemanticError) *MessagePublishResult {
	code := sem.Name.MQTTReason()
	return &MessagePublishResult{
		Allow:          false,
		Reason:         sem.Error(),
		ReasonCode:     &code,
		UserProperties: sem.UserProperties(),
	}
}

// A2AErrorProperty is re-exported for broker logging and tests. The canonical
// definition lives in pkg/a2a/task so both HTTP and MQTT bindings agree.
const A2AErrorProperty = task.PropertyAgentError

// propertyStrings converts MQTT user properties into deterministic key/value
// pairs for logs. It is not part of the hook contract but useful for tests.
func propertyStrings(props map[string][]byte) []string {
	out := make([]string, 0, len(props))
	for k, v := range props {
		out = append(out, k+"="+string(v))
	}
	sortStrings(out)
	return out
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && strings.Compare(values[j-1], values[j]) > 0; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}
