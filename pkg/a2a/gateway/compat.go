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
	"encoding/json"
	"strings"

	"github.com/turtacn/grovemq/pkg/a2a/task"
)

// ExtendedCardLookup returns the extended Agent Card for an address. It is
// supplied by the Registry, which reads the authenticated x-extended-card
// field from the signed card. A nil result means no extended card is configured.
type ExtendedCardLookup func(address string) (json.RawMessage, bool)

// CapabilityLookup reports an agent's optional capability flags from its
// registered Agent Card. It is supplied by the Registry. A nil lookup means
// optional capabilities are unavailable and must be reported as unsupported.
type CapabilityLookup func(address string) (streaming, push, extended bool)

// CompatibilityContext holds broker-side state for A2A 1.0 optional methods
// that are answered locally rather than forwarded to the MQTT agent. The MQTT
// binding delivers task updates natively, so push notification configs are
// stored and acknowledged here for API parity; extended cards come from the
// Registry's authenticated card extension.
type CompatibilityContext struct {
	PushStore    *task.PushStore
	ExtendedCard ExtendedCardLookup
	Capabilities CapabilityLookup

	// PushNotificationsSupported is a global fallback used when Capabilities
	// is nil. When Capabilities is set, the per-agent flag wins.
	PushNotificationsSupported bool
	// ExtendedCardSupported is a global fallback used when Capabilities is nil.
	ExtendedCardSupported bool
	// RequireAuthForExtendedCard requires an authenticated principal before
	// releasing an extended card, which may carry private details.
	RequireAuthForExtendedCard bool
}

// pushSupported resolves the push-notification capability for one agent.
func (ctx *CompatibilityContext) pushSupported(address string) bool {
	if ctx.Capabilities != nil {
		_, push, _ := ctx.Capabilities(address)
		return push
	}
	return ctx.PushNotificationsSupported
}

// extendedSupported resolves the extended-card capability for one agent.
func (ctx *CompatibilityContext) extendedSupported(address string) bool {
	if ctx.Capabilities != nil {
		_, _, extended := ctx.Capabilities(address)
		return extended
	}
	return ctx.ExtendedCardSupported
}

// serveCompatibilityMethod handles A2A 1.0 optional methods locally. It
// returns (result, errorResponse, handled).
func serveCompatibilityMethod(ctx *CompatibilityContext, req *task.Request, address, principal string) (interface{}, *task.Response, bool) {
	if ctx == nil || req == nil {
		return nil, nil, false
	}
	switch req.Method {
	case task.MethodCreatePushConfig, task.MethodGetPushConfig,
		task.MethodListPushConfigs, task.MethodDeletePushConfig:
		return servePushMethod(ctx, req, address)
	case task.MethodGetExtendedCard:
		return serveExtendedCard(ctx, req, address, principal)
	default:
		return nil, nil, false
	}
}

func servePushMethod(ctx *CompatibilityContext, req *task.Request, address string) (interface{}, *task.Response, bool) {
	if !ctx.pushSupported(address) || ctx.PushStore == nil {
		return nil, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorPushNotSupported, "agent does not support push notification configs")), true
	}
	var params struct {
		TaskID   string          `json:"taskId"`
		ConfigID string          `json:"configId"`
		Config   json.RawMessage `json:"taskPushNotificationConfig"`
	}
	if len(req.Params) > 0 {
		_ = json.Unmarshal(req.Params, &params)
	}
	// A2A 1.0 uses taskId/configId in params; JSON-RPC SDK aliases sometimes
	// nest the full config object as the params themselves.
	if params.TaskID == "" && len(params.Config) == 0 {
		var cfg task.PushNotificationConfig
		if err := json.Unmarshal(req.Params, &cfg); err == nil && cfg.TaskID != "" {
			params.TaskID = cfg.TaskID
			params.Config, _ = json.Marshal(cfg)
		}
	}
	switch req.Method {
	case task.MethodCreatePushConfig:
		cfg, err := task.ParsePushConfig(params.Config)
		if err != nil {
			return nil, task.NewSemanticErrorResponse(req.ID, task.NewError(task.ErrorInvalidParams, err.Error())), true
		}
		created, semErr := ctx.PushStore.Create(params.TaskID, cfg)
		if semErr != nil {
			return nil, task.NewSemanticErrorResponse(req.ID, semErr), true
		}
		return created, nil, true
	case task.MethodGetPushConfig:
		cfg, semErr := ctx.PushStore.Get(params.TaskID, params.ConfigID)
		if semErr != nil {
			return nil, task.NewSemanticErrorResponse(req.ID, semErr), true
		}
		return cfg, nil, true
	case task.MethodListPushConfigs:
		configs, semErr := ctx.PushStore.List(params.TaskID)
		if semErr != nil {
			return nil, task.NewSemanticErrorResponse(req.ID, semErr), true
		}
		return configs, nil, true
	case task.MethodDeletePushConfig:
		if semErr := ctx.PushStore.Delete(params.TaskID, params.ConfigID); semErr != nil {
			return nil, task.NewSemanticErrorResponse(req.ID, semErr), true
		}
		return map[string]bool{"deleted": true}, nil, true
	}
	return nil, nil, false
}

func serveExtendedCard(ctx *CompatibilityContext, req *task.Request, address, principal string) (interface{}, *task.Response, bool) {
	if !ctx.extendedSupported(address) {
		return nil, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorUnsupportedOperation, "agent does not support extended card")), true
	}
	if ctx.RequireAuthForExtendedCard && strings.TrimSpace(principal) == "" {
		return nil, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorUnauthorized, "extended card requires authentication")), true
	}
	if ctx.ExtendedCard == nil {
		return nil, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorExtendedCardMissing, "no extended card configured")), true
	}
	card, ok := ctx.ExtendedCard(address)
	if !ok {
		return nil, task.NewSemanticErrorResponse(req.ID,
			task.NewError(task.ErrorExtendedCardMissing, "no extended card configured for "+address)), true
	}
	return card, nil, true
}
