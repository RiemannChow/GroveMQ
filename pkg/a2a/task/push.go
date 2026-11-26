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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PushAuthInfo is the A2A 1.0 AuthenticationInfo carried by a push config.
type PushAuthInfo struct {
	Schemes     []string `json:"schemes,omitempty"`
	Credentials string   `json:"credentials,omitempty"`
	Scheme      string   `json:"scheme,omitempty"`
}

// PushNotificationConfig is the A2A 1.0 TaskPushNotificationConfig.
type PushNotificationConfig struct {
	ID             string                 `json:"id"`
	TaskID         string                 `json:"taskId,omitempty"`
	URL            string                 `json:"url"`
	Authentication *PushAuthInfo          `json:"authentication,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// PushStore keeps push notification configs in memory. The MQTT binding does
// not need HTTP webhooks for task updates, but A2A 1.0 SDKs still call the
// four config methods, so GroveMQ stores them and acknowledges them faithfully.
type PushStore struct {
	mu      sync.RWMutex
	configs map[string]map[string]*PushNotificationConfig // taskID -> configID
}

// NewPushStore creates an empty push config store.
func NewPushStore() *PushStore {
	return &PushStore{configs: make(map[string]map[string]*PushNotificationConfig)}
}

// Create adds a config, assigning an ID when the client omits one.
func (s *PushStore) Create(taskID string, cfg *PushNotificationConfig) (*PushNotificationConfig, *SemanticError) {
	if s == nil {
		return nil, NewError(ErrorInternal, "push store is not available")
	}
	if cfg == nil || cfg.URL == "" {
		return nil, NewError(ErrorInvalidParams, "push config url is required")
	}
	stored := *cfg
	stored.TaskID = taskID
	if stored.ID == "" {
		stored.ID = "push-" + randomConfigID()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.configs[taskID] == nil {
		s.configs[taskID] = make(map[string]*PushNotificationConfig)
	}
	if _, exists := s.configs[taskID][stored.ID]; exists {
		return nil, NewError(ErrorInvalidParams, "push config id already exists: "+stored.ID)
	}
	s.configs[taskID][stored.ID] = &stored
	return &stored, nil
}

// Get returns one config by task and config ID.
func (s *PushStore) Get(taskID, configID string) (*PushNotificationConfig, *SemanticError) {
	if s == nil {
		return nil, NewError(ErrorInternal, "push store is not available")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.configs[taskID][configID]
	if !ok {
		return nil, TaskNotFound(configID)
	}
	copyCfg := *cfg
	return &copyCfg, nil
}

// List returns all configs for a task, sorted by ID for deterministic output.
func (s *PushStore) List(taskID string) ([]*PushNotificationConfig, *SemanticError) {
	if s == nil {
		return nil, NewError(ErrorInternal, "push store is not available")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.configs[taskID]))
	for id := range s.configs[taskID] {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*PushNotificationConfig, 0, len(ids))
	for _, id := range ids {
		copyCfg := *s.configs[taskID][id]
		out = append(out, &copyCfg)
	}
	return out, nil
}

// Delete removes one config and reports whether it existed.
func (s *PushStore) Delete(taskID, configID string) *SemanticError {
	if s == nil {
		return NewError(ErrorInternal, "push store is not available")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.configs[taskID][configID]; !ok {
		return TaskNotFound(configID)
	}
	delete(s.configs[taskID], configID)
	if len(s.configs[taskID]) == 0 {
		delete(s.configs, taskID)
	}
	return nil
}

// ParsePushConfig decodes a push config from JSON-RPC params.
func ParsePushConfig(params json.RawMessage) (*PushNotificationConfig, error) {
	var cfg PushNotificationConfig
	if err := json.Unmarshal(params, &cfg); err != nil {
		return nil, fmt.Errorf("invalid push config: %w", err)
	}
	return &cfg, nil
}

func randomConfigID() string {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
