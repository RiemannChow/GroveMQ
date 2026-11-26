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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/turtacn/grovemq/pkg/storage"
)

// StoreKeyPrefix namespaces A2A task records inside the shared key-value
// backend, so Bolt-backed sessions and A2A tasks never collide.
const StoreKeyPrefix = "a2a/task/"

// Store persists complete A2A Task objects (status, artifacts and history) so
// a task survives a broker restart and can be queried while its agent is
// offline. It is a thin, prefix-scoped layer over the broker's generic
// storage.Store; passing nil disables persistence without affecting callers.
type Store struct {
	backend storage.Store
	mu      sync.RWMutex
	keys    map[string]struct{}
}

// NewStore wraps a key-value backend. A nil backend yields a disabled store
// whose methods are safe no-ops (reads miss, writes succeed but persist
// nothing). Callers can keep a single nil-check-free path.
func NewStore(backend storage.Store) *Store {
	s := &Store{backend: backend, keys: make(map[string]struct{})}
	_ = s.hydrateKeys()
	return s
}

// Enabled reports whether a durable backend is attached.
func (s *Store) Enabled() bool {
	return s != nil && s.backend != nil
}

// key builds a namespaced, address-scoped task key. The address is part of the
// key so two agents may use the same task ID without overwriting each other.
func key(address, taskID string) string {
	return StoreKeyPrefix + address + "/" + taskID
}

// hydrateKeys rebuilds the in-memory index from an enumerable backend. The
// generic Store interface has no iteration method, so backends that implement
// ForEach (BoltStore) are detected via a narrow interface. An in-memory backend
// has no cross-restart state and simply yields no keys.
func (s *Store) hydrateKeys() error {
	if s == nil || s.backend == nil {
		return nil
	}
	enumerable, ok := s.backend.(interface {
		ForEach(fn func(key string, value []byte) error) error
	})
	if !ok {
		return nil
	}
	return enumerable.ForEach(func(k string, _ []byte) error {
		if strings.HasPrefix(k, StoreKeyPrefix) {
			s.keys[k] = struct{}{}
		}
		return nil
	})
}

// Save writes a task, replacing any previous record with the same address and
// ID. A timestamp is stamped on the status when the caller left it empty so
// persisted records always carry a meaningful ordering hint.
func (s *Store) Save(address string, t *Task) error {
	if !s.Enabled() || t == nil || t.ID == "" {
		return nil
	}
	if t.Status.Timestamp == "" {
		t.Status.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	raw, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("a2a store: encode task %s: %w", t.ID, err)
	}
	k := key(address, t.ID)
	if err := s.backend.Set(k, raw); err != nil {
		return fmt.Errorf("a2a store: save task %s: %w", t.ID, err)
	}
	s.mu.Lock()
	s.keys[k] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Get returns a persisted task by address and task ID. The boolean is false
// when the store is disabled, the key is absent, or the payload is corrupt.
func (s *Store) Get(address, taskID string) (*Task, bool) {
	if !s.Enabled() || address == "" || taskID == "" {
		return nil, false
	}
	k := key(address, taskID)
	value, err := s.backend.Get(k)
	if err != nil {
		return nil, false
	}
	raw, ok := value.([]byte)
	if !ok {
		return nil, false
	}
	var t Task
	if err := json.Unmarshal(raw, &t); err != nil || t.ID == "" {
		return nil, false
	}
	return &t, true
}

// AddArtifact appends an artifact to a persisted task and writes it back in
// one operation. This is the durable counterpart of the streaming artifact
// events emitted over MQTT.
func (s *Store) AddArtifact(address, taskID string, a Artifact) (*Task, error) {
	t, ok := s.Get(address, taskID)
	if !ok {
		return nil, fmt.Errorf("a2a store: task %s not found", taskID)
	}
	if err := t.AddArtifact(a); err != nil {
		return nil, err
	}
	if err := s.Save(address, t); err != nil {
		return nil, err
	}
	return t, nil
}

// SetState transitions a persisted task using the A2A state machine, so an
// illegal transition cannot silently overwrite durable state.
func (s *Store) SetState(address, taskID string, state TaskState) (*Task, error) {
	t, ok := s.Get(address, taskID)
	if !ok {
		return nil, fmt.Errorf("a2a store: task %s not found", taskID)
	}
	if err := t.Transition(state); err != nil {
		return nil, err
	}
	if err := s.Save(address, t); err != nil {
		return nil, err
	}
	return t, nil
}

// Delete removes one persisted task.
func (s *Store) Delete(address, taskID string) bool {
	if !s.Enabled() {
		return false
	}
	k := key(address, taskID)
	if err := s.backend.Delete(k); err != nil {
		return false
	}
	s.mu.Lock()
	_, existed := s.keys[k]
	delete(s.keys, k)
	s.mu.Unlock()
	return existed
}

// List returns all persisted tasks for an address, sorted by ID for stable
// output. Records written by a backend whose Get fails are skipped rather
// than aborting the whole listing.
func (s *Store) List(address string) []*Task {
	if !s.Enabled() {
		return nil
	}
	prefix := StoreKeyPrefix + address + "/"
	s.mu.RLock()
	keys := make([]string, 0, len(s.keys))
	for k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	out := make([]*Task, 0, len(keys))
	for _, k := range keys {
		value, err := s.backend.Get(k)
		if err != nil {
			continue
		}
		raw, ok := value.([]byte)
		if !ok {
			continue
		}
		var t Task
		if json.Unmarshal(raw, &t) == nil && t.ID != "" {
			out = append(out, &t)
		}
	}
	return out
}
