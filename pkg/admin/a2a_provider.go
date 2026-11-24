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

package admin

import (
	"github.com/turtacn/grovemq/pkg/a2a/registry"
)

// a2aRegistryAdapter adapts pkg/a2a/registry.Registry to A2AProvider without
// making the registry depend on the admin package.
type a2aRegistryAdapter struct {
	r *registry.Registry
}

// NewA2AProvider wraps a Registry for use by the admin API. A nil registry
// returns nil, which leaves the A2A endpoints disabled.
func NewA2AProvider(r *registry.Registry) A2AProvider {
	if r == nil {
		return nil
	}
	return &a2aRegistryAdapter{r: r}
}

func (a *a2aRegistryAdapter) List() []A2AAgent {
	entries := a.r.List()
	out := make([]A2AAgent, 0, len(entries))
	for _, e := range entries {
		out = append(out, toAPIAgent(&e))
	}
	return out
}

func (a *a2aRegistryAdapter) Get(address string) (*A2AAgent, bool) {
	e, ok := a.r.Get(address)
	if !ok {
		return nil, false
	}
	out := toAPIAgent(e)
	return &out, true
}

func (a *a2aRegistryAdapter) Delete(address string) bool { return a.r.Delete(address) }

func (a *a2aRegistryAdapter) Stats() A2AStats {
	s := a.r.Stats()
	return A2AStats{
		Enabled:         s.Enabled,
		Registered:      s.Registered,
		Online:          s.Online,
		Offline:         s.Offline,
		ValidationFails: s.ValidationFails,
		RateLimited:     s.RateLimited,
		RejectedBySize:  s.RejectedBySize,
	}
}

func toAPIAgent(e *registry.Entry) A2AAgent {
	return A2AAgent{
		Address:      e.Address,
		OrgID:        e.OrgID,
		UnitID:       e.UnitID,
		AgentID:      e.AgentID,
		Card:         e.Card,
		Status:       e.Status,
		StatusSource: e.StatusSource,
		RegisteredAt: e.RegisteredAt,
		UpdatedAt:    e.UpdatedAt,
	}
}
