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

// Package load contains A2A foundation performance baselines.
//
// These benchmarks measure the three hot paths that P0 touches, so later A2A
// Registry work can detect regressions:
//
//   - BenchmarkA2ABaseline_RetainedStore: retained PUBLISH with user properties.
//   - BenchmarkA2ABaseline_RetainedLoad: retained delivery on subscription.
//   - BenchmarkA2ABaseline_IdentityHook: A2A discovery identity decision.
//
// Run with:
//
//	go test -run '^$' -bench BenchmarkA2ABaseline -benchmem ./tests/load/
package load

import (
	"context"
	"fmt"
	"testing"

	"github.com/turtacn/grovemq/pkg/hooks"
	"github.com/turtacn/grovemq/pkg/retainer"
	"github.com/turtacn/grovemq/pkg/storage/messages"
)

func newBaselineRetainer(b *testing.B) *retainer.Retainer {
	b.Helper()
	backend, err := messages.NewMemoryBackend(messages.DefaultConfig())
	if err != nil {
		b.Fatalf("new memory backend: %v", err)
	}
	r := retainer.New(backend, retainer.DefaultConfig())
	b.Cleanup(func() { _ = r.Close() })
	return r
}

func BenchmarkA2ABaseline_RetainedStore(b *testing.B) {
	r := newBaselineRetainer(b)
	ctx := context.Background()
	props := map[string][]byte{
		"a2a-status":        []byte("online"),
		"a2a-status-source": []byte("broker"),
	}
	payload := []byte(`{"name":"agent","skills":[]}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := fmt.Sprintf("$a2a/v1/discovery/org/unit/agent-%d", i)
		if err := r.StoreRetainedWithProperties(ctx, topic, payload, 1, "org/unit/"+fmt.Sprintf("agent-%d", i), props); err != nil {
			b.Fatalf("store retained: %v", err)
		}
	}
	b.ReportMetric(float64(b.N), "retained-stores/op")
}

func BenchmarkA2ABaseline_RetainedLoad(b *testing.B) {
	r := newBaselineRetainer(b)
	ctx := context.Background()
	props := map[string][]byte{"a2a-status": []byte("online")}
	for i := 0; i < 200; i++ {
		topic := fmt.Sprintf("$a2a/v1/discovery/org/unit/agent-%d", i)
		if err := r.StoreRetainedWithProperties(ctx, topic, []byte("card"), 1, "org/unit/"+fmt.Sprintf("agent-%d", i), props); err != nil {
			b.Fatalf("seed retained: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, err := r.GetRetainedMessages(ctx, "$a2a/v1/discovery/+/+/+")
		if err != nil {
			b.Fatalf("get retained: %v", err)
		}
		if len(msgs) != 200 {
			b.Fatalf("expected 200 cards, got %d", len(msgs))
		}
	}
	b.ReportMetric(200, "cards/query")
}

func BenchmarkA2ABaseline_IdentityHook(b *testing.B) {
	h := hooks.NewA2AIdentityHook()
	c := &hooks.MessagePublishCtx{
		Topic:    "$a2a/v1/discovery/org/unit/agent-1",
		ClientID: "org/unit/agent-1",
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := h.OnMessagePublish(ctx, c)
		if err != nil || res == nil || !res.Allow {
			b.Fatalf("identity hook rejected valid registration")
		}
	}
}
