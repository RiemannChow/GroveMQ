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
	"sync"
	"time"
)

// rateLimitReason is the rejection reason returned when a publish exceeds the
// client's configured rate.
const rateLimitReason = "publish rate limit exceeded"

// tokenBucket is a minimal token-bucket rate limiter. It is not safe for
// concurrent use on its own; RateLimitHook guards it with a mutex.
type tokenBucket struct {
	rate    float64 // tokens replenished per second
	burst   float64 // maximum tokens the bucket can hold
	tokens  float64 // current tokens
	updated time.Time
}

// allow attempts to consume one token, replenishing based on elapsed time since
// the last call. It returns true if a token was available.
func (b *tokenBucket) allow(now time.Time) bool {
	elapsed := now.Sub(b.updated).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.updated = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// RateLimitHook enforces a per-client publish rate limit using token buckets.
// It is INERT by default (rate 0 means unlimited), so registering it has no
// effect until a default or per-client rate is configured.
type RateLimitHook struct {
	BaseHook
	mu           sync.Mutex
	defaultRate  float64
	defaultBurst int
	buckets      map[string]*tokenBucket
}

// NewRateLimitHook returns a rate-limit hook with no limit configured.
func NewRateLimitHook() *RateLimitHook {
	return &RateLimitHook{
		defaultRate:  0,
		defaultBurst: 0,
		buckets:      make(map[string]*tokenBucket),
	}
}

// SetDefaultRate sets the rate (tokens/sec) and burst applied to clients that
// have no explicit per-client rate. rate <= 0 disables limiting entirely.
func (h *RateLimitHook) SetDefaultRate(rate float64, burst int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	h.defaultRate = rate
	h.defaultBurst = burst
}

// SetClientRate sets an explicit rate (tokens/sec) and burst for a single client.
func (h *RateLimitHook) SetClientRate(clientID string, rate float64, burst int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rate < 0 {
		rate = 0
	}
	if burst < 0 {
		burst = 0
	}
	h.buckets[clientID] = &tokenBucket{
		rate:    rate,
		burst:   float64(burst),
		tokens:  float64(burst),
		updated: time.Now(),
	}
}

// Clear removes any explicit rate for a client, reverting it to the default.
func (h *RateLimitHook) Clear(clientID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.buckets, clientID)
}

// Name implements Hook.
func (h *RateLimitHook) Name() string { return "rate_limit" }

// Points reports the points this hook implements (it only ever acts on publish).
func (h *RateLimitHook) Points() []string {
	return []string{OnMessagePublishPoint}
}

// OnMessagePublish is the decision hook: it allows the publish if the client is
// within its rate budget and rejects (Allow=false) otherwise.
func (h *RateLimitHook) OnMessagePublish(ctx context.Context, c *MessagePublishCtx) (*MessagePublishResult, error) {
	h.mu.Lock()

	// No default limit: only explicit per-client buckets are enforced.
	if h.defaultRate <= 0 && h.defaultBurst <= 0 {
		bucket, ok := h.buckets[c.ClientID]
		if !ok {
			h.mu.Unlock()
			return &MessagePublishResult{Allow: true}, nil
		}
		now := time.Now()
		allowed := bucket.allow(now)
		h.mu.Unlock()
		if !allowed {
			return &MessagePublishResult{Allow: false, Reason: rateLimitReason}, nil
		}
		return &MessagePublishResult{Allow: true}, nil
	}

	// A default limit is configured: lazily create and PERSIST a per-client
	// bucket seeded from the default rate/burst so the budget is tracked across
	// calls (an explicit per-client rate overrides the default).
	bucket, ok := h.buckets[c.ClientID]
	if !ok {
		bucket = &tokenBucket{
			rate:    h.defaultRate,
			burst:   float64(h.defaultBurst),
			tokens:  float64(h.defaultBurst),
			updated: time.Now(),
		}
		h.buckets[c.ClientID] = bucket
	}
	now := time.Now()
	allowed := bucket.allow(now)
	h.mu.Unlock()
	if !allowed {
		return &MessagePublishResult{Allow: false, Reason: rateLimitReason}, nil
	}
	return &MessagePublishResult{Allow: true}, nil
}
