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

package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"github.com/turtacn/grovemq/pkg/storage"
	"github.com/turtacn/grovemq/pkg/storage/messages"
)

// ReplicatingStore is a storage.Store wrapper that forwards every write to the
// cluster so durable state is replicated to peers for high availability. Reads
// and the local write itself are served by the wrapped store; replication is
// best-effort and asynchronous.
type ReplicatingStore struct {
	inner storage.Store
	kind  string
	put   func(kind, key string, value []byte)
	del   func(kind, key string)
}

// NewReplicatingStore wraps inner and replicates its writes under the given
// kind. put/del are typically bound to a cluster Manager's ReplicateState /
// ReplicateDelete methods.
func NewReplicatingStore(inner storage.Store, kind string, put func(kind, key string, value []byte), del func(kind, key string)) *ReplicatingStore {
	return &ReplicatingStore{inner: inner, kind: kind, put: put, del: del}
}

// Get delegates to the wrapped store.
func (r *ReplicatingStore) Get(key string) (any, error) { return r.inner.Get(key) }

// Set writes locally and replicates the entry to peers.
func (r *ReplicatingStore) Set(key string, value any) error {
	if err := r.inner.Set(key, value); err != nil {
		return err
	}
	var buf bytes.Buffer
	if b, ok := value.([]byte); ok {
		buf.Write(b)
	} else if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	if r.put != nil {
		r.put(r.kind, key, buf.Bytes())
	}
	return nil
}

// Delete removes locally and replicates the deletion to peers.
func (r *ReplicatingStore) Delete(key string) error {
	if err := r.inner.Delete(key); err != nil {
		return err
	}
	if r.del != nil {
		r.del(r.kind, key)
	}
	return nil
}

// Close is a no-op; the underlying store is owned by the caller.
func (r *ReplicatingStore) Close() error { return nil }

// ReplicatingBackend is a messages.StorageBackend wrapper that replicates
// retained-message writes to peers. Only the retained-message operations are
// replicated; the durable stream operations are delegated unchanged.
type ReplicatingBackend struct {
	inner messages.StorageBackend
	put   func(kind, key string, value []byte)
	del   func(kind, key string)
}

// NewReplicatingBackend wraps inner and replicates retained writes under the
// "retained" kind.
func NewReplicatingBackend(inner messages.StorageBackend, put func(kind, key string, value []byte), del func(kind, key string)) *ReplicatingBackend {
	return &ReplicatingBackend{inner: inner, put: put, del: del}
}

// StoreRetained persists locally and replicates to peers.
func (b *ReplicatingBackend) StoreRetained(ctx context.Context, topic string, message messages.Message) error {
	if err := b.inner.StoreRetained(ctx, topic, message); err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(message); err != nil {
		return err
	}
	if b.put != nil {
		b.put("retained", topic, buf.Bytes())
	}
	return nil
}

// GetRetained delegates to the wrapped backend.
func (b *ReplicatingBackend) GetRetained(ctx context.Context, topicFilter string) ([]messages.Message, error) {
	return b.inner.GetRetained(ctx, topicFilter)
}

// DeleteRetained removes locally and replicates the deletion.
func (b *ReplicatingBackend) DeleteRetained(ctx context.Context, topic string) error {
	if err := b.inner.DeleteRetained(ctx, topic); err != nil {
		return err
	}
	if b.del != nil {
		b.del("retained", topic)
	}
	return nil
}

// AppendMessages delegates to the wrapped backend.
func (b *ReplicatingBackend) AppendMessages(ctx context.Context, streamID string, msgs []messages.Message) error {
	return b.inner.AppendMessages(ctx, streamID, msgs)
}

// GetStreams delegates to the wrapped backend.
func (b *ReplicatingBackend) GetStreams(ctx context.Context, topicFilter string, startTime time.Time) ([]messages.StreamInfo, error) {
	return b.inner.GetStreams(ctx, topicFilter, startTime)
}

// CreateIterator delegates to the wrapped backend.
func (b *ReplicatingBackend) CreateIterator(ctx context.Context, streamID string, topicFilter string, startTime time.Time) (messages.Iterator, error) {
	return b.inner.CreateIterator(ctx, streamID, topicFilter, startTime)
}

// Subscribe delegates to the wrapped backend.
func (b *ReplicatingBackend) Subscribe(ctx context.Context, streamID string, opts messages.SubscriptionOptions) (messages.Subscription, error) {
	return b.inner.Subscribe(ctx, streamID, opts)
}

// Stats delegates to the wrapped backend.
func (b *ReplicatingBackend) Stats() messages.StorageStats { return b.inner.Stats() }

// Close delegates to the wrapped backend.
func (b *ReplicatingBackend) Close() error { return b.inner.Close() }
