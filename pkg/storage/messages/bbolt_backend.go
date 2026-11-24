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

package messages

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// retainedBucket holds the retained messages, one per topic (MQTT semantics).
var retainedBucket = []byte("grovemq_retained")

// BoltBackend is a durable implementation of StorageBackend backed by bbolt.
// It provides persistent retained-message storage and no-op stream storage
// (the durable message-stream feature is out of scope for the persistence PoC).
type BoltBackend struct {
	db  *bolt.DB
	mu  sync.RWMutex
	dir string
}

// NewBoltBackend opens (or creates) a bbolt database at dir/retained.db.
func NewBoltBackend(dir string) (*BoltBackend, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "retained.db"), 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(retainedBucket)
		return e
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &BoltBackend{db: db, dir: dir}, nil
}

// StoreRetained persists (or replaces) the retained message for a topic.
func (b *BoltBackend) StoreRetained(ctx context.Context, topic string, message Message) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(message); err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(retainedBucket)
		if bk == nil {
			return nil
		}
		return bk.Put([]byte(topic), buf.Bytes())
	})
}

// GetRetained returns all retained messages whose topic matches topicFilter.
func (b *BoltBackend) GetRetained(ctx context.Context, topicFilter string) ([]Message, error) {
	out := make([]Message, 0)
	err := b.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket(retainedBucket)
		if bk == nil {
			return nil
		}
		return bk.ForEach(func(k, v []byte) error {
			topic := string(k)
			if !matchTopicFilter(topic, topicFilter) {
				return nil
			}
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&msg); err != nil {
				return err
			}
			out = append(out, msg)
			return nil
		})
	})
	return out, err
}

// DeleteRetained removes the retained message for a topic.
func (b *BoltBackend) DeleteRetained(ctx context.Context, topic string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(retainedBucket)
		if bk == nil {
			return nil
		}
		return bk.Delete([]byte(topic))
	})
}

// AllRetained returns every retained message. It is used by the cluster state
// synchronisation path to export local retained messages to a joining peer.
func (b *BoltBackend) AllRetained(ctx context.Context) ([]Message, error) {
	out := make([]Message, 0)
	err := b.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket(retainedBucket)
		if bk == nil {
			return nil
		}
		return bk.ForEach(func(_, v []byte) error {
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&msg); err != nil {
				return err
			}
			out = append(out, msg)
			return nil
		})
	})
	return out, err
}

// --- Stream storage: minimal no-op implementation ---

// AppendMessages is a no-op for the persistence PoC (durable message streams are
// not part of the in-memory hot path used by the broker).
func (b *BoltBackend) AppendMessages(ctx context.Context, streamID string, messages []Message) error {
	return nil
}

// GetStreams returns no streams for the persistence PoC.
func (b *BoltBackend) GetStreams(ctx context.Context, topicFilter string, startTime time.Time) ([]StreamInfo, error) {
	return nil, nil
}

// CreateIterator returns an iterator with no messages.
func (b *BoltBackend) CreateIterator(ctx context.Context, streamID string, topicFilter string, startTime time.Time) (Iterator, error) {
	return &emptyIterator{}, nil
}

// Subscribe returns a no-op subscription.
func (b *BoltBackend) Subscribe(ctx context.Context, streamID string, opts SubscriptionOptions) (Subscription, error) {
	return &noOpSubscription{}, nil
}

// Stats returns storage statistics for the retained backend.
func (b *BoltBackend) Stats() StorageStats {
	count := uint64(0)
	var size uint64
	_ = b.db.View(func(tx *bolt.Tx) error {
		bk := tx.Bucket(retainedBucket)
		if bk == nil {
			return nil
		}
		return bk.ForEach(func(_, v []byte) error {
			count++
			size += uint64(len(v))
			return nil
		})
	})
	return StorageStats{
		RetainedMessages: count,
		StorageSize:      size,
		BackendType:      "bolt",
	}
}

// Close releases the underlying database.
func (b *BoltBackend) Close() error {
	return b.db.Close()
}

// emptyIterator is a no-op Iterator returned when stream storage is not used.
type emptyIterator struct{}

func (e *emptyIterator) Next(ctx context.Context, limit int) ([]Message, error) { return nil, nil }
func (e *emptyIterator) HasMore() bool                                          { return false }
func (e *emptyIterator) Close() error                                           { return nil }

// noOpSubscription is a no-op Subscription.
type noOpSubscription struct{}

func (n *noOpSubscription) Messages() <-chan []Message { return make(chan []Message) }
func (n *noOpSubscription) Ack(seqno uint64) error     { return nil }
func (n *noOpSubscription) Close() error               { return nil }

// matchTopicFilter implements basic MQTT topic-filter matching (exact, '#', '+')
// without importing the retainer package (which would create an import cycle).
func matchTopicFilter(topic, filter string) bool {
	if !reservedTopicAllowed(topic, filter) {
		return false
	}
	if topic == filter {
		return true
	}
	if filter == "#" {
		return true
	}
	tLevels := strings.Split(topic, "/")
	fLevels := strings.Split(filter, "/")
	if len(fLevels) > 0 && fLevels[len(fLevels)-1] == "#" {
		for i := 0; i < len(fLevels)-1; i++ {
			if i >= len(tLevels) {
				return false
			}
			if fLevels[i] != "+" && fLevels[i] != tLevels[i] {
				return false
			}
		}
		return true
	}
	if len(tLevels) != len(fLevels) {
		return false
	}
	for i := 0; i < len(fLevels); i++ {
		if fLevels[i] == "+" {
			continue
		}
		if fLevels[i] != tLevels[i] {
			return false
		}
	}
	return true
}
