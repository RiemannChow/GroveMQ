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

// Package bolt provides a durable, embedded key-value implementation of the
// storage.Store interface backed by bbolt. It gives GroveMQ local persistence so
// that MQTT session state, offline message queues and will messages survive a
// broker restart. Values are encoded with encoding/gob, which round-trips the
// []byte values used throughout the codebase (e.g. json-marshalled sessions).
package bolt

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"

	"github.com/turtacn/grovemq/pkg/storage"
	bolt "go.etcd.io/bbolt"
)

// bucket is the single bbolt bucket used for all key/value entries.
var bucket = []byte("grovemq_state")

// BoltStore is a persistent implementation of storage.Store backed by bbolt.
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens (or creates) a bbolt database rooted at dir. The database
// file is named state.db inside dir, and dir is created if it does not exist.
func NewBoltStore(dir string) (*BoltStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "state.db"), 0o600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucket)
		return e
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &BoltStore{db: db}, nil
}

// Get retrieves a value by key. It returns storage.ErrNotFound when the key is
// absent. Values stored as raw []byte (the common case for the persistent
// session manager, which persists JSON) are returned verbatim as a []byte;
// gob-encoded values stored by other callers are decoded, with a raw-bytes
// fallback so that a []byte stored without gob wrapping is returned unchanged.
func (s *BoltStore) Get(key string) (any, error) {
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return storage.ErrNotFound
		}
		v := b.Get([]byte(key))
		if v == nil {
			return storage.ErrNotFound
		}
		// Copy out of the bolt-managed slice which is only valid for the tx.
		raw = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Attempt gob decode (for non-[]byte values). On failure (e.g. the value is
	// raw JSON bytes, not gob), fall back to returning the bytes as-is.
	var value any
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&value); err == nil {
		return value, nil
	}
	return raw, nil
}

// Set adds or updates a key/value pair. []byte values are stored verbatim so
// callers can type-assert them back; all other values are gob-encoded.
func (s *BoltStore) Set(key string, value any) error {
	var buf bytes.Buffer
	if b, ok := value.([]byte); ok {
		buf.Write(b)
	} else if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return storage.ErrNotFound
		}
		return b.Put([]byte(key), buf.Bytes())
	})
}

// Delete removes a key. Deleting a non-existent key is a no-op.
func (s *BoltStore) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
}

// ForEach iterates over every stored entry, calling fn with the raw gob-encoded
// bytes. It is used by the cluster state synchronisation path to export local
// state to a joining peer.
func (s *BoltStore) ForEach(fn func(key string, value []byte) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			return fn(string(k), append([]byte(nil), v...))
		})
	})
}

// Close releases the underlying database.
func (s *BoltStore) Close() error {
	return s.db.Close()
}
