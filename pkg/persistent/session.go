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

// Package persistent provides persistent session management for MQTT clients
// with support for offline message queuing, will messages, and QoS acknowledgments.
package persistent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/turtacn/grovemq/pkg/metrics"
	"github.com/turtacn/grovemq/pkg/storage"
)

// PersistentSession represents a durable MQTT client session that survives
// disconnections when CleanSession=false
type PersistentSession struct {
	// Basic session information
	ClientID       string        `json:"client_id"`
	CleanSession   bool          `json:"clean_session"`
	Connected      bool          `json:"connected"`
	CreatedAt      time.Time     `json:"created_at"`
	LastActivity   time.Time     `json:"last_activity"`
	ExpiryInterval time.Duration `json:"expiry_interval"`

	// Subscriptions: topic -> QoS
	Subscriptions map[string]byte `json:"subscriptions"`

	// Packet ID management
	PacketIDCounter uint16 `json:"packet_id_counter"`

	// Inflight messages waiting for acknowledgment
	InflightQoS1 map[uint16]*InflightMessage `json:"inflight_qos1"`
	InflightQoS2 map[uint16]*InflightMessage `json:"inflight_qos2"`

	// Offline message queue
	MessageQueue []*QueuedMessage `json:"message_queue"`

	// Will message configuration
	WillMessage *WillMessage `json:"will_message,omitempty"`

	// Synchronization
	mu sync.RWMutex `json:"-"`

	// dirty indicates the session has unsaved changes (offline queue, inflight,
	// etc.) that the periodic flush routine should persist. It is not serialized.
	dirty bool `json:"-"`
}

// InflightMessage represents a message waiting for acknowledgment
type InflightMessage struct {
	PacketID   uint16            `json:"packet_id"`
	Topic      string            `json:"topic"`
	Payload    []byte            `json:"payload"`
	QoS        byte              `json:"qos"`
	Retain     bool              `json:"retain"`
	Timestamp  time.Time         `json:"timestamp"`
	RetryCount int               `json:"retry_count"`
	Headers    map[string]string `json:"headers,omitempty"`
	// PubrelSent is set once the broker has sent PUBREL for a QoS 2 message
	// (after the client's PUBREC). It lets the retry path resend PUBREL rather
	// than the full PUBLISH on subsequent retries.
	PubrelSent bool `json:"pubrel_sent"`
}

// QueuedMessage represents an offline message waiting to be delivered
type QueuedMessage struct {
	Topic      string            `json:"topic"`
	Payload    []byte            `json:"payload"`
	QoS        byte              `json:"qos"`
	Retain     bool              `json:"retain"`
	Timestamp  time.Time         `json:"timestamp"`
	ExpiryTime *time.Time        `json:"expiry_time,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// WillMessage represents the last will and testament message
type WillMessage struct {
	Topic         string        `json:"topic"`
	Payload       []byte        `json:"payload"`
	QoS           byte          `json:"qos"`
	Retain        bool          `json:"retain"`
	DelayInterval time.Duration `json:"delay_interval"`
}

// Config defines configuration for persistent session management
type Config struct {
	// Maximum number of messages to queue for offline clients
	MaxOfflineMessages uint64 `json:"max_offline_messages"`

	// Maximum time to keep messages in offline queue
	MessageExpiryInterval time.Duration `json:"message_expiry_interval"`

	// Default session expiry interval when not specified by client
	DefaultSessionExpiry time.Duration `json:"default_session_expiry"`

	// Interval for retrying unacknowledged QoS 1/2 messages
	RetryInterval time.Duration `json:"retry_interval"`

	// Maximum number of retry attempts for QoS 1/2 messages
	MaxRetryAttempts int `json:"max_retry_attempts"`

	// Cleanup interval for expired sessions and messages
	CleanupInterval time.Duration `json:"cleanup_interval"`

	// Maximum number of inflight messages per session
	MaxInflightMessages uint16 `json:"max_inflight_messages"`
}

// DefaultConfig returns a default configuration for persistent sessions
func DefaultConfig() *Config {
	return &Config{
		MaxOfflineMessages:    10000,
		MessageExpiryInterval: 24 * time.Hour,
		DefaultSessionExpiry:  24 * time.Hour,
		RetryInterval:         30 * time.Second,
		MaxRetryAttempts:      3,
		CleanupInterval:       5 * time.Minute,
		MaxInflightMessages:   100,
	}
}

// SessionManager manages persistent sessions for MQTT clients
type SessionManager struct {
	store  storage.Store
	config *Config

	// Active sessions (connected clients)
	activeSessions sync.Map // map[string]*PersistentSession

	// Will message management
	willManager *WillMessageManager

	// Cleanup and retry management
	cleanupTicker *time.Ticker
	retryTicker   *time.Ticker
	flushTicker   *time.Ticker
	stopCleanup   chan struct{}
	stopRetry     chan struct{}
	stopFlush     chan struct{}
	closed        bool

	// retryCallback is invoked to redeliver an unacknowledged QoS 1/2 message to
	// its subscriber. It is injected by the broker (which owns the session
	// mailboxes) and may be nil if no broker is wired up.
	retryCallback func(clientID, topic string, payload []byte, qos byte, retain bool, packetID uint16) error
	retryMu       sync.RWMutex

	mu sync.RWMutex
}

// SetRetryCallback configures the function used to redeliver inflight messages.
// It must be set before the retry routine begins delivering (the broker sets it
// immediately after NewSessionManager).
func (sm *SessionManager) SetRetryCallback(cb func(clientID, topic string, payload []byte, qos byte, retain bool, packetID uint16) error) {
	sm.retryMu.Lock()
	defer sm.retryMu.Unlock()
	sm.retryCallback = cb
}

// NewSessionManager creates a new persistent session manager
func NewSessionManager(store storage.Store, config *Config) *SessionManager {
	if config == nil {
		config = DefaultConfig()
	}

	sm := &SessionManager{
		store:       store,
		config:      config,
		stopCleanup: make(chan struct{}),
		stopRetry:   make(chan struct{}),
		stopFlush:   make(chan struct{}),
	}

	// Initialize will message manager with a placeholder publisher
	// The actual publisher will be set by the broker
	sm.willManager = NewWillMessageManager(nil)

	// Start background routines
	sm.startCleanupRoutine()
	sm.startRetryRoutine()
	sm.startFlushRoutine()

	return sm
}

// SetWillMessagePublisher sets the publisher for will messages
func (sm *SessionManager) SetWillMessagePublisher(publisher WillMessagePublisher) {
	sm.willManager.publisher = publisher
}

// CreateOrResumeSession creates a new session or resumes an existing persistent session
func (sm *SessionManager) CreateOrResumeSession(clientID string, cleanSession bool, expiryInterval time.Duration) (*PersistentSession, error) {

	now := time.Now()

	// Check if session already exists in active sessions
	if existing, exists := sm.activeSessions.Load(clientID); exists {
		activeSession := existing.(*PersistentSession)
		activeSession.mu.Lock()
		activeSession.Connected = true
		activeSession.LastActivity = now
		if cleanSession {
			// Clean session requested, clear all state and update clean session flag
			activeSession.CleanSession = true
			activeSession.Subscriptions = make(map[string]byte)
			activeSession.MessageQueue = nil
			activeSession.InflightQoS1 = make(map[uint16]*InflightMessage)
			activeSession.InflightQoS2 = make(map[uint16]*InflightMessage)
			activeSession.WillMessage = nil
		}
		activeSession.mu.Unlock()

		log.Printf("[INFO] Resumed active session for client %s (cleanSession: %t)", clientID, cleanSession)
		return activeSession, nil
	}

	// Try to load persistent session from storage
	if !cleanSession {
		if session, err := sm.loadSession(clientID); err == nil {
			// Resume existing persistent session
			session.Connected = true
			session.LastActivity = now
			if expiryInterval > 0 {
				session.ExpiryInterval = expiryInterval
			}

			sm.activeSessions.Store(clientID, session)
			log.Printf("[INFO] Resumed persistent session for client %s (offline messages: %d, subscriptions: %d)",
				clientID, len(session.MessageQueue), len(session.Subscriptions))
			return session, nil
		}
	}

	// Create new session
	session := &PersistentSession{
		ClientID:        clientID,
		CleanSession:    cleanSession,
		Connected:       true,
		CreatedAt:       now,
		LastActivity:    now,
		ExpiryInterval:  expiryInterval,
		Subscriptions:   make(map[string]byte),
		PacketIDCounter: 0,
		InflightQoS1:    make(map[uint16]*InflightMessage),
		InflightQoS2:    make(map[uint16]*InflightMessage),
		MessageQueue:    nil,
	}

	if session.ExpiryInterval == 0 {
		session.ExpiryInterval = sm.config.DefaultSessionExpiry
	}
	if expiryInterval > 0 {
		session.ExpiryInterval = expiryInterval
	}

	if existing, loaded := sm.activeSessions.LoadOrStore(clientID, session); loaded {
		// Another goroutine created the session concurrently. Reuse the existing one.
		activeSession := existing.(*PersistentSession)
		activeSession.mu.Lock()
		activeSession.Connected = true
		activeSession.LastActivity = now
		if cleanSession {
			activeSession.CleanSession = true
			activeSession.Subscriptions = make(map[string]byte)
			activeSession.MessageQueue = nil
			activeSession.InflightQoS1 = make(map[uint16]*InflightMessage)
			activeSession.InflightQoS2 = make(map[uint16]*InflightMessage)
			activeSession.WillMessage = nil
		}
		activeSession.mu.Unlock()

		log.Printf("[INFO] Resumed active session for client %s (cleanSession: %t)", clientID, cleanSession)
		return activeSession, nil
	}

	// Save to persistent storage if not clean session
	if !cleanSession {
		if err := sm.saveSession(session); err != nil {
			log.Printf("[ERROR] Failed to save new persistent session for %s: %v", clientID, err)
		}
	}

	log.Printf("[INFO] Created new session for client %s (cleanSession: %t)", clientID, cleanSession)
	return session, nil
}

// DisconnectSession marks a session as disconnected and handles will message
func (sm *SessionManager) DisconnectSession(clientID string, graceful bool) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	session.Connected = false
	session.LastActivity = time.Now()

	// Handle will message if disconnection was not graceful
	if !graceful && session.WillMessage != nil {
		log.Printf("[INFO] Triggering will message for client %s due to abnormal disconnection", clientID)

		// Publish will message using the will manager
		ctx := context.Background()
		if err := sm.willManager.PublishWillMessage(ctx, session.WillMessage, clientID); err != nil {
			log.Printf("[ERROR] Failed to publish will message for client %s: %v", clientID, err)
		}
	} else if graceful {
		// Cancel any scheduled will messages for graceful disconnection
		sm.willManager.CancelWillMessage(clientID)
		log.Printf("[INFO] Cancelled scheduled will messages for client %s (graceful disconnect)", clientID)
	}

	// Clear will message after disconnection (graceful or after publishing)
	session.WillMessage = nil

	// If clean session, remove from active sessions and storage
	if session.CleanSession {
		sm.activeSessions.Delete(clientID)
		sm.deleteSession(clientID)
		log.Printf("[INFO] Removed clean session for client %s", clientID)
	} else {
		// Save persistent session state (session.mu is held by this function).
		if err := sm.saveSessionLocked(session); err != nil {
			log.Printf("[ERROR] Failed to save session state for %s: %v", clientID, err)
		}
		log.Printf("[INFO] Saved persistent session for client %s (offline messages: %d)", clientID, len(session.MessageQueue))
	}

	return nil
}

// GetSession returns the active session for clientID, or nil if there is none.
func (sm *SessionManager) GetSession(clientID string) *PersistentSession {
	if v, ok := sm.activeSessions.Load(clientID); ok {
		return v.(*PersistentSession)
	}
	return nil
}

// ForEachSession invokes fn for every active session (connected or persisted).
func (sm *SessionManager) ForEachSession(fn func(*PersistentSession)) {
	sm.activeSessions.Range(func(_, v interface{}) bool {
		if ps, ok := v.(*PersistentSession); ok {
			fn(ps)
		}
		return true
	})
}

// AddSubscription adds a subscription to the session
func (sm *SessionManager) AddSubscription(clientID, topic string, qos byte) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	session.Subscriptions[topic] = qos
	session.LastActivity = time.Now()

	// Save to storage if persistent session (session.mu is held by this function).
	if !session.CleanSession {
		if err := sm.saveSessionLocked(session); err != nil {
			log.Printf("[ERROR] Failed to save subscription for %s: %v", clientID, err)
		}
	}

	log.Printf("[INFO] Added subscription for client %s: topic=%s, qos=%d", clientID, topic, qos)
	return nil
}

// RemoveSubscription removes a subscription from the session
func (sm *SessionManager) RemoveSubscription(clientID, topic string) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	delete(session.Subscriptions, topic)
	session.LastActivity = time.Now()

	// Save to storage if persistent session (session.mu is held by this function).
	if !session.CleanSession {
		if err := sm.saveSessionLocked(session); err != nil {
			log.Printf("[ERROR] Failed to save unsubscription for %s: %v", clientID, err)
		}
	}

	log.Printf("[INFO] Removed subscription for client %s: topic=%s", clientID, topic)
	return nil
}

// SetWillMessage sets the will message for a session
func (sm *SessionManager) SetWillMessage(clientID, topic string, payload []byte, qos byte, retain bool, delayInterval time.Duration) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	session.WillMessage = &WillMessage{
		Topic:         topic,
		Payload:       payload,
		QoS:           qos,
		Retain:        retain,
		DelayInterval: delayInterval,
	}

	log.Printf("[INFO] Set will message for client %s: topic=%s", clientID, topic)
	return nil
}

// QueueMessage queues a message for offline delivery
func (sm *SessionManager) QueueMessage(clientID, topic string, payload []byte, qos byte, retain bool) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	// Don't queue messages for connected clients
	if session.Connected {
		return nil
	}

	// Don't queue messages for clean sessions
	if session.CleanSession {
		return nil
	}

	// Check queue size limit
	if uint64(len(session.MessageQueue)) >= sm.config.MaxOfflineMessages {
		// Remove oldest message to make room
		session.MessageQueue = session.MessageQueue[1:]
		log.Printf("[WARN] Offline message queue full for client %s, dropping oldest message", clientID)
	}

	// Create queued message
	msg := &QueuedMessage{
		Topic:     topic,
		Payload:   payload,
		QoS:       qos,
		Retain:    retain,
		Timestamp: time.Now(),
	}

	// Set expiry time if configured
	if sm.config.MessageExpiryInterval > 0 {
		expiryTime := msg.Timestamp.Add(sm.config.MessageExpiryInterval)
		msg.ExpiryTime = &expiryTime
	}

	session.MessageQueue = append(session.MessageQueue, msg)

	// Mark the session dirty instead of re-marshaling and persisting the whole
	// session (queue up to MaxOfflineMessages entries) on EVERY queued publish.
	// Re-marshaling per message is O(n) per message -> O(n^2) total, which
	// stalls the broker under heavy offline load. The periodic flush routine
	// (startFlushRoutine) persists dirty sessions once per interval. The
	// in-memory store is a no-op on save anyway, so this is safe for both
	// backends; the only trade-off is losing at most ~1s of queued offline
	// messages on a crash (durable backend only -- the in-memory backend never
	// survives a crash regardless).
	session.dirty = true

	log.Printf("[INFO] Queued offline message for client %s: topic=%s, queue_size=%d", clientID, topic, len(session.MessageQueue))
	return nil
}

// NextPacketID generates the next packet ID for QoS 1/2 messages
func (sm *SessionManager) NextPacketID(clientID string) (uint16, error) {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return 0, fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	// Increment packet ID (skip 0)
	session.PacketIDCounter++
	if session.PacketIDCounter == 0 {
		session.PacketIDCounter = 1
	}

	return session.PacketIDCounter, nil
}

// StoreOutboundInflight records a QoS 1/2 message that has been handed to a
// subscriber's session for delivery, so it can be retransmitted until the
// client acknowledges it. It enforces MaxInflightMessages per session; if the
// limit is reached it returns an error and the caller should treat the message
// as dropped (not delivered).
func (sm *SessionManager) StoreOutboundInflight(clientID string, msg *InflightMessage) error {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return fmt.Errorf("session not found for client %s", clientID)
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	if len(session.InflightQoS1)+len(session.InflightQoS2) >= int(sm.config.MaxInflightMessages) {
		return fmt.Errorf("max inflight messages (%d) exceeded for client %s", sm.config.MaxInflightMessages, clientID)
	}

	msg.Timestamp = time.Now()
	if msg.QoS == 2 {
		if session.InflightQoS2 == nil {
			session.InflightQoS2 = make(map[uint16]*InflightMessage)
		}
		session.InflightQoS2[msg.PacketID] = msg
	} else {
		if session.InflightQoS1 == nil {
			session.InflightQoS1 = make(map[uint16]*InflightMessage)
		}
		session.InflightQoS1[msg.PacketID] = msg
	}
	session.dirty = true
	return nil
}

// RemoveInflight removes a QoS 1/2 inflight message once the client has
// acknowledged it (PUBACK for QoS 1, PUBCOMP for QoS 2). PUBREC is handled
// separately via MarkInflightPubrelSent. It is safe to call for unknown packet
// IDs (duplicate/out-of-order acks are tolerated).
func (sm *SessionManager) RemoveInflight(clientID string, packetID uint16, qos byte) {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	if qos == 2 {
		delete(session.InflightQoS2, packetID)
	} else {
		delete(session.InflightQoS1, packetID)
	}
	session.dirty = true
}

// CountInflight returns the number of outstanding QoS 1/2 messages awaiting
// acknowledgement for a client. It is used primarily for testing/observability.
func (sm *SessionManager) CountInflight(clientID string) int {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return 0
	}
	session := value.(*PersistentSession)
	session.mu.RLock()
	defer session.mu.RUnlock()
	return len(session.InflightQoS1) + len(session.InflightQoS2)
}

// MarkInflightPubrelSent records that the broker has sent PUBREL for a QoS 2
// inflight message (in response to the client's PUBREC). Subsequent retries
// resend PUBREL instead of the full PUBLISH.
func (sm *SessionManager) MarkInflightPubrelSent(clientID string, packetID uint16) {
	value, exists := sm.activeSessions.Load(clientID)
	if !exists {
		return
	}
	session := value.(*PersistentSession)

	session.mu.Lock()
	defer session.mu.Unlock()

	if msg, ok := session.InflightQoS2[packetID]; ok {
		msg.PubrelSent = true
		msg.Timestamp = time.Now()
		session.dirty = true
	}
}

// Close shuts down the session manager
func (sm *SessionManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Check if already closed
	if sm.closed {
		return nil
	}
	sm.closed = true

	if sm.cleanupTicker != nil {
		sm.cleanupTicker.Stop()
		close(sm.stopCleanup)
	}
	if sm.retryTicker != nil {
		sm.retryTicker.Stop()
		close(sm.stopRetry)
	}
	if sm.flushTicker != nil {
		sm.flushTicker.Stop()
		close(sm.stopFlush)
	}

	// Close will message manager
	if sm.willManager != nil {
		sm.willManager.Close()
	}

	// Save all active sessions
	sm.activeSessions.Range(func(_, value any) bool {
		session := value.(*PersistentSession)
		if !session.CleanSession {
			if err := sm.saveSession(session); err != nil {
				log.Printf("[ERROR] Failed to save session %s during shutdown: %v", session.ClientID, err)
			}
		}
		return true
	})

	return nil
}

// Private helper methods

// saveSession persists a session. It takes the session's read lock itself so
// that it is safe to call from code paths that do not already hold the lock
// (e.g. Close or CreateOrResumeSession). Callers that already hold session.mu
// must use saveSessionLocked instead, since sync.RWMutex is not reentrant.
func (sm *SessionManager) saveSession(session *PersistentSession) error {
	session.mu.RLock()
	defer session.mu.RUnlock()
	return sm.saveSessionLocked(session)
}

// saveSessionLocked persists a session whose lock is already held by the
// caller. Marshaling a session touches its maps and slices, so it must never
// run concurrently with a writer; holding the lock here is what keeps startup
// and shutdown saves from racing with subscription and queue updates.
func (sm *SessionManager) saveSessionLocked(session *PersistentSession) error {
	key := fmt.Sprintf("session:%s", session.ClientID)
	data, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}
	return sm.store.Set(key, data)
}

func (sm *SessionManager) loadSession(clientID string) (*PersistentSession, error) {
	key := fmt.Sprintf("session:%s", clientID)
	data, err := sm.store.Get(key)
	if err != nil {
		return nil, err
	}

	var session PersistentSession
	if err := json.Unmarshal(data.([]byte), &session); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}

	return &session, nil
}

func (sm *SessionManager) deleteSession(clientID string) error {
	key := fmt.Sprintf("session:%s", clientID)
	return sm.store.Delete(key)
}

func (sm *SessionManager) startCleanupRoutine() {
	sm.cleanupTicker = time.NewTicker(sm.config.CleanupInterval)

	go func() {
		for {
			select {
			case <-sm.cleanupTicker.C:
				sm.cleanupExpiredSessions()
			case <-sm.stopCleanup:
				return
			}
		}
	}()

	log.Printf("[INFO] Started session cleanup routine with interval: %v", sm.config.CleanupInterval)
}

func (sm *SessionManager) startRetryRoutine() {
	sm.retryTicker = time.NewTicker(sm.config.RetryInterval)

	go func() {
		for {
			select {
			case <-sm.retryTicker.C:
				sm.retryInflightMessages()
			case <-sm.stopRetry:
				return
			}
		}
	}()

	log.Printf("[INFO] Started message retry routine with interval: %v", sm.config.RetryInterval)
}

// flushInterval controls how often dirty sessions are persisted. It bounds the
// crash-durability window for offline queues / inflight state to ~1s while
// avoiding the O(n^2) per-message re-marshaling that QueueMessage previously
// performed.
const flushInterval = 1 * time.Second

// startFlushRoutine periodically persists sessions that have been marked dirty
// (by QueueMessage and the inflight bookkeeping) instead of re-marshaling the
// entire session on every change. This is what eliminates the O(n^2) offline
// queue persistence cost under heavy offline load.
func (sm *SessionManager) startFlushRoutine() {
	sm.flushTicker = time.NewTicker(flushInterval)

	go func() {
		for {
			select {
			case <-sm.flushTicker.C:
				sm.flushDirtySessions()
			case <-sm.stopFlush:
				return
			}
		}
	}()

	log.Printf("[INFO] Started session flush routine with interval: %v", flushInterval)
}

// flushDirtySessions persists every session flagged dirty, then clears the flag.
// Marshaling is done under the session lock to avoid racing with concurrent
// modifications; the (potentially slow) store write happens outside the lock.
// The in-memory store is a no-op on Set, so this is effectively free there.
func (sm *SessionManager) flushDirtySessions() {
	sm.activeSessions.Range(func(_, value any) bool {
		session := value.(*PersistentSession)
		session.mu.Lock()
		if !session.dirty {
			session.mu.Unlock()
			return true
		}
		session.dirty = false
		data, mErr := json.Marshal(session)
		session.mu.Unlock()
		if mErr != nil {
			log.Printf("[ERROR] Failed to marshal session %s: %v", session.ClientID, mErr)
			session.mu.Lock()
			session.dirty = true
			session.mu.Unlock()
			return true
		}
		if err := sm.store.Set(fmt.Sprintf("session:%s", session.ClientID), data); err != nil {
			log.Printf("[ERROR] Failed to flush session %s: %v", session.ClientID, err)
			session.mu.Lock()
			session.dirty = true
			session.mu.Unlock()
		}
		return true
	})
}

func (sm *SessionManager) cleanupExpiredSessions() {
	now := time.Now()

	// Take a snapshot of current sessions to avoid holding the write lock for long.
	snapshot := make([]*PersistentSession, 0)
	sm.activeSessions.Range(func(_, value any) bool {
		session := value.(*PersistentSession)
		snapshot = append(snapshot, session)
		return true
	})

	var expiredClients []string

	for _, session := range snapshot {
		session.mu.Lock()

		// Remove expired offline sessions after we finish scanning to minimize time holding the write lock.
		if !session.Connected && now.Sub(session.LastActivity) > session.ExpiryInterval {
			expiredClients = append(expiredClients, session.ClientID)
			session.mu.Unlock()
			continue
		}

		// Clean up expired messages in queue
		var validMessages []*QueuedMessage
		for _, msg := range session.MessageQueue {
			if msg.ExpiryTime == nil || now.Before(*msg.ExpiryTime) {
				validMessages = append(validMessages, msg)
			}
		}

		if len(validMessages) != len(session.MessageQueue) {
			session.MessageQueue = validMessages
			if !session.CleanSession {
				if err := sm.saveSessionLocked(session); err != nil {
					log.Printf("[ERROR] Failed to save session while cleaning up messages for %s: %v", session.ClientID, err)
				}
			}
			log.Printf("[INFO] Cleaned up expired messages for client %s", session.ClientID)
		}

		session.mu.Unlock()
	}

	if len(expiredClients) == 0 {
		return
	}

	// Remove expired sessions while holding the write lock to ensure map consistency.
	sm.mu.Lock()
	for _, clientID := range expiredClients {
		value, exists := sm.activeSessions.Load(clientID)
		if !exists {
			continue
		}
		session := value.(*PersistentSession)

		session.mu.Lock()
		if !session.Connected && now.Sub(session.LastActivity) > session.ExpiryInterval {
			sm.activeSessions.Delete(clientID)
			sm.deleteSession(clientID)
			log.Printf("[INFO] Cleaned up expired session for client %s", clientID)
		}
		session.mu.Unlock()
	}
	sm.mu.Unlock()
}

func (sm *SessionManager) retryInflightMessages() {
	sm.retryMu.RLock()
	cb := sm.retryCallback
	sm.retryMu.RUnlock()
	if cb == nil {
		return
	}

	now := time.Now()
	retryInterval := sm.config.RetryInterval
	if retryInterval <= 0 {
		retryInterval = 30 * time.Second
	}

	// Snapshot inflight messages that are due for a retry. We copy out the
	// minimal data we need under each session's lock to avoid holding it while
	// invoking the (potentially slow) redelivery callback.
	type pending struct {
		clientID string
		msg      *InflightMessage
	}
	var due []pending
	var totalInflight int

	sm.activeSessions.Range(func(_, value any) bool {
		session := value.(*PersistentSession)
		session.mu.Lock()
		totalInflight += len(session.InflightQoS1) + len(session.InflightQoS2)
		for _, m := range session.InflightQoS1 {
			if m.RetryCount < sm.config.MaxRetryAttempts && now.Sub(m.Timestamp) >= retryInterval {
				due = append(due, pending{session.ClientID, m})
			}
		}
		for _, m := range session.InflightQoS2 {
			if m.RetryCount < sm.config.MaxRetryAttempts && now.Sub(m.Timestamp) >= retryInterval {
				due = append(due, pending{session.ClientID, m})
			}
		}
		session.mu.Unlock()
		return true
	})

	// Expose the live inflight count for observability.
	metrics.InflightCount.Set(float64(totalInflight))

	for _, p := range due {
		if err := cb(p.clientID, p.msg.Topic, p.msg.Payload, p.msg.QoS, p.msg.Retain, p.msg.PacketID); err != nil {
			// Redelivery failed (e.g. subscriber mailbox full / offline); leave
			// the inflight entry in place for the next retry cycle.
			log.Printf("[DEBUG] Redelivery deferred for %s packet %d: %v", p.clientID, p.msg.PacketID, err)
			continue
		}
		// Bump the retry counter and reset the timer so we don't spin.
		if value, exists := sm.activeSessions.Load(p.clientID); exists {
			s := value.(*PersistentSession)
			s.mu.Lock()
			if m, ok := s.InflightQoS1[p.msg.PacketID]; ok {
				m.RetryCount++
				m.Timestamp = now
			} else if m, ok := s.InflightQoS2[p.msg.PacketID]; ok {
				m.RetryCount++
				m.Timestamp = now
			}
			s.dirty = true
			s.mu.Unlock()
		}
	}
}
