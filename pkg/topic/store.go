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

// Package topic provides a thread-safe, in-memory data structure for managing
// MQTT topic subscriptions. It maps topic strings to a list of subscriber
// mailboxes, enabling the broker to efficiently route messages to the correct
// recipients. This implementation supports MQTT topic wildcards (+ and #).
package topic

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/turtacn/grovemq/pkg/actor"
)

// Subscription represents a single topic subscription with its QoS level.
type Subscription struct {
	ClientID string
	Mailbox  *actor.Mailbox
	QoS      byte
}

// SharedSubscription represents a subscription to a shared topic with group management.
type SharedSubscription struct {
	Group     string
	Topic     string
	Mailboxes []*Subscription
	// NextIndex is a monotonically increasing round-robin cursor. It is read
	// under Store.mu.RLock by GetSubscribers, so it must be updated atomically
	// rather than with a plain assignment (a write under a shared lock races
	// with other readers). Use atomic.AddUint32/LoadUint32/StoreUint32 only.
	NextIndex uint32
}

// Store provides a thread-safe, in-memory mapping of topic strings to lists of
// subscriber mailboxes. It is the central component for tracking which clients
// are subscribed to which topics.
type Store struct {
	subscriptions       map[string][]*Subscription
	sharedSubscriptions map[string]*SharedSubscription // key: "group/topic"
	mu                  sync.RWMutex
}

// NewStore creates and initializes a new, empty topic Store.
func NewStore() *Store {
	return &Store{
		subscriptions:       make(map[string][]*Subscription),
		sharedSubscriptions: make(map[string]*SharedSubscription),
	}
}

// Subscribe adds a subscriber's mailbox to the list of subscribers for a given
// topic with the specified QoS level. If the topic does not exist, it is created.
// Supports MQTT shared subscriptions with $share/group/topic format.
//
//   - topic: The topic string to subscribe to (may be shared subscription format).
//   - clientID: The subscribing client's ID (used by the broker for per-subscriber
//     QoS 1/2 inflight tracking).
//   - mailbox: The mailbox of the subscribing actor, which will receive the messages.
//   - qos: The QoS level for this subscription.
func (s *Store) Subscribe(topic string, clientID string, mailbox *actor.Mailbox, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this is a shared subscription ($share/group/topic)
	if strings.HasPrefix(topic, "$share/") {
		s.subscribeShared(topic, clientID, mailbox, qos)
	} else {
		s.subscriptions[topic] = s.addOrUpdateSubscription(s.subscriptions[topic], clientID, mailbox, qos)
	}
}

// subscribeShared handles shared subscription logic
func (s *Store) subscribeShared(shareTopic string, clientID string, mailbox *actor.Mailbox, qos byte) {
	// Parse $share/group/topic format
	parts := strings.SplitN(shareTopic, "/", 3)
	if len(parts) != 3 {
		// Invalid format, treat as regular subscription
		sub := &Subscription{ClientID: clientID, Mailbox: mailbox, QoS: qos}
		s.subscriptions[shareTopic] = append(s.subscriptions[shareTopic], sub)
		return
	}

	group := parts[1]
	actualTopic := parts[2]
	groupKey := group + "/" + actualTopic

	// Create or get existing shared subscription
	sharedSub, exists := s.sharedSubscriptions[groupKey]
	if !exists {
		sharedSub = &SharedSubscription{
			Group:     group,
			Topic:     actualTopic,
			Mailboxes: []*Subscription{},
			NextIndex: 0,
		}
		s.sharedSubscriptions[groupKey] = sharedSub
	}

	sharedSub.Mailboxes = s.addOrUpdateSubscription(sharedSub.Mailboxes, clientID, mailbox, qos)
}

// Unsubscribe removes a subscriber's mailbox from a specific topic's
// subscription list. If the mailbox is the last subscriber for that topic, the
// topic entry is removed from the store. Supports shared subscriptions.
//
// - topic: The topic string to unsubscribe from.
// - mailbox: The mailbox of the unsubscribing actor.
func (s *Store) Unsubscribe(topic string, mailbox *actor.Mailbox) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this is a shared subscription
	if strings.HasPrefix(topic, "$share/") {
		s.unsubscribeShared(topic, mailbox)
	} else {
		// Regular unsubscribe
		if subscribers, ok := s.subscriptions[topic]; ok {
			var newSubscribers []*Subscription
			for _, sub := range subscribers {
				if sub.Mailbox != mailbox {
					newSubscribers = append(newSubscribers, sub)
				}
			}
			if len(newSubscribers) > 0 {
				s.subscriptions[topic] = newSubscribers
			} else {
				delete(s.subscriptions, topic)
			}
		}
	}
}

// unsubscribeShared handles shared subscription unsubscribe logic
func (s *Store) unsubscribeShared(shareTopic string, mailbox *actor.Mailbox) {
	// Parse $share/group/topic format
	parts := strings.SplitN(shareTopic, "/", 3)
	if len(parts) != 3 {
		// Invalid format, treat as regular unsubscription
		if subscribers, ok := s.subscriptions[shareTopic]; ok {
			var newSubscribers []*Subscription
			for _, sub := range subscribers {
				if sub.Mailbox != mailbox {
					newSubscribers = append(newSubscribers, sub)
				}
			}
			if len(newSubscribers) > 0 {
				s.subscriptions[shareTopic] = newSubscribers
			} else {
				delete(s.subscriptions, shareTopic)
			}
		}
		return
	}

	group := parts[1]
	actualTopic := parts[2]
	groupKey := group + "/" + actualTopic

	// Remove from shared subscription
	if sharedSub, exists := s.sharedSubscriptions[groupKey]; exists {
		var newMailboxes []*Subscription
		for _, sub := range sharedSub.Mailboxes {
			if sub.Mailbox != mailbox {
				newMailboxes = append(newMailboxes, sub)
			}
		}

		if len(newMailboxes) > 0 {
			sharedSub.Mailboxes = newMailboxes
			// The cursor no longer needs clamping: selection always applies
			// modulo len(Mailboxes), so a shrinking group stays in bounds.
		} else {
			// No more subscribers, remove the shared subscription
			delete(s.sharedSubscriptions, groupKey)
		}
	}
}

// GetSubscribers returns a slice of all subscriptions for a specific topic.
// This method now supports MQTT wildcards (+ for single-level, # for multi-level).
//
// - topic: The topic for which to retrieve subscribers.
//
// Returns a copy of the slice of subscriber subscriptions that match the topic.
func (s *Store) GetSubscribers(topic string) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var allSubs []*Subscription

	// Check all stored subscription patterns
	for subTopic, subs := range s.subscriptions {
		if matchesTopicFilter(topic, subTopic) {
			allSubs = append(allSubs, subs...)
		}
	}

	// Check shared subscriptions for this topic
	sharedSubs := s.getSharedSubscribersForTopic(topic)
	allSubs = append(allSubs, sharedSubs...)

	// Return a true copy to prevent race conditions
	subsCopy := make([]*Subscription, 0, len(allSubs))
	for _, sub := range allSubs {
		subsCopy = append(subsCopy, sub)
	}
	return subsCopy
}

// getSharedSubscribersForTopic returns subscribers from shared subscriptions
// using round-robin distribution for load balancing
func (s *Store) getSharedSubscribersForTopic(topic string) []*Subscription {
	// We assume caller already holds at least a read lock for sharedSubscriptions.
	var selectedSubs []*Subscription

	// Check all shared subscriptions to see if topic matches
	for _, sharedSub := range s.sharedSubscriptions {
		if matchesTopicFilter(topic, sharedSub.Topic) && len(sharedSub.Mailboxes) > 0 {
			// Round-robin selection. The mailbox slice is stable because the
			// caller holds at least an RLock (writers need the exclusive lock),
			// but NextIndex is shared by every concurrent reader, so advance it
			// atomically. The unbounded uint32 wraps naturally; modulo keeps the
			// index inside the current slice.
			idx := atomic.AddUint32(&sharedSub.NextIndex, 1) - 1
			selected := sharedSub.Mailboxes[int(idx%uint32(len(sharedSub.Mailboxes)))]
			selectedSubs = append(selectedSubs, selected)
		}
	}

	return selectedSubs
}

// addOrUpdateSubscription ensures a mailbox is only subscribed once for a given topic.
// If the mailbox already exists, its QoS is updated; otherwise it is appended.
func (s *Store) addOrUpdateSubscription(subs []*Subscription, clientID string, mailbox *actor.Mailbox, qos byte) []*Subscription {
	for _, existing := range subs {
		if existing.Mailbox == mailbox {
			existing.QoS = qos
			existing.ClientID = clientID
			return subs
		}
	}

	return append(subs, &Subscription{
		ClientID: clientID,
		Mailbox:  mailbox,
		QoS:      qos,
	})
}

// RemoveAllSubscriptions removes every subscription entry associated with the given mailbox.
// It returns the topic filters that were removed so callers can perform additional bookkeeping.
func (s *Store) RemoveAllSubscriptions(mailbox *actor.Mailbox) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if mailbox == nil {
		return nil
	}

	var removedTopics []string

	// Handle regular subscriptions
	for topic, subs := range s.subscriptions {
		updated := make([]*Subscription, 0, len(subs))
		removedFromTopic := false

		for _, sub := range subs {
			if sub.Mailbox == mailbox {
				removedFromTopic = true
				continue
			}
			updated = append(updated, sub)
		}

		if removedFromTopic {
			removedTopics = append(removedTopics, topic)
			if len(updated) > 0 {
				s.subscriptions[topic] = updated
			} else {
				delete(s.subscriptions, topic)
			}
		}
	}

	// Handle shared subscriptions
	for groupKey, sharedSub := range s.sharedSubscriptions {
		updated := make([]*Subscription, 0, len(sharedSub.Mailboxes))
		removedFromShared := false

		for _, sub := range sharedSub.Mailboxes {
			if sub.Mailbox == mailbox {
				removedFromShared = true
				continue
			}
			updated = append(updated, sub)
		}

		if removedFromShared {
			if len(updated) > 0 {
				sharedSub.Mailboxes = updated
				// Selection is modulo len(Mailboxes); no cursor clamping needed.
			} else {
				delete(s.sharedSubscriptions, groupKey)
			}
		}
	}

	return removedTopics
}

// matchesTopicFilter checks if a published topic matches a subscription topic filter.
// Implements MQTT 3.1.1 specification for topic matching with wildcards.
//
// Per the MQTT spec, a filter beginning with a wildcard must NOT match a topic
// name beginning with '$' (reserved/system topics such as $SYS and $a2a).
// An explicit filter that itself starts with '$' still matches normally.
func matchesTopicFilter(topic, filter string) bool {
	if !reservedTopicAllowed(topic, filter) {
		return false
	}
	topicSegments := strings.Split(topic, "/")
	filterSegments := strings.Split(filter, "/")

	topicLen := len(topicSegments)
	filterLen := len(filterSegments)

	for i := 0; i < filterLen; i++ {
		if i >= topicLen {
			// If filter has more segments but the last one is not '#', no match
			return filterSegments[i] == "#" && i == filterLen-1
		}

		filterSegment := filterSegments[i]
		topicSegment := topicSegments[i]

		if filterSegment == "#" {
			// '#' must be the last segment in the filter
			return i == filterLen-1
		}

		if filterSegment != "+" && filterSegment != topicSegment {
			return false
		}
	}

	// If we finished iterating through the filter, the topic must have the same number of segments
	return topicLen == filterLen
}

// reservedTopicAllowed implements the MQTT rule that wildcard filters must not
// match reserved topics (names beginning with '$'), unless the filter itself
// explicitly starts with '$'. Exact matches are always allowed.
func reservedTopicAllowed(topic, filter string) bool {
	if topic == filter {
		return true
	}
	if len(topic) > 0 && topic[0] == '$' {
		return len(filter) > 0 && filter[0] == '$'
	}
	return true
}
