package topic

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// ErrInvalidSharedFilter is returned by ParseSharedFilter for a malformed
// $share/<group>/<topic> filter. SUBSCRIBE must reject these with MQTT 5
// reason code 0x9F (Topic Filter invalid) rather than silently treating the
// string as an ordinary topic.
var ErrInvalidSharedFilter = errors.New("topic: invalid $share filter")

// IsSharedFilter reports whether filter uses the $share/ prefix.
func IsSharedFilter(filter string) bool {
	return filter == "$share" || strings.HasPrefix(filter, "$share/")
}

// ParseSharedFilter splits "$share/<group>/<topic>" into ("<group>", "<topic>").
// A non-shared filter returns ("", filter, nil). Malformed forms (missing
// parts, empty group/topic, or a group containing wildcards / '/' / leading
// '$') return an error.
//
// The group name is the key used by the forwarding-plane selector, so it must
// be a plain identifier; the returned topic is the real filter that
// matchesTopicFilter compares against a published topic.
func ParseSharedFilter(filter string) (group, topic string, err error) {
	if !IsSharedFilter(filter) {
		return "", filter, nil
	}
	parts := strings.SplitN(filter, "/", 3)
	if len(parts) != 3 || parts[1] == "" || parts[2] == "" {
		return "", "", fmt.Errorf("%w: %q", ErrInvalidSharedFilter, filter)
	}
	group = parts[1]
	if strings.ContainsAny(group, "+#/") || strings.HasPrefix(group, "$") {
		return "", "", fmt.Errorf("%w: group %q contains reserved characters", ErrInvalidSharedFilter, group)
	}
	return group, parts[2], nil
}

// GetSubscribersForGroups returns ordinary subscribers of topic plus exactly
// one round-robin member of each shared group listed in groups.
//
// It exists for the single-target shared-subscription forwarding path: when a
// node is selected for groups {g1, g2}, it must NOT deliver to groups g3, g4
// that happen to have local members (those groups were assigned to other
// nodes). Delivering via the plain GetSubscribers would do exactly that and
// produce duplicate deliveries.
//
// Callers must hold no lock; the method takes Store.mu.RLock itself.
func (s *Store) GetSubscribersForGroups(topic string, groups map[string]bool) []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var allSubs []*Subscription
	for subTopic, subs := range s.subscriptions {
		if matchesTopicFilter(topic, subTopic) {
			allSubs = append(allSubs, subs...)
		}
	}

	if len(groups) > 0 {
		for _, sharedSub := range s.sharedSubscriptions {
			if !groups[sharedSub.Group] || len(sharedSub.Mailboxes) == 0 {
				continue
			}
			if !matchesTopicFilter(topic, sharedSub.Topic) {
				continue
			}
			// Same atomic cursor discipline as getSharedSubscribersForTopic:
			// NextIndex is read under a shared lock and shared by all readers.
			idx := atomic.AddUint32(&sharedSub.NextIndex, 1) - 1
			allSubs = append(allSubs, sharedSub.Mailboxes[int(idx%uint32(len(sharedSub.Mailboxes)))])
		}
	}

	subsCopy := make([]*Subscription, 0, len(allSubs))
	subsCopy = append(subsCopy, allSubs...)
	return subsCopy
}
