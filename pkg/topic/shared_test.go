package topic

import (
	"errors"
	"testing"

	"github.com/turtacn/grovemq/pkg/actor"
)

func TestParseSharedFilter(t *testing.T) {
	cases := []struct {
		filter string
		group  string
		topic  string
		bad    bool
	}{
		{"sensors/temp", "", "sensors/temp", false},
		{"$share/g1/sensors/#", "g1", "sensors/#", false},
		{"$share/g1/a/b", "g1", "a/b", false},
		{"$share", "", "", true},
		{"$share/", "", "", true},
		{"$share/g1", "", "", true},
		{"$share//topic", "", "", true},
		{"$share/g1/", "", "", true},
		{"$share/g+1/topic", "", "", true},
		{"$share/g#1/topic", "", "", true},
		{"$share/$sys/topic", "", "", true},
	}
	for _, c := range cases {
		g, tp, err := ParseSharedFilter(c.filter)
		if c.bad {
			if !errors.Is(err, ErrInvalidSharedFilter) {
				t.Errorf("%q: err = %v, want ErrInvalidSharedFilter", c.filter, err)
			}
			continue
		}
		if err != nil || g != c.group || tp != c.topic {
			t.Errorf("%q: got (%q,%q,%v), want (%q,%q,nil)", c.filter, g, tp, err, c.group, c.topic)
		}
	}
}

func TestIsSharedFilter(t *testing.T) {
	if !IsSharedFilter("$share/g/t") || IsSharedFilter("a/b") {
		t.Fatal("IsSharedFilter mismatch")
	}
}

func TestGetSubscribersForGroups(t *testing.T) {
	s := NewStore()
	mPlain := actor.NewMailbox(10)
	mG1 := actor.NewMailbox(10)
	mG2 := actor.NewMailbox(10)
	s.Subscribe("sensors/#", "plain", mPlain, 0)
	s.Subscribe("$share/g1/sensors/#", "g1", mG1, 0)
	s.Subscribe("$share/g2/sensors/#", "g2", mG2, 0)

	// Plain GetSubscribers: ordinary + one member from *every* shared group.
	if got := len(s.GetSubscribers("sensors/temp")); got != 3 {
		t.Fatalf("GetSubscribers = %d, want 3", got)
	}

	// Selected for g1 only: ordinary subscriber + exactly one g1 member.
	got := s.GetSubscribersForGroups("sensors/temp", map[string]bool{"g1": true})
	if len(got) != 2 {
		t.Fatalf("GetSubscribersForGroups = %d, want 2", len(got))
	}
	seen := map[*actor.Mailbox]bool{}
	for _, sub := range got {
		seen[sub.Mailbox] = true
	}
	if !seen[mPlain] || !seen[mG1] || seen[mG2] {
		t.Fatalf("wrong members delivered: plain=%v g1=%v g2=%v", seen[mPlain], seen[mG1], seen[mG2])
	}

	// Empty group set behaves like an ordinary-subscribers-only lookup.
	if got := s.GetSubscribersForGroups("sensors/temp", nil); len(got) != 1 || got[0].Mailbox != mPlain {
		t.Fatalf("empty group set = %d subs, want only the ordinary subscriber", len(got))
	}
}
