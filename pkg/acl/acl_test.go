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

package acl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRule(id, perm, action, user, client, topic string) *Rule {
	return &Rule{
		ID:         id,
		Permission: perm,
		Action:     action,
		Username:   user,
		ClientID:   client,
		Topic:      topic,
		Enabled:    true,
	}
}

func TestMatchTopicFilter(t *testing.T) {
	cases := []struct {
		topic, filter string
		want          bool
	}{
		{"a/b/c", "a/b/c", true},
		{"a/b/c", "a/+/c", true},
		{"a/b/c", "a/#", true},
		{"a/b/c", "a/b/+", true},
		{"a/b/c", "a/b", false},
		{"a/b/c", "a/b/d", false},
		{"a/b/c", "#", true},
		{"a", "+", true},
		{"a/b/c", "x/#", false},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, matchTopicFilter(tc.topic, tc.filter), "%s ~ %s", tc.topic, tc.filter)
	}
}

func TestManager_CheckAccess_AllowDeny(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.AddRule(newRule("r1", PermissionDeny, ActionAll, "", "", "sensors/#")))

	allow, matched, err := m.CheckAccess("c1", "u1", "sensors/temp", ActionPublish)
	require.NoError(t, err)
	assert.False(t, allow)
	require.NotNil(t, matched)
	assert.Equal(t, "r1", matched.ID)

	// topic outside the rule -> no match
	allow, matched, err = m.CheckAccess("c1", "u1", "other/topic", ActionPublish)
	require.NoError(t, err)
	assert.True(t, allow)
	assert.Nil(t, matched)
}

func TestManager_UsernameAndAnonymous(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.AddRule(newRule("u", PermissionDeny, ActionAll, "alice", "", "x")))

	// alice denied
	allow, _, _ := m.CheckAccess("c1", "alice", "x", ActionPublish)
	assert.False(t, allow)
	// bob allowed (no match)
	allow, matched, _ := m.CheckAccess("c1", "bob", "x", ActionPublish)
	assert.True(t, allow)
	assert.Nil(t, matched)

	// anonymous rule matches empty username only
	m2 := NewManager()
	require.NoError(t, m2.AddRule(newRule("anon", PermissionDeny, ActionAll, AnonymousSubject, "", "y")))
	allow, _, _ = m2.CheckAccess("c1", "", "y", ActionPublish)
	assert.False(t, allow)
	allow, _, _ = m2.CheckAccess("c1", "bob", "y", ActionPublish)
	assert.True(t, allow)
}

func TestManager_ClientID(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.AddRule(newRule("c", PermissionDeny, ActionAll, "", "device-1", "z")))

	allow, _, _ := m.CheckAccess("device-1", "u1", "z", ActionPublish)
	assert.False(t, allow)
	allow, _, _ = m.CheckAccess("device-2", "u1", "z", ActionPublish)
	assert.True(t, allow)
}

func TestManager_ActionSpecific(t *testing.T) {
	m := NewManager()
	// deny publish only
	require.NoError(t, m.AddRule(newRule("p", PermissionDeny, ActionPublish, "", "", "ctrl/#")))

	allow, _, _ := m.CheckAccess("c1", "u1", "ctrl/foo", ActionPublish)
	assert.False(t, allow)
	allow, _, _ = m.CheckAccess("c1", "u1", "ctrl/foo", ActionSubscribe)
	assert.True(t, allow)
}

func TestManager_PriorityFirstMatch(t *testing.T) {
	m := NewManager()
	// Narrow deny has higher priority (lower number) than the broad allow, so
	// the deny is evaluated first for matching topics.
	require.NoError(t, m.AddRule(&Rule{ID: "deny-one", Permission: PermissionDeny, Action: ActionAll, Topic: "secret", Enabled: true, Priority: 1}))
	require.NoError(t, m.AddRule(&Rule{ID: "allow-all", Permission: PermissionAllow, Action: ActionAll, Topic: "#", Enabled: true, Priority: 100}))

	allow, matched, _ := m.CheckAccess("c1", "u1", "secret", ActionPublish)
	assert.False(t, allow)
	require.NotNil(t, matched)
	assert.Equal(t, "deny-one", matched.ID)

	allow, _, _ = m.CheckAccess("c1", "u1", "public", ActionPublish)
	assert.True(t, allow)
}

func TestEngine_DisabledAllowsAll(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.AddRule(newRule("r", PermissionDeny, ActionAll, "", "", "#")))
	e := NewEngine(m, &ACLConfig{Enabled: false, DefaultAction: DefaultActionDeny})

	allow, _, _ := e.CheckPublishAccess("c1", "u1", "", "anything")
	assert.True(t, allow)
}

func TestEngine_DefaultAction(t *testing.T) {
	m := NewManager()
	e := NewEngine(m, &ACLConfig{Enabled: true, DefaultAction: DefaultActionAllow})
	allow, _, _ := e.CheckPublishAccess("c1", "u1", "", "anything")
	assert.True(t, allow)

	e2 := NewEngine(m, &ACLConfig{Enabled: true, DefaultAction: DefaultActionDeny})
	allow, _, _ = e2.CheckPublishAccess("c1", "u1", "", "anything")
	assert.False(t, allow)
}

func TestEngine_UsernameExtracted(t *testing.T) {
	m := NewManager()
	require.NoError(t, m.AddRule(newRule("r", PermissionDeny, ActionAll, "alice", "", "#")))
	e := NewEngine(m, &ACLConfig{Enabled: true, DefaultAction: DefaultActionAllow})

	// alice denied
	allow, _, _ := e.CheckPublishAccess("c1", "alice", "", "x")
	assert.False(t, allow)
	// bob allowed (default allow)
	allow, _, _ = e.CheckPublishAccess("c1", "bob", "", "x")
	assert.True(t, allow)
}

func TestManager_AddUpdateRemove(t *testing.T) {
	m := NewManager()
	r := newRule("", PermissionAllow, ActionAll, "", "", "a")
	require.NoError(t, m.AddRule(r))
	require.NotEmpty(t, r.ID)

	require.NoError(t, m.UpdateRule(r.ID, &Rule{Permission: PermissionDeny, Topic: "a"}))
	got := m.GetRule(r.ID)
	require.NotNil(t, got)
	assert.Equal(t, PermissionDeny, got.Permission)

	require.NoError(t, m.RemoveRule(r.ID))
	assert.Nil(t, m.GetRule(r.ID))
}

func TestManager_InvalidRule(t *testing.T) {
	m := NewManager()
	err := m.AddRule(newRule("bad", "maybe", ActionAll, "", "", "a"))
	assert.Error(t, err)
}
