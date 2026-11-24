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

// Package task implements the A2A 1.0 task data model, state machine and
// JSON-RPC envelope used by the MQTT binding. It has no broker dependency so
// the same types can be shared by agents, gateways and tests.
package task

import (
	"encoding/json"
	"fmt"
	"time"
)

// TaskState is the A2A 1.0 TaskState enum.
type TaskState string

const (
	TaskStateUnspecified   TaskState = "UNSPECIFIED"
	TaskStateSubmitted     TaskState = "SUBMITTED"
	TaskStateWorking       TaskState = "WORKING"
	TaskStateCompleted     TaskState = "COMPLETED"
	TaskStateFailed        TaskState = "FAILED"
	TaskStateCanceled      TaskState = "CANCELED"
	TaskStateInputRequired TaskState = "INPUT_REQUIRED"
	TaskStateRejected      TaskState = "REJECTED"
	TaskStateAuthRequired  TaskState = "AUTH_REQUIRED"
)

// AllStates lists every A2A 1.0 task state in canonical order.
var AllStates = []TaskState{
	TaskStateUnspecified,
	TaskStateSubmitted,
	TaskStateWorking,
	TaskStateCompleted,
	TaskStateFailed,
	TaskStateCanceled,
	TaskStateInputRequired,
	TaskStateRejected,
	TaskStateAuthRequired,
}

// Part is one content fragment of a Message or Artifact. A2A requires task
// outputs to live in Artifact parts; Messages carry communication and control.
type Part struct {
	Kind     string          `json:"kind,omitempty"`
	Text     string          `json:"text,omitempty"`
	Data     json.RawMessage `json:"data,omitempty"`
	MIMEType string          `json:"mimeType,omitempty"`
}

// Message is an A2A Message. Role values follow spec/a2a.proto, e.g.
// "ROLE_USER" and "ROLE_AGENT".
type Message struct {
	MessageID string                 `json:"messageId"`
	Role      string                 `json:"role,omitempty"`
	Parts     []Part                 `json:"parts,omitempty"`
	TaskID    string                 `json:"taskId,omitempty"`
	ContextID string                 `json:"contextId,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Artifact is an immutable task output.
type Artifact struct {
	ArtifactID  string                 `json:"artifactId"`
	Name        string                 `json:"name,omitempty"`
	Description string                 `json:"description,omitempty"`
	Parts       []Part                 `json:"parts,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// TaskStatus is the current state plus an optional status message.
type TaskStatus struct {
	State     TaskState `json:"state"`
	Message   *Message  `json:"message,omitempty"`
	Timestamp string    `json:"timestamp,omitempty"`
}

// Task is the canonical A2A Task carried in MQTT payloads and gateway replies.
type Task struct {
	ID        string                 `json:"id"`
	ContextID string                 `json:"contextId,omitempty"`
	Status    TaskStatus             `json:"status"`
	Artifacts []Artifact             `json:"artifacts,omitempty"`
	History   []Message              `json:"history,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// NewTask creates a task in SUBMITTED with a UTC RFC3339 timestamp.
func NewTask(id, contextID string) *Task {
	return &Task{
		ID:        id,
		ContextID: contextID,
		Status: TaskStatus{
			State:     TaskStateSubmitted,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
	}
}

// IsTerminal reports whether no further transition is legal.
func IsTerminal(state TaskState) bool {
	switch state {
	case TaskStateCompleted, TaskStateFailed, TaskStateCanceled, TaskStateRejected:
		return true
	default:
		return false
	}
}

// IsInterruptible reports whether a task is paused and can be resumed.
func IsInterruptible(state TaskState) bool {
	return state == TaskStateInputRequired || state == TaskStateAuthRequired
}

// CanTransition implements the A2A 1.0 task lifecycle. Terminal states are
// absorbing; interrupted states may resume or end, but never return to
// SUBMITTED.
func CanTransition(from, to TaskState) bool {
	if from == to {
		return !IsTerminal(from)
	}
	switch from {
	case TaskStateSubmitted:
		return to == TaskStateWorking || to == TaskStateFailed ||
			to == TaskStateCanceled || to == TaskStateRejected ||
			to == TaskStateAuthRequired
	case TaskStateWorking:
		return to == TaskStateInputRequired || to == TaskStateAuthRequired ||
			to == TaskStateCompleted || to == TaskStateFailed ||
			to == TaskStateCanceled || to == TaskStateRejected
	case TaskStateInputRequired, TaskStateAuthRequired:
		return to == TaskStateWorking || to == TaskStateFailed ||
			to == TaskStateCanceled || to == TaskStateRejected
	default:
		return false
	}
}

// ValidateTransition returns a semantic error when the transition is illegal.
func ValidateTransition(from, to TaskState) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("illegal task state transition %s -> %s", from, to)
}

// Transition updates a task state and timestamp after validation.
func (t *Task) Transition(to TaskState) error {
	if t == nil {
		return fmt.Errorf("nil task")
	}
	if err := ValidateTransition(t.Status.State, to); err != nil {
		return err
	}
	t.Status.State = to
	t.Status.Timestamp = time.Now().UTC().Format(time.RFC3339)
	return nil
}

// AddArtifact appends an output artifact. Task outputs must not be placed in
// Message.History, so this helper is the canonical way to attach results.
func (t *Task) AddArtifact(a Artifact) error {
	if t == nil {
		return fmt.Errorf("nil task")
	}
	if a.ArtifactID == "" {
		return fmt.Errorf("artifact id is required")
	}
	t.Artifacts = append(t.Artifacts, a)
	return nil
}
