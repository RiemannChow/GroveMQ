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

package task

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// StreamEventKind mirrors the A2A 1.0 streaming event kinds.
type StreamEventKind string

const (
	// StreamEventStatus carries a TaskStatusUpdateEvent.
	StreamEventStatus StreamEventKind = "status"
	// StreamEventArtifact carries a TaskArtifactUpdateEvent.
	StreamEventArtifact StreamEventKind = "artifact"
)

// StreamEvent is one event on a task stream. Task and Artifact are mutually
// exclusive and selected by Kind.
type StreamEvent struct {
	Kind      StreamEventKind `json:"kind"`
	Address   string          `json:"address"`
	TaskID    string          `json:"taskId"`
	Task      *Task           `json:"task,omitempty"`
	Artifact  *Artifact       `json:"artifact,omitempty"`
	Final     bool            `json:"final,omitempty"`
	Timestamp string          `json:"timestamp"`
}

// StreamHub fans A2A task events out to in-process subscribers and closes
// subscriptions when a task reaches a terminal state. Broker clients use the
// equivalent MQTT topics; this hub is for the HTTP gateway and embedded
// agents that need the same lifecycle semantics without another socket.
type StreamHub struct {
	mu         sync.RWMutex
	streams    map[string]map[*Stream]struct{}
	closed     map[string]bool
	bufferSize int
}

// Stream is one SubscribeToTask stream. It stops receiving events after the
// task reaches a terminal state; Done is closed at that point.
type Stream struct {
	hub     *StreamHub
	key     string
	address string
	taskID  string
	events  chan StreamEvent
	done    chan struct{}
	once    sync.Once
}

// NewStreamHub creates an empty task stream hub. bufferSize <= 0 selects a
// small default so a slow subscriber cannot block the publisher indefinitely.
func NewStreamHub(bufferSize int) *StreamHub {
	if bufferSize <= 0 {
		bufferSize = 16
	}
	return &StreamHub{
		streams:    make(map[string]map[*Stream]struct{}),
		closed:     make(map[string]bool),
		bufferSize: bufferSize,
	}
}

// Subscribe registers a stream for one task. The first event is the current
// task status, as required by SubscribeToTask. Subscribing to a terminal task
// returns an immediate final event followed by Done.
func (h *StreamHub) Subscribe(address, taskID string, current *Task) (*Stream, error) {
	if h == nil {
		return nil, fmt.Errorf("a2a: nil stream hub")
	}
	if err := ValidateTopicAddress(address, taskID); err != nil {
		return nil, err
	}
	key := address + "/" + taskID

	s := &Stream{
		hub:     h,
		key:     key,
		address: address,
		taskID:  taskID,
		events:  make(chan StreamEvent, h.bufferSize),
		done:    make(chan struct{}),
	}

	h.mu.RLock()
	alreadyClosed := h.closed[key]
	h.mu.RUnlock()
	if alreadyClosed {
		// A terminal task still receives one final snapshot as its first event.
		if current != nil {
			event := StreamEvent{
				Kind:      StreamEventStatus,
				Address:   address,
				TaskID:    taskID,
				Task:      cloneTask(current),
				Final:     true,
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}
			s.events <- event
		}
		s.close()
		return s, nil
	}
	h.mu.Lock()
	if h.streams[key] == nil {
		h.streams[key] = make(map[*Stream]struct{})
	}
	h.streams[key][s] = struct{}{}
	h.mu.Unlock()

	if current != nil {
		_ = h.PublishStatus(address, current)
	}
	return s, nil
}

// PublishStatus sends a status event to all subscribers of task.ID and closes
// streams whose new state is terminal.
func (h *StreamHub) PublishStatus(address string, task *Task) error {
	if h == nil || task == nil {
		return fmt.Errorf("a2a: nil task status")
	}
	if err := ValidateTopicAddress(address, task.ID); err != nil {
		return err
	}
	event := StreamEvent{
		Kind:      StreamEventStatus,
		Address:   address,
		TaskID:    task.ID,
		Task:      cloneTask(task),
		Final:     IsTerminal(task.Status.State),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	h.publish(event, event.Final)
	return nil
}

// PublishArtifact sends an artifact event to all subscribers of taskID.
func (h *StreamHub) PublishArtifact(address, taskID string, artifact *Artifact) error {
	if h == nil || artifact == nil {
		return fmt.Errorf("a2a: nil artifact")
	}
	if err := ValidateTopicAddress(address, taskID); err != nil {
		return err
	}
	event := StreamEvent{
		Kind:      StreamEventArtifact,
		Address:   address,
		TaskID:    taskID,
		Artifact:  artifact,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	h.publish(event, false)
	return nil
}

// Events returns the event channel. The channel is never closed; select on
// Done to observe terminal closure.
func (s *Stream) Events() <-chan StreamEvent {
	return s.events
}

// Done is closed when the task reaches a terminal state or the stream is
// explicitly closed.
func (s *Stream) Done() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.done
}

// Close removes the stream from its hub. It is safe to call more than once.
func (s *Stream) Close() {
	if s == nil || s.hub == nil {
		return
	}
	s.hub.mu.Lock()
	delete(s.hub.streams[s.key], s)
	if len(s.hub.streams[s.key]) == 0 {
		delete(s.hub.streams, s.key)
	}
	s.hub.mu.Unlock()
	s.close()
}

func (s *Stream) close() {
	s.once.Do(func() { close(s.done) })
}

// cloneTask snapshots the observable Task fields so a later Transition does
// not rewrite events that are already queued for a subscriber.
func cloneTask(t *Task) *Task {
	if t == nil {
		return nil
	}
	out := *t
	out.Artifacts = append([]Artifact(nil), t.Artifacts...)
	out.History = append([]Message(nil), t.History...)
	if t.Status.Message != nil {
		msg := *t.Status.Message
		msg.Parts = append([]Part(nil), t.Status.Message.Parts...)
		out.Status.Message = &msg
	}
	return &out
}

func (h *StreamHub) publish(event StreamEvent, terminal bool) {
	key := event.Address + "/" + event.TaskID

	h.mu.RLock()
	streams := make([]*Stream, 0, len(h.streams[key]))
	for s := range h.streams[key] {
		streams = append(streams, s)
	}
	closed := h.closed[key]
	h.mu.RUnlock()

	// Stable delivery order across tests and embedded agents.
	sort.Slice(streams, func(i, j int) bool {
		return streams[i].address+streams[i].taskID < streams[j].address+streams[j].taskID
	})

	for _, s := range streams {
		select {
		case s.events <- event:
		default:
			// A slow in-process subscriber must not block the hot path.
		}
	}

	if terminal {
		h.mu.Lock()
		h.closed[key] = true
		for _, s := range streams {
			delete(h.streams[key], s)
			s.close()
		}
		delete(h.streams, key)
		h.mu.Unlock()
	} else if closed {
		// The task already ended; do not deliver events to a closed stream.
		for _, s := range streams {
			s.Close()
		}
	}
}
