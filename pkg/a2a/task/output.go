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
	"encoding/json"
	"fmt"
	"strings"
)

// ValidateTaskResult enforces the A2A rule that task outputs live in
// Artifacts. Text-only Messages remain valid for communication and control;
// data/file parts emitted by the agent are treated as misplaced results.
func ValidateTaskResult(t *Task) error {
	if t == nil {
		return fmt.Errorf("a2a: nil task result")
	}
	if t.Status.Message != nil {
		if err := validateAgentMessage(t.Status.Message, "task.status.message"); err != nil {
			return err
		}
	}
	for i := range t.History {
		if err := validateAgentMessage(&t.History[i], fmt.Sprintf("task.history[%d]", i)); err != nil {
			return err
		}
	}
	return nil
}

// ValidateResultPlacement checks a successful JSON-RPC result envelope. It is
// deliberately tolerant of result shapes GroveMQ does not model: only A2A Task
// and Message results are subject to the Artifact-only output rule.
func ValidateResultPlacement(result json.RawMessage) error {
	if len(result) == 0 || string(result) == "null" {
		return nil
	}
	var envelope struct {
		Task    *Task    `json:"task"`
		Message *Message `json:"message"`
	}
	if err := json.Unmarshal(result, &envelope); err != nil {
		return nil // not an A2A task result; leave validation to the SDK
	}
	if envelope.Task != nil {
		return ValidateTaskResult(envelope.Task)
	}
	if envelope.Message != nil {
		return validateAgentMessage(envelope.Message, "result.message")
	}
	// Bare Task results are returned by GetTask in some SDK versions.
	var bare Task
	if err := json.Unmarshal(result, &bare); err == nil && bare.ID != "" && bare.Status.State != "" {
		return ValidateTaskResult(&bare)
	}
	return nil
}

func validateAgentMessage(m *Message, where string) error {
	if m == nil || strings.EqualFold(m.Role, "ROLE_USER") {
		return nil
	}
	for i, part := range m.Parts {
		if !isResultPart(part) {
			continue
		}
		return fmt.Errorf("a2a: %s.parts[%d] carries task output in a Message; use an Artifact instead", where, i)
	}
	return nil
}

func isResultPart(p Part) bool {
	return p.Data != nil || strings.TrimSpace(p.MIMEType) != "" ||
		strings.EqualFold(p.Kind, "data") || strings.EqualFold(p.Kind, "file") ||
		strings.EqualFold(p.Kind, "raw")
}
