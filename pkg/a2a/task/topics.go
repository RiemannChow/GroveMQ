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
	"strings"
)

const (
	// TopicPrefix is the reserved MQTT topic family used by A2A over MQTT.
	TopicPrefix = "$a2a/v1/"
	// RequestSuffix is appended to an agent address for abstract operations.
	RequestSuffix = "/requests"
	// StatusSuffix is appended to a task topic for TaskStatusUpdateEvent.
	StatusSuffix = "/status"
	// ArtifactSuffix is appended to a task topic for TaskArtifactUpdateEvent.
	ArtifactSuffix = "/artifacts"
	// InboxSuffix is the optional generic response topic advertised by an
	// Agent Card. Responses normally travel through MQTT v5 Response Topic.
	InboxSuffix = "/inbox"
	// SharedPrefix is the MQTT 5 shared-subscription prefix. A2A agents that
	// scale out use "$share/<group>/<request topic>" so each task request is
	// delivered to exactly one worker in the group.
	SharedPrefix = "$share/"
)

// RequestTopic returns the request topic for a three-part agent address.
func RequestTopic(address string) (string, error) {
	if err := ValidateAddress(address); err != nil {
		return "", err
	}
	return TopicPrefix + address + RequestSuffix, nil
}

// StatusTopic returns the streaming status topic for one task.
func StatusTopic(address, taskID string) (string, error) {
	if err := ValidateTopicAddress(address, taskID); err != nil {
		return "", err
	}
	return TopicPrefix + address + "/tasks/" + taskID + StatusSuffix, nil
}

// ArtifactTopic returns the streaming artifact topic for one task.
func ArtifactTopic(address, taskID string) (string, error) {
	if err := ValidateTopicAddress(address, taskID); err != nil {
		return "", err
	}
	return TopicPrefix + address + "/tasks/" + taskID + ArtifactSuffix, nil
}

// InboxTopic returns the optional generic response inbox for an agent.
func InboxTopic(address string) (string, error) {
	if err := ValidateAddress(address); err != nil {
		return "", err
	}
	return TopicPrefix + address + InboxSuffix, nil
}

// ValidateSharedGroup checks that a shared-subscription group name is safe to
// embed in an MQTT topic filter. MQTT forbids wildcards and '/' in the group
// segment; A2A additionally rejects '$' so a group cannot smuggle a reserved
// topic prefix.
func ValidateSharedGroup(group string) error {
	if group == "" {
		return fmt.Errorf("a2a: shared subscription group is required")
	}
	for _, r := range group {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("a2a: invalid character %q in shared group %q", r, group)
		}
	}
	return nil
}

// SharedRequestTopic returns the MQTT shared-subscription filter an agent
// worker pool uses to consume task requests. Every member subscribes to the
// same filter; the broker delivers each request to exactly one worker.
func SharedRequestTopic(group, address string) (string, error) {
	if err := ValidateSharedGroup(group); err != nil {
		return "", err
	}
	request, err := RequestTopic(address)
	if err != nil {
		return "", err
	}
	return SharedPrefix + group + "/" + request, nil
}

// ParseSharedRequestTopic extracts the group and agent address from a shared
// request filter produced by SharedRequestTopic. Non-shared filters and
// malformed values are reported as not matching.
func ParseSharedRequestTopic(filter string) (group, address string, ok bool) {
	if !strings.HasPrefix(filter, SharedPrefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(filter, SharedPrefix)
	idx := strings.IndexByte(rest, '/')
	if idx <= 0 {
		return "", "", false
	}
	group = rest[:idx]
	request := rest[idx+1:]
	if ValidateSharedGroup(group) != nil {
		return "", "", false
	}
	address, ok = ParseRequestTopic(request)
	return group, address, ok
}

// ParseRequestTopic extracts an agent address from an A2A request topic.
func ParseRequestTopic(topic string) (string, bool) {
	if !strings.HasPrefix(topic, TopicPrefix) || !strings.HasSuffix(topic, RequestSuffix) {
		return "", false
	}
	address := strings.TrimSuffix(strings.TrimPrefix(topic, TopicPrefix), RequestSuffix)
	if strings.Contains(address, "/tasks/") {
		return "", false
	}
	return address, ValidateAddress(address) == nil
}

// ValidateAddress checks the {org}/{unit}/{agent} naming rule. It mirrors
// hooks.ValidA2AAgentAddress without importing hooks, so the task model stays
// usable by standalone A2A SDK helpers.
func ValidateAddress(address string) error {
	parts := strings.Split(address, "/")
	if len(parts) != 3 {
		return fmt.Errorf("a2a: agent address must be {org}/{unit}/{agent}: %q", address)
	}
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("a2a: agent address contains an empty segment: %q", address)
		}
		for _, r := range p {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			default:
				return fmt.Errorf("a2a: invalid character %q in agent address %q", r, address)
			}
		}
	}
	return nil
}

// ValidateTaskID checks that a task ID is safe to embed in a topic segment.
func ValidateTaskID(taskID string) error {
	if taskID == "" {
		return fmt.Errorf("a2a: task id is required")
	}
	for _, r := range taskID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf("a2a: invalid character %q in task id %q", r, taskID)
		}
	}
	return nil
}

func ValidateTopicAddress(address, taskID string) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	return ValidateTaskID(taskID)
}
