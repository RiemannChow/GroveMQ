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

// Package schema implements the A2A Agent Card data model and validation for
// A2A over MQTT. The canonical model is defined by A2A Protocol Specification
// 1.0.0 (spec/a2a.proto); this package is the Go representation used by the
// Registry. It deliberately has no dependency on the broker or hook packages.
package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ProtocolBindingMQTT is the binding value advertised in a normalized card.
const ProtocolBindingMQTT = "A2A-over-MQTT"

// ProtocolVersion is the A2A version GroveMQ implements.
const ProtocolVersion = "1.0"

// AgentInterface describes one transport interface an agent supports.
type AgentInterface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocolBinding"`
	ProtocolVersion string `json:"protocolVersion"`
	Tenant          string `json:"tenant,omitempty"`
}

// AgentCapabilities indicates optional features an agent supports.
type AgentCapabilities struct {
	Streaming         bool `json:"streaming,omitempty"`
	PushNotifications bool `json:"pushNotifications,omitempty"`
	ExtendedAgentCard bool `json:"extendedAgentCard,omitempty"`
}

// AgentSkill is one capability offered by an agent.
type AgentSkill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// AgentCard is an agent's self-description. This mirrors the A2A 1.0 fields
// relevant to discovery; unknown fields are preserved on round-trip only in
// normalized form (extra fields are not retained).
type AgentCard struct {
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	Version             string            `json:"version"`
	URL                 string            `json:"url,omitempty"`
	SupportedInterfaces []AgentInterface  `json:"supportedInterfaces,omitempty"`
	Capabilities        AgentCapabilities `json:"capabilities,omitempty"`
	DefaultInputModes   []string          `json:"defaultInputModes,omitempty"`
	DefaultOutputModes  []string          `json:"defaultOutputModes,omitempty"`
	Skills              []AgentSkill      `json:"skills"`
}

// ValidationMode selects how strictly a published card is checked.
type ValidationMode string

const (
	// ModeStrict requires the full A2A 1.0 required field set.
	ModeStrict ValidationMode = "strict"
	// ModeLoose accepts EMQX-style simplified cards and normalizes them.
	ModeLoose ValidationMode = "loose"
)

// ParseMode converts a config string into a ValidationMode, defaulting to
// strict. Unknown values return an error so bad configuration is not silent.
func ParseMode(s string) (ValidationMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "strict":
		return ModeStrict, nil
	case "loose":
		return ModeLoose, nil
	default:
		return "", fmt.Errorf("invalid validation mode %q (want strict or loose)", s)
	}
}

// Parse decodes a card from JSON. It rejects unknown syntax but does not
// validate required fields; call Validate for that.
func Parse(data []byte) (*AgentCard, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty agent card")
	}
	var card AgentCard
	if err := json.Unmarshal(data, &card); err != nil {
		return nil, fmt.Errorf("invalid agent card JSON: %w", err)
	}
	return &card, nil
}

// Validate checks a card against the selected mode. In loose mode a simplified
// card (name/description/version/url/skills) is valid; in strict mode the full
// A2A 1.0 required field set must be present.
func Validate(card *AgentCard, mode ValidationMode) error {
	if card == nil {
		return fmt.Errorf("nil agent card")
	}
	if err := validateCommon(card); err != nil {
		return err
	}

	if mode == ModeLoose {
		return nil
	}

	if len(card.SupportedInterfaces) == 0 {
		return fmt.Errorf("supportedInterfaces is required")
	}
	for i, iface := range card.SupportedInterfaces {
		if strings.TrimSpace(iface.URL) == "" {
			return fmt.Errorf("supportedInterfaces[%d].url is required", i)
		}
		if strings.TrimSpace(iface.ProtocolBinding) == "" {
			return fmt.Errorf("supportedInterfaces[%d].protocolBinding is required", i)
		}
		if strings.TrimSpace(iface.ProtocolVersion) == "" {
			return fmt.Errorf("supportedInterfaces[%d].protocolVersion is required", i)
		}
	}
	if len(card.DefaultInputModes) == 0 {
		return fmt.Errorf("defaultInputModes is required")
	}
	if len(card.DefaultOutputModes) == 0 {
		return fmt.Errorf("defaultOutputModes is required")
	}
	for i, skill := range card.Skills {
		if len(skill.Tags) == 0 {
			return fmt.Errorf("skills[%d].tags is required", i)
		}
	}
	return nil
}

func validateCommon(card *AgentCard) error {
	if strings.TrimSpace(card.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if strings.TrimSpace(card.Description) == "" {
		return fmt.Errorf("description is required")
	}
	if strings.TrimSpace(card.Version) == "" {
		return fmt.Errorf("version is required")
	}
	if len(card.Skills) == 0 {
		return fmt.Errorf("skills must contain at least one entry")
	}
	seen := make(map[string]struct{}, len(card.Skills))
	for i, skill := range card.Skills {
		if strings.TrimSpace(skill.ID) == "" {
			return fmt.Errorf("skills[%d].id is required", i)
		}
		if strings.TrimSpace(skill.Name) == "" {
			return fmt.Errorf("skills[%d].name is required", i)
		}
		if strings.TrimSpace(skill.Description) == "" {
			return fmt.Errorf("skills[%d].description is required", i)
		}
		if _, dup := seen[skill.ID]; dup {
			return fmt.Errorf("duplicate skill id %q", skill.ID)
		}
		seen[skill.ID] = struct{}{}
	}
	return nil
}

// Normalize fills in A2A 1.0 fields missing from a simplified (EMQX-style)
// card. The returned card is safe to store and is a copy, not an alias.
func Normalize(card *AgentCard, bindingURL string) *AgentCard {
	out := *card
	out.Skills = append([]AgentSkill(nil), card.Skills...)
	for i := range out.Skills {
		out.Skills[i].Tags = append([]string(nil), out.Skills[i].Tags...)
	}
	out.DefaultInputModes = append([]string(nil), card.DefaultInputModes...)
	out.DefaultOutputModes = append([]string(nil), card.DefaultOutputModes...)
	out.SupportedInterfaces = append([]AgentInterface(nil), card.SupportedInterfaces...)

	if len(out.SupportedInterfaces) == 0 {
		url := card.URL
		if url == "" {
			url = bindingURL
		}
		out.SupportedInterfaces = []AgentInterface{{
			URL:             url,
			ProtocolBinding: ProtocolBindingMQTT,
			ProtocolVersion: ProtocolVersion,
		}}
	}
	if len(out.DefaultInputModes) == 0 {
		out.DefaultInputModes = []string{"text/plain", "application/json"}
	}
	if len(out.DefaultOutputModes) == 0 {
		out.DefaultOutputModes = []string{"text/plain", "application/json"}
	}
	for i := range out.Skills {
		if len(out.Skills[i].Tags) == 0 {
			out.Skills[i].Tags = []string{out.Skills[i].ID}
		}
	}
	return &out
}

// ValidateAndNormalize parses, validates and (in loose mode) normalizes a
// published card in one step. In strict mode the card is returned unchanged.
func ValidateAndNormalize(data []byte, mode ValidationMode, bindingURL string) (*AgentCard, error) {
	card, err := Parse(data)
	if err != nil {
		return nil, err
	}
	if err := Validate(card, mode); err != nil {
		return nil, err
	}
	if mode == ModeLoose {
		card = Normalize(card, bindingURL)
	}
	return card, nil
}
