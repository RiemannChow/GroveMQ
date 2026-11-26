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

package schema

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const simplifiedCard = `{
  "name": "IoT Operations Agent",
  "description": "Monitors factory telemetry and coordinates remediation.",
  "version": "1.2.3",
  "url": "mqtts://broker.example.com:8883",
  "skills": [{"id":"device-diagnostics","name":"Device Diagnostics","description":"Analyzes telemetry."}]
}`

const fullCard = `{
  "name": "IoT Operations Agent",
  "description": "Monitors factory telemetry.",
  "version": "1.2.3",
  "supportedInterfaces": [{"url":"mqtt://broker:1883","protocolBinding":"A2A-over-MQTT","protocolVersion":"1.0"}],
  "capabilities": {"streaming": true},
  "defaultInputModes": ["application/json"],
  "defaultOutputModes": ["application/json"],
  "skills": [{"id":"diag","name":"Diagnostics","description":"Analyzes telemetry.","tags":["iot"]}]
}`

func TestValidateSimplifiedCard(t *testing.T) {
	card, err := Parse([]byte(simplifiedCard))
	require.NoError(t, err)

	// Loose mode (EMQX-style simplified card) is valid.
	assert.NoError(t, Validate(card, ModeLoose))
	// Strict mode requires the full A2A 1.0 field set.
	err = Validate(card, ModeStrict)
	require.Error(t, err)
	// Strict mode rejects a simplified card on one of the missing A2A 1.0
	// fields (supportedInterfaces / input-output modes / skill tags).
	assert.NotEmpty(t, err.Error())
}

func TestValidateFullCard(t *testing.T) {
	card, err := Parse([]byte(fullCard))
	require.NoError(t, err)
	assert.NoError(t, Validate(card, ModeStrict))
	assert.NoError(t, Validate(card, ModeLoose))
}

func TestValidateRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{"no name", `{"description":"d","version":"1","skills":[{"id":"a","name":"a","description":"a","tags":["t"]}]}`, "name"},
		{"no skills", `{"name":"n","description":"d","version":"1"}`, "skills"},
		{"bad skill", `{"name":"n","description":"d","version":"1","skills":[{"name":"a","description":"a","tags":["t"]}]}`, "id"},
		{"duplicate skill", `{"name":"n","description":"d","version":"1","skills":[{"id":"a","name":"a","description":"a","tags":["t"]},{"id":"a","name":"b","description":"b","tags":["t"]}]}`, "duplicate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			card, err := Parse([]byte(tt.json))
			require.NoError(t, err)
			err = Validate(card, ModeLoose)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestNormalizeSimplifiedCard(t *testing.T) {
	card, err := ValidateAndNormalize([]byte(simplifiedCard), ModeLoose, "mqtt://fallback:1883")
	require.NoError(t, err)

	require.Len(t, card.SupportedInterfaces, 1)
	assert.Equal(t, "mqtts://broker.example.com:8883", card.SupportedInterfaces[0].URL)
	assert.Equal(t, ProtocolBindingMQTT, card.SupportedInterfaces[0].ProtocolBinding)
	assert.Equal(t, ProtocolVersion, card.SupportedInterfaces[0].ProtocolVersion)
	assert.NotEmpty(t, card.DefaultInputModes)
	assert.NotEmpty(t, card.DefaultOutputModes)
	assert.Equal(t, []string{"device-diagnostics"}, card.Skills[0].Tags)

	// Normalized card now passes strict validation.
	assert.NoError(t, Validate(card, ModeStrict))
}

func TestNormalizeUsesBindingURLFallback(t *testing.T) {
	raw := `{"name":"n","description":"d","version":"1","skills":[{"id":"a","name":"a","description":"a"}]}`
	card, err := ValidateAndNormalize([]byte(raw), ModeLoose, "mqtt://fallback:1883")
	require.NoError(t, err)
	assert.Equal(t, "mqtt://fallback:1883", card.SupportedInterfaces[0].URL)
}

func TestParseMode(t *testing.T) {
	m, err := ParseMode("")
	require.NoError(t, err)
	assert.Equal(t, ModeStrict, m)

	m, err = ParseMode("LOOSE")
	require.NoError(t, err)
	assert.Equal(t, ModeLoose, m)

	_, err = ParseMode("bogus")
	assert.Error(t, err)
}

func TestUnknownCardFieldsSurviveNormalizeAndMarshal(t *testing.T) {
	raw := `{"name":"n","description":"d","version":"1","url":"mqtt://b:1883",` +
		`"skills":[{"id":"a","name":"a","description":"a"}],` +
		`"x-vendor-extension":{"enabled":true,"tier":3},"x-future-field":[1,2]}`
	card, err := ValidateAndNormalize([]byte(raw), ModeLoose, "mqtt://fallback:1883")
	require.NoError(t, err)

	require.Contains(t, card.Extra, "x-vendor-extension")
	require.Contains(t, card.Extra, "x-future-field")

	out, err := json.Marshal(card)
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &decoded))
	assert.JSONEq(t, `{"enabled":true,"tier":3}`, string(decoded["x-vendor-extension"]))
	assert.JSONEq(t, `[1,2]`, string(decoded["x-future-field"]))

	// Normalization must fill known fields without losing extension data.
	assert.NotEmpty(t, card.SupportedInterfaces)
	assert.NoError(t, Validate(card, ModeStrict))
}

func TestKnownCardFieldsWinOverStaleExtra(t *testing.T) {
	card, err := Parse([]byte(`{"name":"n","description":"d","version":"1","skills":[]}`))
	require.NoError(t, err)
	card.Extra = map[string]json.RawMessage{"name": json.RawMessage(`"stale"`)}

	out, err := json.Marshal(card)
	require.NoError(t, err)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &decoded))
	assert.JSONEq(t, `"n"`, string(decoded["name"]))
}

// TestEMQXCardFieldsSurviveGroveMQNormalization covers Definition of Done item
// 7: the simplified card shape accepted by EMQX 6.2 is accepted by GroveMQ and
// none of its fields are changed, only derived A2A 1.0 fields are added.
func TestEMQXCardFieldsSurviveGroveMQNormalization(t *testing.T) {
	parsed, err := Parse([]byte(simplifiedCard))
	require.NoError(t, err)
	require.NoError(t, Validate(parsed, ModeLoose))
	normalized := Normalize(parsed, "mqtt://grovemq:1883")

	out, err := json.Marshal(normalized)
	require.NoError(t, err)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &got))
	var want map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(simplifiedCard), &want))

	for _, key := range []string{"name", "description", "version", "url"} {
		assert.JSONEq(t, string(want[key]), string(got[key]), key)
	}
	var skills []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got["skills"], &skills))
	require.Len(t, skills, 1)
	assert.JSONEq(t, `"device-diagnostics"`, string(skills[0]["id"]))
	assert.JSONEq(t, `"Device Diagnostics"`, string(skills[0]["name"]))
	assert.JSONEq(t, `"Analyzes telemetry."`, string(skills[0]["description"]))

	require.NoError(t, Validate(normalized, ModeStrict))
	assert.Equal(t, ProtocolBindingMQTT, normalized.SupportedInterfaces[0].ProtocolBinding)
}

// TestFullA2ACardRoundTripsUnchanged verifies that a strict A2A 1.0 card with
// an EMQX extension field survives a GroveMQ read without field differences.
func TestFullA2ACardRoundTripsUnchanged(t *testing.T) {
	raw := strings.Replace(fullCard, `"skills":`, `"x-emqx-agent-type":"mqtt","skills":`, 1)
	card, err := Parse([]byte(raw))
	require.NoError(t, err)
	require.NoError(t, Validate(card, ModeStrict))

	out, err := json.Marshal(card)
	require.NoError(t, err)
	assert.JSONEq(t, raw, string(out))
}
