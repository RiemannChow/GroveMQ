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
