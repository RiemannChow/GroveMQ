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

package registry

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/a2a/schema"
	"github.com/turtacn/grovemq/pkg/hooks"
)

func rsaJWK(kid string, key *rsa.PrivateKey) string {
	n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
	return `{"keys":[{"kty":"RSA","kid":"` + kid + `","alg":"RS256","use":"sig","n":"` + n + `","e":"` + e + `"}]}`
}

// TestRegistryRejectsForgedSignature covers Definition of Done item 6: a card
// signed by a different key than the allowlisted JWKS entry must be rejected in
// strict validation mode.
func TestRegistryRejectsForgedSignature(t *testing.T) {
	trusted, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(rsaJWK("trusted-key", trusted)))
	}))
	defer jwks.Close()

	card, err := schema.Parse([]byte(fullCard))
	require.NoError(t, err)
	header := map[string]interface{}{
		"alg": "RS256", "kid": "trusted-key", "jku": jwks.URL + "/jwks",
	}
	input, err := schema.SigningInput(card, header)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, attacker, crypto.SHA256, digest[:])
	require.NoError(t, err)
	headerJSON, err := json.Marshal(header)
	require.NoError(t, err)
	card.Signatures = []schema.AgentCardSignature{{
		Protected: base64.RawURLEncoding.EncodeToString(headerJSON),
		Signature: base64.RawURLEncoding.EncodeToString(sig),
	}}
	payload, err := json.Marshal(card)
	require.NoError(t, err)

	cfg := *DefaultConfig()
	cfg.Enabled = true
	cfg.Validate = schema.ModeStrict
	cfg.Security = &schema.SecurityConfig{
		RequireMetadata:    true,
		AllowedJKUPrefixes: []string{jwks.URL + "/"},
	}
	r := New(cfg)
	addr := "org/unit/agent"

	assert.False(t, publish(t, r, addr, hooks.A2ADiscoveryPrefix+addr, payload))
	_, ok := r.Get(addr)
	assert.False(t, ok)
	assert.Equal(t, int64(1), r.Stats().SignatureFails)
	assert.Equal(t, int64(0), r.Stats().SecurityVerified)
}

// TestRegistryAcceptsTrustedSignature verifies the positive side of the same
// acceptance criterion so a failing verifier cannot be confused with strict
// mode rejecting an unsigned card.
func TestRegistryAcceptsTrustedSignature(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(rsaJWK("trusted-key", key)))
	}))
	defer jwks.Close()

	card, err := schema.Parse([]byte(fullCard))
	require.NoError(t, err)
	header := map[string]interface{}{
		"alg": "RS256", "kid": "trusted-key", "jku": jwks.URL + "/jwks",
	}
	input, err := schema.SigningInput(card, header)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	headerJSON, err := json.Marshal(header)
	require.NoError(t, err)
	card.Signatures = []schema.AgentCardSignature{{
		Protected: base64.RawURLEncoding.EncodeToString(headerJSON),
		Signature: base64.RawURLEncoding.EncodeToString(sig),
	}}
	payload, err := json.Marshal(card)
	require.NoError(t, err)

	cfg := *DefaultConfig()
	cfg.Enabled = true
	cfg.Validate = schema.ModeStrict
	cfg.Security = &schema.SecurityConfig{AllowedJKUPrefixes: []string{jwks.URL + "/"}}
	r := New(cfg)
	addr := "org/unit/agent"

	require.True(t, publish(t, r, addr, hooks.A2ADiscoveryPrefix+addr, payload))
	assert.Equal(t, int64(1), r.Stats().SecurityVerified)
}
