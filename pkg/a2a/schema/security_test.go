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
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
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
)

func testCard() *AgentCard {
	return &AgentCard{
		Name:        "Signed Agent",
		Description: "Verifies JWS signatures",
		Version:     "1.0.0",
		URL:         "mqtt://broker:1883",
		Skills:      []AgentSkill{{ID: "verify", Name: "Verify", Description: "Checks signatures"}},
		SecuritySchemes: map[string]json.RawMessage{
			"bearer": json.RawMessage(`{"type":"http","scheme":"bearer"}`),
		},
		Security: []map[string][]string{{"bearer": {}}},
	}
}

func jwksServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func rsaJWKSBody(t *testing.T, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes())
	return `{"keys":[{"kty":"RSA","kid":"` + kid + `","alg":"RS256","use":"sig","n":"` + n + `","e":"` + e + `"}]}`
}

func signCardRSA(t *testing.T, card *AgentCard, key *rsa.PrivateKey, header map[string]interface{}) {
	t.Helper()
	input, err := SigningInput(card, header)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	headerJSON, err := json.Marshal(header)
	require.NoError(t, err)
	card.Signatures = []AgentCardSignature{{
		Protected: base64.RawURLEncoding.EncodeToString(headerJSON),
		Signature: base64.RawURLEncoding.EncodeToString(sig),
	}}
}

func marshalCard(t *testing.T, card *AgentCard) []byte {
	t.Helper()
	data, err := json.Marshal(card)
	require.NoError(t, err)
	return data
}

func TestSecurityVerifierAcceptsRS256Card(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := jwksServer(t, rsaJWKSBody(t, key, "test-key"))

	card := testCard()
	signCardRSA(t, card, key, map[string]interface{}{
		"alg": "RS256", "kid": "test-key", "jku": server.URL + "/jwks",
	})
	data := marshalCard(t, card)

	v := NewSecurityVerifier(&SecurityConfig{
		RequireMetadata:    true,
		AllowedJKUPrefixes: []string{server.URL + "/"},
	})
	require.NoError(t, v.Verify(context.Background(), data, card))
}

func TestSecurityVerifierRejectsTamperedCard(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := jwksServer(t, rsaJWKSBody(t, key, "test-key"))

	card := testCard()
	signCardRSA(t, card, key, map[string]interface{}{
		"alg": "RS256", "kid": "test-key", "jku": server.URL + "/jwks",
	})
	card.Name = "Tampered Agent"

	v := NewSecurityVerifier(&SecurityConfig{
		RequireMetadata:    true,
		AllowedJKUPrefixes: []string{server.URL + "/"},
	})
	var secErr *SecurityError
	require.ErrorAs(t, v.Verify(context.Background(), marshalCard(t, card), card), &secErr)
}

func TestSecurityVerifierAcceptsES256Card(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	x := base64.RawURLEncoding.EncodeToString(key.X.Bytes())
	y := base64.RawURLEncoding.EncodeToString(key.Y.Bytes())
	server := jwksServer(t, `{"keys":[{"kty":"EC","kid":"ec-key","alg":"ES256","use":"sig","crv":"P-256","x":"`+x+`","y":"`+y+`"}]}`)

	card := testCard()
	header := map[string]interface{}{
		"alg": "ES256", "kid": "ec-key", "jku": server.URL + "/jwks",
	}
	input, err := SigningInput(card, header)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	require.NoError(t, err)
	signature := append(make([]byte, 32), make([]byte, 32)...)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])
	headerJSON, err := json.Marshal(header)
	require.NoError(t, err)
	card.Signatures = []AgentCardSignature{{
		Protected: base64.RawURLEncoding.EncodeToString(headerJSON),
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	}}

	v := NewSecurityVerifier(&SecurityConfig{AllowedJKUPrefixes: []string{server.URL + "/"}})
	require.NoError(t, v.Verify(context.Background(), marshalCard(t, card), card))
}

func TestSecurityVerifierPreservesUnknownFieldsInSignature(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := jwksServer(t, rsaJWKSBody(t, key, "test-key"))

	// Emulate a standard SDK adding an extension field before signing.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(marshalCard(t, testCard()), &raw))
	raw["x-sdk-extension"] = json.RawMessage(`{"enabled":true}`)
	payload, err := canonicalCardPayload(mustMarshal(t, raw))
	require.NoError(t, err)
	header := map[string]interface{}{"alg": "RS256", "kid": "test-key", "jku": server.URL + "/jwks"}
	headerJSON, err := json.Marshal(header)
	require.NoError(t, err)
	protected := base64.RawURLEncoding.EncodeToString(headerJSON)
	input := protected + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	require.NoError(t, err)
	raw["signatures"] = mustMarshal(t, []AgentCardSignature{{
		Protected: protected,
		Signature: base64.RawURLEncoding.EncodeToString(sig),
	}})
	data := mustMarshal(t, raw)
	card, err := Parse(data)
	require.NoError(t, err)

	v := NewSecurityVerifier(&SecurityConfig{AllowedJKUPrefixes: []string{server.URL + "/"}})
	require.NoError(t, v.Verify(context.Background(), data, card))
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

func TestSecurityVerifierRejectsUnlistedJKU(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	card := testCard()
	signCardRSA(t, card, key, map[string]interface{}{
		"alg": "RS256", "kid": "test-key", "jku": "https://attacker.example/jwks",
	})

	v := NewSecurityVerifier(&SecurityConfig{AllowedJKUPrefixes: []string{"https://corp.example/"}})
	err = v.Verify(context.Background(), marshalCard(t, card), card)
	var secErr *SecurityError
	require.ErrorAs(t, err, &secErr)
	assert.Contains(t, err.Error(), "not allowed")
}

func TestSecurityVerifierRequiresMetadata(t *testing.T) {
	card := &AgentCard{
		Name: "Plain", Description: "No security", Version: "1.0.0",
		Skills: []AgentSkill{{ID: "x", Name: "X", Description: "X"}},
	}
	v := NewSecurityVerifier(&SecurityConfig{RequireMetadata: true})
	var secErr *SecurityError
	require.ErrorAs(t, v.Verify(context.Background(), nil, card), &secErr)
}

func TestSecurityVerifierOptionalByDefault(t *testing.T) {
	card := testCard()
	v := NewSecurityVerifier(nil)
	require.NoError(t, v.Verify(context.Background(), marshalCard(t, card), card))
}

func TestJKUAllowlistRequiresExactPrefix(t *testing.T) {
	assert.True(t, jkuAllowed("https://corp.example/jwks", []string{"https://corp.example/"}))
	assert.False(t, jkuAllowed("https://corp.example.evil/jwks", []string{"https://corp.example/"}))
	assert.False(t, jkuAllowed("https://corp.example/jwks", nil))
	assert.False(t, jkuAllowed("", []string{"https://corp.example/"}))
}
