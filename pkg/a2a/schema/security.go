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
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const (
	// PropertySecurityMetadata marks a card that passed security verification.
	PropertySecurityMetadata = "x-grovemq-security-verified"
	// PropertyCardNormalized marks a card normalized from EMQX loose format.
	PropertyCardNormalized = "x-grovemq-card-normalized"
	// DefaultJWKSTimeout bounds a JWKS fetch.
	DefaultJWKSTimeout = 3 * time.Second
	maxJWKSBytes       = 1 << 20
)

// SecurityError marks a rejected JWS/signature or missing security metadata,
// allowing the Registry to distinguish it from a schema validation failure.
type SecurityError struct{ Err error }

func (e *SecurityError) Error() string { return e.Err.Error() }
func (e *SecurityError) Unwrap() error { return e.Err }

// SecurityConfig controls Agent Card signature verification.
type SecurityConfig struct {
	// RequireMetadata rejects cards with no securitySchemes/security metadata.
	RequireMetadata bool
	// AllowedJKUPrefixes is the allowlist for JWS protected-header jku URLs.
	// Verification is disabled when a signed card uses a JKU outside it.
	AllowedJKUPrefixes []string
	// Client is used for JWKS fetches; http.DefaultClient when nil.
	Client *http.Client
}

// SecurityVerifier verifies flattened JWS signatures carried by Agent Cards.
type SecurityVerifier struct {
	cfg    SecurityConfig
	client *http.Client
}

// NewSecurityVerifier creates a verifier. A nil config disables verification
// but still reports cards that contain signatures as unverified.
func NewSecurityVerifier(cfg *SecurityConfig) *SecurityVerifier {
	v := &SecurityVerifier{}
	if cfg != nil {
		v.cfg = *cfg
		v.cfg.AllowedJKUPrefixes = append([]string(nil), cfg.AllowedJKUPrefixes...)
	}
	v.client = v.cfg.Client
	if v.client == nil {
		v.client = &http.Client{Timeout: DefaultJWKSTimeout}
	}
	return v
}

// Verify checks metadata requirements and all signatures on card. It returns
// nil for an unsigned card unless RequireMetadata is set.
func (v *SecurityVerifier) Verify(ctx context.Context, data []byte, card *AgentCard) error {
	if v == nil || card == nil {
		return fmt.Errorf("a2a: nil security verifier or card")
	}
	if v.cfg.RequireMetadata && !hasSecurityMetadata(card) {
		return &SecurityError{Err: fmt.Errorf("a2a: securitySchemes and security are required")}
	}
	if len(card.Signatures) == 0 {
		if v.cfg.RequireMetadata {
			return &SecurityError{Err: fmt.Errorf("a2a: at least one JWS signature is required")}
		}
		return nil
	}
	payload, err := canonicalCardPayload(data)
	if err != nil {
		return &SecurityError{Err: err}
	}
	for i, sig := range card.Signatures {
		if err := v.verifySignature(ctx, payload, sig); err != nil {
			return &SecurityError{Err: fmt.Errorf("a2a: signature %d: %w", i, err)}
		}
	}
	return nil
}

func hasSecurityMetadata(card *AgentCard) bool {
	return len(card.SecuritySchemes) > 0 && (len(card.Security) > 0 || len(card.SecurityRequirements) > 0)
}

// canonicalCardPayload returns the deterministic JSON bytes covered by a
// signature: the raw card with its signatures removed. Object keys are sorted
// by encoding/json, and unknown SDK extension fields are preserved verbatim.
func canonicalCardPayload(data []byte) ([]byte, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("a2a: invalid card JSON for signature verification: %w", err)
	}
	delete(raw, "signatures")
	return json.Marshal(raw)
}

// SigningInput builds the RFC 7515 JWS signing input for a card. SDK authors
// can use it to produce a signature GroveMQ will accept.
func SigningInput(card *AgentCard, protectedHeader map[string]interface{}) (string, error) {
	copyCard := *card
	copyCard.Signatures = nil
	unsigned, err := json.Marshal(&copyCard)
	if err != nil {
		return "", err
	}
	payload, err := canonicalCardPayload(unsigned)
	if err != nil {
		return "", err
	}
	headerJSON, err := json.Marshal(protectedHeader)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
		base64.RawURLEncoding.EncodeToString(payload), nil
}

func (v *SecurityVerifier) verifySignature(ctx context.Context, payload []byte, sig AgentCardSignature) error {
	headerBytes, err := base64.RawURLEncoding.DecodeString(sig.Protected)
	if err != nil {
		return fmt.Errorf("invalid protected header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		JKU string `json:"jku"`
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return fmt.Errorf("invalid protected header JSON: %w", err)
	}
	if !jkuAllowed(header.JKU, v.cfg.AllowedJKUPrefixes) {
		return fmt.Errorf("jku %q is not allowed", header.JKU)
	}
	keys, err := v.fetchJWKS(ctx, header.JKU)
	if err != nil {
		return err
	}
	key, ok := findJWK(keys, header.Kid, header.Alg)
	if !ok {
		return fmt.Errorf("no JWKS key for kid %q and alg %q", header.Kid, header.Alg)
	}
	signature, err := base64.RawURLEncoding.DecodeString(sig.Signature)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	input := sig.Protected + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	switch header.Alg {
	case "RS256":
		pub, err := rsaJWK(key)
		if err != nil {
			return err
		}
		return rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], signature)
	case "ES256":
		pub, err := ecJWK(key)
		if err != nil {
			return err
		}
		if len(signature) != 64 {
			return fmt.Errorf("ES256 signature must be 64 bytes")
		}
		r := new(big.Int).SetBytes(signature[:32])
		s := new(big.Int).SetBytes(signature[32:])
		if !ecdsa.Verify(pub, digest[:], r, s) {
			return fmt.Errorf("ES256 signature verification failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported JWS alg %q", header.Alg)
	}
}

func jkuAllowed(jku string, prefixes []string) bool {
	if jku == "" || len(prefixes) == 0 {
		return false
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(jku, prefix) {
			return true
		}
	}
	return false
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func findJWK(keys []jwk, kid, alg string) (jwk, bool) {
	for _, key := range keys {
		if key.Kid != kid || key.Use == "enc" {
			continue
		}
		if key.Alg == "" || key.Alg == alg {
			return key, true
		}
	}
	return jwk{}, false
}

func (v *SecurityVerifier) fetchJWKS(ctx context.Context, jku string) ([]jwk, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jku, nil)
	if err != nil {
		return nil, fmt.Errorf("invalid jku: %w", err)
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch JWKS: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil || len(body) > maxJWKSBytes {
		return nil, fmt.Errorf("fetch JWKS: response unreadable or too large")
	}
	var set jwkSet
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, fmt.Errorf("invalid JWKS: %w", err)
	}
	return set.Keys, nil
}

func rsaJWK(key jwk) (*rsa.PublicKey, error) {
	if key.Kty != "RSA" {
		return nil, fmt.Errorf("JWK kty %q is not RSA", key.Kty)
	}
	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, fmt.Errorf("invalid RSA modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil || len(eBytes) == 0 {
		return nil, fmt.Errorf("invalid RSA exponent")
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}

func ecJWK(key jwk) (*ecdsa.PublicKey, error) {
	if key.Kty != "EC" || key.Crv != "P-256" {
		return nil, fmt.Errorf("unsupported EC JWK %q/%q", key.Kty, key.Crv)
	}
	xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("invalid EC x coordinate: %w", err)
	}
	yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil {
		return nil, fmt.Errorf("invalid EC y coordinate: %w", err)
	}
	return &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}
