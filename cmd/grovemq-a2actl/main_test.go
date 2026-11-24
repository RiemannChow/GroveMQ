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

package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func testClient(t *testing.T, handler http.HandlerFunc) (*cliClient, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &cliClient{
		baseURL:  server.URL,
		username: "admin",
		password: "public",
		http:     &http.Client{Timeout: 2 * time.Second},
	}, server
}

func TestRunGetEncodesAddressAndAuthenticates(t *testing.T) {
	var gotPath, gotEscapedPath, gotMethod string
	client, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "public" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotEscapedPath = r.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"address":"org/unit/agent","status":"online"}}`))
	})

	address := "org/unit/agent"
	var out bytes.Buffer
	if err := run(client, []string{"get", address}, &out); err != nil {
		t.Fatalf("run get: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("method = %s, want GET", gotMethod)
	}
	if gotPath != "/api/v5/a2a/agents/"+address {
		t.Fatalf("path = %q", gotPath)
	}
	if gotEscapedPath != "/api/v5/a2a/agents/"+url.PathEscape(address) {
		t.Fatalf("escaped path = %q", gotEscapedPath)
	}
	if !strings.Contains(out.String(), `"status": "online"`) {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunUnregisterUsesDelete(t *testing.T) {
	var gotMethod string
	client, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		_, _ = w.Write([]byte(`{"code":0,"data":{"result":"unregistered"}}`))
	})

	var out bytes.Buffer
	if err := run(client, []string{"unregister", "org/unit/agent"}, &out); err != nil {
		t.Fatalf("run unregister: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %s, want DELETE", gotMethod)
	}
	if !strings.Contains(out.String(), "unregistered") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunStatsAndAgents(t *testing.T) {
	client, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v5/a2a/stats":
			_, _ = w.Write([]byte(`{"code":0,"data":{"registered":2,"online":1}}`))
		case "/api/v5/a2a/agents":
			_, _ = w.Write([]byte(`{"code":0,"data":[{"address":"org/unit/agent"}]}`))
		default:
			http.NotFound(w, r)
		}
	})

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"stats"}, `"registered": 2`},
		{[]string{"agents"}, `"address": "org/unit/agent"`},
	} {
		var out bytes.Buffer
		if err := run(client, tc.args, &out); err != nil {
			t.Fatalf("run %v: %v", tc.args, err)
		}
		if !strings.Contains(out.String(), tc.want) {
			t.Fatalf("run %v output = %q, want %q", tc.args, out.String(), tc.want)
		}
	}
}

func TestRunReportsDisabledRegistry(t *testing.T) {
	client, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"code":501,"message":"A2A Registry is not enabled"}`))
	})

	var out bytes.Buffer
	err := run(client, []string{"stats"}, &out)
	if err == nil {
		t.Fatal("expected error when registry is disabled")
	}
	if !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("error = %q", err)
	}
}

func TestRunArgumentValidation(t *testing.T) {
	client, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be called")
	})

	var out bytes.Buffer
	for _, args := range [][]string{
		{"get"},
		{"unregister", "a", "b"},
		{"agents", "extra"},
		{"unknown"},
	} {
		if err := run(client, args, &out); err == nil {
			t.Fatalf("run %v: expected error", args)
		}
	}
}
