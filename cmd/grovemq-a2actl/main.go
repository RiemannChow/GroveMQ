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

// Command grovemq-a2actl is a small CLI for the GroveMQ A2A Registry admin API.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type apiResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type cliClient struct {
	baseURL  string
	username string
	password string
	http     *http.Client
}

func (c *cliClient) call(method, path string) (json.RawMessage, error) {
	req, err := http.NewRequest(method, strings.TrimRight(c.baseURL, "/")+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call admin API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var env apiResponse
	decodeErr := json.Unmarshal(body, &env)
	if resp.StatusCode != http.StatusOK {
		if decodeErr == nil && env.Message != "" {
			return nil, fmt.Errorf("admin API returned %d: %s", resp.StatusCode, env.Message)
		}
		return nil, fmt.Errorf("admin API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if decodeErr != nil {
		return nil, fmt.Errorf("decode response: %w", decodeErr)
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("admin API error %d: %s", env.Code, env.Message)
	}
	return env.Data, nil
}

func printData(w io.Writer, data json.RawMessage) error {
	if len(data) == 0 || string(data) == "null" {
		_, err := fmt.Fprintln(w, "{}")
		return err
	}
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	pretty, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("format response data: %w", err)
	}
	_, err = fmt.Fprintln(w, string(pretty))
	return err
}

func run(c *cliClient, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] == "help" {
		printUsage(out)
		return nil
	}

	command := args[0]
	rest := args[1:]
	switch command {
	case "agents", "list":
		if len(rest) != 0 {
			return fmt.Errorf("%s takes no arguments", command)
		}
		data, err := c.call(http.MethodGet, "/api/v5/a2a/agents")
		if err != nil {
			return err
		}
		return printData(out, data)

	case "agent", "get":
		address, err := singleAddress(command, rest)
		if err != nil {
			return err
		}
		data, err := c.call(http.MethodGet, "/api/v5/a2a/agents/"+url.PathEscape(address))
		if err != nil {
			return err
		}
		return printData(out, data)

	case "unregister", "delete":
		address, err := singleAddress(command, rest)
		if err != nil {
			return err
		}
		data, err := c.call(http.MethodDelete, "/api/v5/a2a/agents/"+url.PathEscape(address))
		if err != nil {
			return err
		}
		return printData(out, data)

	case "stats":
		if len(rest) != 0 {
			return fmt.Errorf("stats takes no arguments")
		}
		data, err := c.call(http.MethodGet, "/api/v5/a2a/stats")
		if err != nil {
			return err
		}
		return printData(out, data)

	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func singleAddress(command string, rest []string) (string, error) {
	if len(rest) != 1 || rest[0] == "" {
		return "", fmt.Errorf("usage: grovemq-a2actl %s <org>/<unit>/<agent>", command)
	}
	return rest[0], nil
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `Usage: grovemq-a2actl [flags] <command> [args]

Commands:
  agents                    List registered A2A agents
  get <org/unit/agent>      Show one agent card and status
  unregister <address>      Remove one agent from the registry
  stats                     Show registry counters

Flags:
`)
	flag.CommandLine.PrintDefaults()
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	endpoint := flag.String("endpoint", envOr("GROVEMQ_DASHBOARD_URL", "http://127.0.0.1:18083"), "Dashboard base URL")
	username := flag.String("user", envOr("GROVEMQ_DASHBOARD_USER", "admin"), "Dashboard username")
	password := flag.String("password", envOr("GROVEMQ_DASHBOARD_PASSWORD", "public"), "Dashboard password")
	timeout := flag.Duration("timeout", 5*time.Second, "HTTP request timeout")
	flag.Parse()

	client := &cliClient{
		baseURL:  *endpoint,
		username: *username,
		password: *password,
		http:     &http.Client{Timeout: *timeout},
	}
	if err := run(client, flag.Args(), os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "grovemq-a2actl:", err)
		os.Exit(1)
	}
}
