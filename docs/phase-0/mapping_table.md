# Erlang to Go Module Mapping Table

This document maps the classic Erlang/OTP broker concepts to their GroveMQ Go packages. It serves as a high-level architectural guide to the codebase.

| Erlang/OTP Concept | Reference Module             | Go Target Module                       | Remarks                                                                                                                      |
| ------------------ | ---------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `gen_server`       | `broker`                | `pkg/broker`                           | The central broker logic, managing message routing and topics. Will be a main supervised process.                            |
| `gen_server`       | `session`               | `pkg/session`                          | Manages the lifecycle of a single client session. Will be an actor supervised by the broker.                               |
| `gen_server`       | `connection`            | `pkg/transport` + `pkg/connection`     | Handles the raw TCP/WS connection. The transport layer accepts connections and spawns connection actors.                     |
| `supervisor`       | `sup`                   | `pkg/supervisor`                       | The root supervisor for the application, implementing a `one_for_one` strategy as a starting point.                        |
| `gen_server`       | `cm`                    | `pkg/cluster`                          | Cluster Manager. Handles node state, peer connections, and cluster-wide routing via gRPC.                                  |
| `gen_server`       | `router`                | `pkg/topic`                            | Manages the topic trie and subscription lookups. The `topic` package will contain this logic.                              |
| `gen_server`       | `listeners`             | `pkg/transport`                        | Listens for incoming network connections (TCP, TLS, WS) and passes them to connection actors.                              |
| `mnesia/ETS`       | `persistent_session_ds` | `pkg/storage` + `pkg/session`          | Manages durable sessions. The Go version will use a pluggable storage backend (e.g., Redis/Postgres) via the `pkg/storage` interface. |
| `mnesia/ETS`       | `shared_sub`            | `pkg/topic` + `pkg/storage`            | Manages shared subscription state. This will be part of the topic management and may require a distributed data store.     |
| `mnesia/ETS`       | `authz_cache`           | `pkg/auth` + `pkg/storage`             | Caching layer for authorization rules. A Go implementation would use an in-memory cache like Ristretto or Redis.         |
| `gen_server`       | `access_control`        | `pkg/auth`                             | Handles the core authorization logic (ACLs). A new `auth` package will be created for this.                                |
| `callback module`  | `hooks`                 | `pkg/hooks`                            | Implements a pub/sub system for internal events, allowing for extensibility.                                               |
| `gen_server`       | `alarm`                 | `pkg/monitoring`                       | Manages system alarms. Will be implemented using Prometheus alerts. A new `monitoring` package will house this.            |
| `gen_server`       | `metrics`               | `pkg/monitoring`                       | Collects and exposes system metrics, likely using the Prometheus client library for Go.                                    |
| `gen_server`       | `grovemq_cluster`               | `pkg/cluster` + `pkg/discovery`        | Handles node discovery (e.g., via K8s) and gRPC communication for clustering.                                              |