# A2A over MQTT — GroveMQ Protocol Contract (v1)

This document freezes the wire contract GroveMQ implements for cross-framework
Agent discovery and task exchange. It is intended to be **broker-neutral**: the
topic names, MQTT v5 property mappings and payload shapes below are compatible
with EMQX 6.2's A2A Registry and with the layered A2A Protocol Specification
1.0.0 (Canonical Data Model / Abstract Operations / Protocol Bindings).

Anything that changes this contract requires a version bump and an update here
first. Implementation details live in code; this document is the source of
truth for interoperability.

---

## 1. Layering

| A2A layer | GroveMQ mapping |
|---|---|
| Layer 1 — Canonical Data Model (`spec/a2a.proto`) | `AgentCard`, `Task`, `Message`, `Part`, `Artifact`, `TaskState` carried as JSON in MQTT payloads |
| Layer 2 — Abstract Operations | `SendMessage`, `GetTask`, `CancelTask`, `SubscribeToTask` encoded as JSON-RPC style `method` values |
| Layer 3 — MQTT binding | Topic hierarchy + MQTT v5 `Response Topic` / `Correlation Data` / User Properties defined below |

The binding identifier advertised by an Agent Card is `A2A-over-MQTT` (a new
`protocolBinding` value proposed to the A2A community). For compatibility with
EMQX 6.2, `MQTT` and `JSONRPC` are accepted aliases during v1.

---

## 2. Topic hierarchy

All A2A topics are under the reserved `$a2a/` prefix. Per MQTT rules, a `#` or
`+` wildcard filter MUST NOT match topics beginning with `$`; an explicit
`$a2a/...` filter is required. GroveMQ enforces this in every matcher
(subscription routing, retained delivery, memory/bbolt storage backends).

### 2.1 Discovery (retained)

```
$a2a/v1/discovery/{org_id}/{unit_id}/{agent_id}
```

- Each segment matches `^[A-Za-z0-9._-]+$` and must be non-empty.
- Registration: QoS 1, `RETAIN=1`, payload = Agent Card JSON.
- De-registration: publish an empty payload with `RETAIN=1` to the same topic.
- The full three-part address must equal the publishing client's Client ID.
  GroveMQ rejects non-matching publishes with PUBACK/PUBREC reason code `0x87`.
- On subscription, the broker delivers retained cards with `RETAIN=1` and
  attaches status user properties (see §3).

### 2.2 Tasks and messages

```
$a2a/v1/{org_id}/{unit_id}/{agent_id}/requests
$a2a/v1/{org_id}/{unit_id}/{agent_id}/tasks/{task_id}/status
$a2a/v1/{org_id}/{unit_id}/{agent_id}/tasks/{task_id}/artifacts
$a2a/v1/{org_id}/{unit_id}/{agent_id}/inbox
```

- Requests carry `Response Topic`; `Correlation Data` carries `taskId` or
  `messageId` for pairing.
- Long-running tasks publish progress to the `status` topic and outputs to the
  `artifacts` topic. Consumers subscribe rather than poll.
- Work pools use MQTT shared subscriptions:
  `$share/{pool_id}/$a2a/v1/{org}/{unit}/{agent}/requests`.

---

## 3. MQTT v5 property mapping

| A2A concept | MQTT v5 carrier |
|---|---|
| Request/response pairing | `Response Topic` + `Correlation Data` |
| Agent availability | User Property `a2a-status` = `online` \| `offline` \| `lwt` |
| Status origin | User Property `a2a-status-source` = `broker` \| `agent` \| `lwt` |
| Protocol version | User Property `A2A-Version` = `1.0` |
| Extensions | User Property `A2A-Extensions` (comma-separated URIs) |
| Content type | `Content Type` = `application/a2a+json` |
| Semantic errors | PUBACK/PUBREC reason code + User Property `a2a-error` |

User Properties set on a retained PUBLISH are stored with the retained message
and re-delivered to later subscribers. This is required so status metadata can
be attached to cards at delivery time.

Status semantics: an explicit client DISCONNECT produces `a2a-status=offline`
with `a2a-status-source=agent`; an abrupt close produces `lwt`/`lwt`; a
broker-side disconnect (admin kick) or a broker health-check decision produces
`offline`/`broker`. A card published by an already connected client starts in
`online`/`broker`, so new subscribers observe the live state immediately.

---

## 4. Agent Card payload

A strict card follows A2A 1.0 and contains at least `name`, `description`,
`version`, `supportedInterfaces`, `capabilities`, `defaultInputModes`,
`defaultOutputModes` and `skills`. One `supportedInterfaces` entry should be:

```json
{
  "url": "mqtt://grovemq:1883",
  "protocolBinding": "A2A-over-MQTT",
  "protocolVersion": "1.0"
}
```

EMQX-style simplified cards (`name`, `description`, `version`, `url`, `skills`)
are accepted in compatibility mode and normalized by the Registry layer.

---

## 5. Task semantics

- Task states: `UNSPECIFIED`, `SUBMITTED`, `WORKING`, `COMPLETED`, `FAILED`,
  `CANCELED`, `INPUT_REQUIRED`, `REJECTED`, `AUTH_REQUIRED`.
- Terminal: `COMPLETED`, `FAILED`, `CANCELED`, `REJECTED`. Interruptible:
  `INPUT_REQUIRED`, `AUTH_REQUIRED`.
- A `Message` carries communication and control; task outputs MUST be carried
  in `Artifact` objects, never in a `Message`.
- Streaming subscriptions must be closed when a task reaches a terminal state.

---

## 6. Implementation status (P0 + P1 + P2)

This section tracks which parts of the contract are enforced by the current
code. P1 adds the opt-in Registry; P2 adds task exchange and SDK-compatible
JSON-RPC access.

| Contract item | Status |
|---|---|
| Reserved `$` topics excluded from wildcard filters | Enforced (all four matchers) |
| Retained messages preserve MQTT v5 User Properties | Enforced |
| Discovery topic ↔ Client ID binding | Enforced |
| Single shared engine instances across broker/dashboard | Enforced |
| Agent Card schema validation | Implemented (strict + loose normalization, P1) |
| Agent Card JWS verification with JKU allowlist | Implemented (P2, opt-in) |
| Registry index, online-status tracking and rate limits | Implemented (P1) |
| `a2a-status` annotation at delivery | Implemented (P1) |
| Admin API, CLI and dashboard discovery views | Implemented (P1) |
| Task state machine + artifact/status topics | Implemented (P2) |
| MQTT request/response correlation and PUBACK error mapping | Implemented (P2) |
| HTTP JSON-RPC gateway | Implemented (P2, opt-in) |
| A2A Prometheus metrics | Implemented (P2) |
| Unknown Agent Card fields preserved on round-trip | Enforced (`schema.AgentCard.Extra`) |
| Terminal stream tombstones reclaimed by TTL sweep | Enforced (`task.StreamHub.StartJanitor` / `SweepClosed`) |
| Task results rejected from Messages and forced into Artifacts | Enforced (`task.ValidateTaskResult` / gateway result check) |
| Multi-level broker mountpoint warning at startup | Implemented (`cmd/grovemq` A2A Registry startup check) |
| Online status for cards published after CONNECT | Enforced (`registry` live connection set seeds initial card state) |
| LWT status and broker-originated status decisions | Enforced (`classifyDisconnect`, `Registry.SetStatus`) |
| Unregistered agent task returns `TaskNotFoundError` | Implemented when the Registry is enabled (`gateway.AgentLookup`) |
| Disabled Registry is a no-op on all hook points | Enforced (`registry.Config.Enabled` guards) |
| Definition-of-Done contract tests | `tests/contract/a2a_dod_test.go`, `a2a_p1_test.go`, `a2a_p2_test.go` |
| Task request identity binding | Opt-in (`broker.a2a_request_auth.policy`); `hooks.A2ARequestAuthHook`, PUBACK `UnauthorizedError` (0x02) |
| HTTP gateway bearer authentication | Opt-in (`broker.a2a_gateway.tokens`); principal promoted to MQTT CONNECT user for policy binding |
| Streaming over MQTT status/artifacts topics | Implemented (`gateway.MQTTStreamBridge` subscribes before request, relays QoS 1 events into `task.StreamHub`) |
| HTTP `message/stream` SSE | Implemented (`gateway.NewStreamHandler` emits `text/event-stream`, closes on final event) |
| Registry warm-up from retained cards | Implemented (`Registry.RestoreCard`, broker `GetRetainedMessages`, startup warm-up) |
| Push Notification Config (4 methods) | Implemented (`task.PushStore` + `gateway.CompatibilityContext`, answered locally for API parity) |
| Get Extended Agent Card | Implemented (`Registry.ExtendedCard` from `x-extended-card`; capability-gated, auth required when tokens are set) |
| Task/Artifact persistence | Implemented (`task.Store` over the shared durable key-value store; mirror on success, offline `tasks/get` fallback) |
| Shared-subscription work pools | Implemented (`task.SharedRequestTopic` filter helper; broker `$share` round-robin verified end-to-end) |

P1 is configured through `broker.a2a_registry` and is disabled by default.
Enabling it registers the `a2a-registry` hook, attaches status annotations to
retained discoveries, and exposes `/api/v5/a2a/agents` and `/api/v5/a2a/stats`.
P2 adds `broker.a2a_gateway`, also disabled by default, which serves
`POST /a2a/v1/{org}/{unit}/{agent}` on the dashboard port. Configuration is
startup-only; auth/ACL/hooks remain the only hot-reloaded sections.

### 6.1 P2 task exchange contract

- Request topic: `$a2a/v1/{org}/{unit}/{agent}/requests` (QoS 1).
- Responses use MQTT 5.0 Response Topic and Correlation Data, not a shared
  response topic, so concurrent SDK calls are isolated.
- Streaming events use
  `$a2a/v1/{org}/{unit}/{agent}/tasks/{taskId}/status` and
  `$a2a/v1/{org}/{unit}/{agent}/tasks/{taskId}/artifacts`. A terminal task
  emits `final: true` and the subscription is closed.
- JSON-RPC envelopes are JSON-RPC 2.0 with content type
  `application/a2a+json`. SDK method aliases such as `sendMessage` and
  `getTask` are normalized to canonical A2A 1.0 method names.
- Semantic errors map to JSON-RPC codes and to MQTT PUBACK/PUBREC reason codes
  with an `a2a-error` User Property. MQTT has no per-error reason code, so the
  property is authoritative.
- Signed Agent Cards use flattened JWS. The signature covers the canonical
  card JSON with `signatures` removed; object keys are sorted and unknown
  extension fields are preserved. `jku` must match a configured prefix, and
  `kid`/`alg` select the JWKS key. Signature checks are off unless
  `require_security_metadata` or `allowed_jku_prefixes` is configured.
- Unknown top-level Agent Card fields are kept as raw JSON and re-emitted on
  reads, so a newer A2A version or an EMQX extension field is never silently
  dropped by normalization or the admin API.
- A result whose data/file parts are placed in `Message` (status message or
  history) is rejected at the stream and gateway boundaries; outputs MUST be
  attached as `Artifact` objects. Text-only control messages remain valid.
- In-process `StreamHub` instances expire terminal-task tombstones after
  `DefaultClosedTTL` (10 minutes) via `StartJanitor`; a late subscriber that
  supplies the current task still receives a final event.

### 6.2 A2A 1.0 optional methods

- `CreateTaskPushNotificationConfig`, `GetTaskPushNotificationConfig`,
  `ListTaskPushNotificationConfigs` and `DeleteTaskPushNotificationConfig` are
  answered by the gateway locally. Because MQTT subscriptions already deliver
  task updates natively, no HTTP webhook is invoked; the configs are stored in
  `task.PushStore` purely for SDK API parity. A request to an agent whose Card
  does not advertise `capabilities.pushNotifications` receives
  `PushNotificationNotSupportedError` (-32003).
- `GetExtendedAgentCard` is served from the `x-extended-card` extension of the
  signed Agent Card in the Registry. It requires the agent to advertise
  `capabilities.extendedAgentCard` and, when gateway bearer tokens are
  configured, an authenticated principal. A missing extension returns
  `ExtendedAgentCardNotConfiguredError` (-32007).
- Successful Task results are mirrored into `task.Store`, a prefix-scoped layer
  over the broker's shared key-value backend (`a2a/task/{address}/{taskId}`).
  Artifacts are persisted with the Task. While an agent is offline, `tasks/get`
  is answered from this store, so task state survives an agent restart. With
  `GROVEMQ_DATA_DIR` set the records are durable across broker restarts; without
  it the store is in-memory and behaves as a no-op-safe cache.
- Agent workers scale out by sharing the filter
  `$share/{pool_id}/$a2a/v1/{org}/{unit}/{agent}/requests`. Each request is
  delivered to exactly one pool member (broker round-robin), which preserves
  the A2A guarantee that one task is owned by one worker at a time.

---

## 7. Compatibility notes

- EMQX 6.2: mountpoint-configured listeners are not compatible with A2A
  Registry unless the mountpoint is exactly one topic level. GroveMQ
  implementers should treat the same restriction until tested otherwise.
- Disabling A2A features (default) leaves all ordinary MQTT behaviour and
  performance unchanged.
