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

## 6. Implementation status (P0 + P1)

This section tracks which parts of the contract are enforced by the current
code. P1 adds the opt-in Registry and its discovery/registration closed loop.

| Contract item | Status |
|---|---|
| Reserved `$` topics excluded from wildcard filters | Enforced (all four matchers) |
| Retained messages preserve MQTT v5 User Properties | Enforced |
| Discovery topic ↔ Client ID binding | Enforced |
| Single shared engine instances across broker/dashboard | Enforced |
| Agent Card schema validation | Implemented (strict + loose normalization, P1) |
| Registry index, online-status tracking and rate limits | Implemented (P1) |
| `a2a-status` annotation at delivery | Implemented (P1) |
| Admin API, CLI and dashboard discovery views | Implemented (P1) |
| Task state machine + artifact topics | Pending (P2) |
| HTTP JSON-RPC gateway | Pending (P2) |

P1 is configured through `broker.a2a_registry` and is disabled by default.
Enabling it registers the `a2a-registry` hook, attaches status annotations to
retained discoveries, and exposes `/api/v5/a2a/agents` and `/api/v5/a2a/stats`.
Configuration is startup-only; auth/ACL/hooks remain the only hot-reloaded
sections.

---

## 7. Compatibility notes

- EMQX 6.2: mountpoint-configured listeners are not compatible with A2A
  Registry unless the mountpoint is exactly one topic level. GroveMQ
  implementers should treat the same restriction until tested otherwise.
- Disabling A2A features (default) leaves all ordinary MQTT behaviour and
  performance unchanged.
