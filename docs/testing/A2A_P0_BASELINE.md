# A2A P0 performance baseline

This document records the baseline measurements for the three hot paths touched
by the A2A Foundation P0 work. Later Registry work (schema validation,
`a2a-status` annotation, task topics) must be compared against these numbers to
detect regressions.

## How to reproduce

```bash
go test -run '^$' -bench BenchmarkA2ABaseline -benchmem ./tests/load/
```

The benchmarks are self-contained (in-memory storage backend, no external
broker or network). Run them on an idle machine; absolute numbers vary by
hardware, so the useful signal is the relative change between runs.

## Measurements

Recorded on 2026-11-24 (darwin/amd64, 16 logical CPUs, Go 1.27.3):

| Benchmark | Scenario | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| `BenchmarkA2ABaseline_RetainedStore` | Retained PUBLISH with two v5 user properties (`a2a-status`, `a2a-status-source`) | 6,442 | 1,211 | 16 |
| `BenchmarkA2ABaseline_RetainedLoad` | Subscription-time delivery of 200 retained agent cards via `$a2a/v1/discovery/+/+/+` | 202,625 | 237,410 | 1,013 |
| `BenchmarkA2ABaseline_IdentityHook` | A2A discovery Client ID ↔ topic decision on the synchronous PUBLISH hook | 160 | 72 | 2 |

## Interpretation

- The identity hook adds ~160 ns to a discovery PUBLISH and is zero-cost for
  all other topics (it returns before any parsing).
- Retained store cost is dominated by the defensive copy of user properties;
  this is intentional so later status annotation cannot mutate stored state.
- Retained load scales with the number of matching cards (~1 µs per card in
  this run) and is the path to watch once a Registry maintains thousands of
  agents.

## Regression gate

A change that increases any of the above by more than 20% without a documented
reason should be treated as a regression. Re-run and update this table when the
hardware or Go version changes.
