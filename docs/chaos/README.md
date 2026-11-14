# Chaos Engineering

This directory documents the chaos / resilience tooling that actually ships with GroveMQ. It replaces an earlier index that referenced documents which were never committed — every link below points to a real file.

## What is in the repository

| Path | Purpose |
|------|---------|
| [`../../cmd/chaos-cli/`](../../cmd/chaos-cli/) | Unified CLI for running scenarios, launching the dashboard, generating reports and comparing baselines |
| [`../../cmd/chaos-dashboard/`](../../cmd/chaos-dashboard/) | Local web dashboard showing live chaos metrics (default port `8888`) |
| [`../../tests/chaos/kill_pod.sh`](../../tests/chaos/kill_pod.sh) | Randomly deletes one `app=grovemq` pod in a Kubernetes namespace to verify StatefulSet self-healing |
| [`../../.github/workflows/chaos-tests.yml`](../../.github/workflows/chaos-tests.yml) | Scheduled / manually triggered chaos workflow (local scenarios, Kind + Chaos Mesh, performance baseline) |
| [`../../.github/workflows/chaos-continuous.yml`](../../.github/workflows/chaos-continuous.yml) | Continuous chaos workflow with configurable scenarios and duration |
| [`../../scripts/health-check-simple.sh`](../../scripts/health-check-simple.sh) | Quick 3-node cluster health check |
| [`../../scripts/performance-test.sh`](../../scripts/performance-test.sh) | Performance / baseline load generation |
| [`../../scripts/generate-html-report.py`](../../scripts/generate-html-report.py) | Renders chaos results into an HTML report |

## Running scenarios locally

The local scenarios do not require Kubernetes. Build the CLI from the repository root and list what is available:

```sh
go run ./cmd/chaos-cli list
go run ./cmd/chaos-cli test baseline
go run ./cmd/chaos-cli test network-delay
```

Available scenarios (from `cmd/chaos-cli/main.go`): `baseline`, `network-delay`, `high-network-delay`, `network-loss`, `high-network-loss`, `combined-network`, `cpu-stress`, `extreme-cpu-stress`, `clock-skew`, `cascade-failure`.

Other useful commands:

```sh
go run ./cmd/chaos-cli status                       # current chaos test status
go run ./cmd/chaos-cli dashboard                   # live metrics dashboard on :8888
go run ./cmd/chaos-cli report <results-dir>        # generate an HTML report
go run ./cmd/chaos-cli baseline create <dir> <out> # capture a baseline
go run ./cmd/chaos-cli baseline compare <file> <dir>
```

## Kubernetes / Chaos Mesh

The scheduled workflows create a Kind cluster, install Chaos Mesh, deploy a GroveMQ cluster and inject faults. See [`chaos-tests.yml`](../../.github/workflows/chaos-tests.yml) for the full step list, and [`tests/chaos/kill_pod.sh`](../../tests/chaos/kill_pod.sh) for a minimal standalone pod-kill experiment.

## Cluster testing (related)

For non-chaos cluster verification, see the [testing guides](../testing/CLUSTER_TESTING_GUIDE.md) and the [cluster test tool](../../cmd/cluster-test/README.md).
