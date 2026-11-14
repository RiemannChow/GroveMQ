# GroveMQ Documentation

Welcome to the GroveMQ documentation. This directory contains all project documentation organized by category.

> 📖 Root READMEs: [English](../README.md) · [简体中文](../README.zh-CN.md)

## 📂 Directory Structure

```
docs/
├── chaos/              # Chaos engineering tooling index
├── guides/             # User and operation guides
├── testing/            # Testing documentation
├── reports/            # Test reports and status
├── planning/           # Project planning and TODOs
├── phase-0/            # Baseline metrics and module map
├── phase-1..3/         # Per-phase risk registers
├── phase-4/            # Data migration plan
├── final-migration-plan.md
├── mqttx.md            # MQTTX client notes
└── README.md           # This file
```

## 🔥 Chaos Engineering (`chaos/`)

- [Chaos Engineering Index](chaos/README.md) — the chaos CLI, dashboard, pod-kill script, CI workflows and how to run scenarios locally.

## 📖 Guides (`guides/`)

User guides and operational documentation.

| Document | Description |
|----------|-------------|
| [DEPLOYMENT.md](guides/DEPLOYMENT.md) | Deployment guide for production |
| [CONFIG_GUIDE.md](guides/CONFIG_GUIDE.md) | Configuration reference |
| [TROUBLESHOOTING.md](guides/TROUBLESHOOTING.md) | Troubleshooting and FAQ |
| [mqttx.md](mqttx.md) | Connecting with the MQTTX client |

## 🧪 Testing (`testing/`)

Testing documentation including cluster tests, concurrency tests, and E2E tests.

| Document | Description |
|----------|-------------|
| [CLUSTER_TESTING_GUIDE.md](testing/CLUSTER_TESTING_GUIDE.md) | Cluster testing guide |
| [CLUSTER_TEST_SUMMARY.md](testing/CLUSTER_TEST_SUMMARY.md) | Cluster test summary |
| [CLUSTER_TEST_IMPROVEMENTS.md](testing/CLUSTER_TEST_IMPROVEMENTS.md) | Test improvements |
| [CLUSTER_E2E_TEST_REPORT.md](testing/CLUSTER_E2E_TEST_REPORT.md) | E2E test report |
| [CONCURRENCY_TESTING_PLAN.md](testing/CONCURRENCY_TESTING_PLAN.md) | Concurrency testing plan |
| [e2e.md](testing/e2e.md) | E2E testing documentation |

## 📊 Reports (`reports/`)

Test reports, status updates, and historical records.

| Document | Description |
|----------|-------------|
| [PROJECT_STATUS.md](reports/PROJECT_STATUS.md) | Current project status |
| [IMPROVEMENTS_SUMMARY.md](reports/IMPROVEMENTS_SUMMARY.md) | Summary of improvements |
| [IMPROVEMENTS.md](reports/IMPROVEMENTS.md) | Detailed improvements log |
| [defensics-report.md](reports/defensics-report.md) | Defensics security report |
| [mbfuzzer.md](reports/mbfuzzer.md) | MB fuzzer report |

## 📋 Planning & Design

| Document | Description |
|----------|-------------|
| [PLAN.md](planning/PLAN.md) | Phased implementation plan |
| [TODO.md](planning/TODO.md) | TODO list and tasks |
| [GIT_COMMIT_MESSAGE.md](planning/GIT_COMMIT_MESSAGE.md) | Git commit message guidelines |
| [baseline_metrics.md](phase-0/baseline_metrics.md) | Performance baseline procedure |
| [mapping_table.md](phase-0/mapping_table.md) | Module / package architecture map |
| [migration_plan.md](phase-4/migration_plan.md) | Data migration strategy |
| [final-migration-plan.md](final-migration-plan.md) | Production cutover plan |
| [RISK.md](phase-1/RISK.md), [phase-2](phase-2/RISK.md), [phase-3](phase-3/RISK.md) | Per-phase risk registers |

## 🚀 Quick Navigation

### For New Users
1. Start with the [English README](../README.md) or [简体中文 README](../README.zh-CN.md)
2. Follow the Getting Started section in the root README
3. Check [DEPLOYMENT.md](guides/DEPLOYMENT.md) for deployment

### For Developers
1. Review [PLAN.md](planning/PLAN.md) and [mapping_table.md](phase-0/mapping_table.md)
2. Check [GIT_COMMIT_MESSAGE.md](planning/GIT_COMMIT_MESSAGE.md)

### For Operators
1. Read [DEPLOYMENT.md](guides/DEPLOYMENT.md)
2. Check [CONFIG_GUIDE.md](guides/CONFIG_GUIDE.md)
3. Review [TROUBLESHOOTING.md](guides/TROUBLESHOOTING.md)

### For Testers
1. Start with [CLUSTER_TESTING_GUIDE.md](testing/CLUSTER_TESTING_GUIDE.md)
2. Review [Chaos Engineering Index](chaos/README.md)
3. Check [CONCURRENCY_TESTING_PLAN.md](testing/CONCURRENCY_TESTING_PLAN.md)

## 📚 Documentation Stats

- **Markdown documents**: 27 files under `docs/` (plus 2 root READMEs)
- **Categories**: guides, testing, reports, planning, phase records, chaos
- **Languages**: English and Chinese (中文)

## 🔗 External Links

- [Project Repository](https://github.com/RiemannChow/GroveMQ)
- [MQTT Specification](https://mqtt.org/specifications/)
- [Chaos Engineering Principles](https://principlesofchaos.org/)
- [Chaos Mesh](https://chaos-mesh.org/)

## 📝 Contributing

When adding new documentation:

1. Place it in the appropriate category directory
2. Update this README.md with a link
3. Follow the existing naming conventions
4. Include proper headers and table of contents
5. Add cross-references where appropriate

---

For the main project README, see [../README.md](../README.md).
