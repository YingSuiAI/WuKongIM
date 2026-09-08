---
scope: subtree
summary: Loads benchmark scenarios, plans deterministic worker assignments, drives bounded workloads, and produces evidence-based reports.
---

# internal/bench Flow

## Responsibility

This subtree implements benchmark configuration, planning, coordination,
workers, protocol clients, metrics, and reports. Specialized scenario runners
live alongside the shared workload path.

## Boundaries

- Shared scenario and plan data live in pkg/bench/model.
- The target adapter and protocol clients exercise the product; they do not
  replace product routing, permission, or storage authority.
- Worker assignment identity separates one execution from later executions
  that reuse a scenario name.
- Reports describe the observed run and its evidence, not unconditional
  production capacity.

## Main Flows

1. Load target, scenario, and worker configuration and validate static input.
2. Build deterministic identity, Channel, traffic, and worker assignments.
3. Preflight the target and workers, then coordinate prepare, connect, warmup,
   measured run, and cooldown.
4. Workers run their assigned workload, record observations, and drain
   assignment-owned connections and receive verification.
5. Aggregate reports, enforced limits, and bounded diagnostics into a terminal
   result.

## Invariants and Failure Semantics

- Delayed control requests cannot act on a different assignment generation.
- Worker, target, configuration, cancellation, and evidence failures retain
  distinct classifications.
- Traffic, retries, diagnostics, and cleanup are bounded by their owning
  configuration and runner.
- Missing terminal evidence is not a passing workload result.

## Read First

- [Configuration loading](config/config.go)
- [Deterministic planner](planner/planner.go)
- [Coordinator lifecycle](coordinator/run.go)
- [Worker control](worker/server.go)
- [Reports and classifications](report/report.go)

## Update Triggers

Update this guide when scenario loading, assignment identity, workload phases,
target interaction, verification, diagnostics, or report semantics change.
