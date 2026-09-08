---
scope: package
summary: Composes configured cluster and node runtimes, use cases, adapters, workers, observability, and their coordinated lifecycle.
---

# internal/app Flow

## Responsibility

This package is the composition root for the product process. It wires cluster,
gateway, API, Manager, use cases, node-local workers, and observability from
configuration and explicit construction options.

## Boundaries

- Entry adapters receive use cases and ports here; they do not construct
  infrastructure themselves.
- Reusable runtime behavior remains in its owning runtime or pkg module.
- Configuration normalization, dependency ownership, startup, shutdown, and
  restore coordination are composition responsibilities.

## Main Flows

1. Normalize configuration, apply construction options, and create the selected
   infrastructure and observability dependencies.
2. Connect message submission, current-body readers, metadata authority,
   delivery, presence, plugins, webhooks, and operational use cases.
3. Start the cluster and required admission/readiness paths before exposing
   configured foreground entries and workers.
4. Roll back started resources on startup failure and coordinate shutdown.
5. Restore maintenance closes entry admission, drains affected work, resets
   sensitive caches, and resumes runtimes against restored state.

## Invariants and Failure Semantics

- A single-node cluster uses the same cluster composition boundary.
- Lifecycle state prevents duplicate start and preserves cleanup errors.
- Optional runtimes are wired explicitly rather than inferred by entry adapters.
- Restore coordination does not substitute cached pre-restore observations
  for the activated durable state.

## Read First

- [Composition types and construction](app.go)
- [Dependency wiring](wiring.go)
- [Lifecycle](lifecycle.go)
- [Backup composition](backup.go)
- [Restore coordination](backup_maintenance.go)

## Update Triggers

Update this guide when dependency wiring, resource ownership, startup readiness,
optional runtime composition, shutdown, or restore boundaries change.
