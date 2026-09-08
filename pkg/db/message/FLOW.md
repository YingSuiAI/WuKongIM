---
scope: package
summary: Owns channel-scoped message storage, append identities, indexes, committed reads, retention, and portable message snapshots.
---

# pkg/db/message Flow

## Responsibility

This package implements durable Channel message storage behind MessageDB and
ChannelLog handles. It owns row codecs, indexes, append/checkpoint state,
idempotency data, retention, and message snapshot import/export.

## Boundaries

- Channel replication and cluster authority select the work and committed cuts.
- This package persists and reads those records; it does not decide product
  permissions, routing, or online delivery.
- Engine-specific implementation remains under pkg/db/internal.
- Durable layouts follow the shared schema-compatibility rules.

## Main Flows

1. Acquire a canonical Channel handle through the registry and operation guard.
2. Validate append identities and sequence continuity, then commit message
   rows, indexes, and catalog changes before publishing append progress.
3. Read forward, reverse, point, or latest views with their explicit bounds.
4. Apply follower data, checkpoints, and retention/truncation through the
   corresponding storage paths.
5. Pin exact backup views and import verified snapshots into the selected
   storage state.

## Invariants and Failure Semantics

- Primary records and secondary indexes share their owning atomic writes.
- Idempotency and proposal evidence retain original durable message identity.
- Retention and truncation preserve the consistency of rows, indexes, catalog,
  and checkpoint state.
- Closing the domain rejects new work and drains active operations and pins.
- Storage does not manufacture quorum commitment from a caller's desired state.

## Read First

- [Domain lifecycle and registry entry](db.go)
- [Append path](append.go)
- [Read paths](read.go)
- [Backup snapshot](backup_snapshot.go)
- [Schema compatibility](../SCHEMA_COMPATIBILITY.md)

## Update Triggers

Update this guide when handle ownership, row/index layout, append identity,
checkpoints, reads, retention, snapshot import/export, or lifecycle changes.
