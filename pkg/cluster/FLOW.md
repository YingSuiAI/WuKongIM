---
scope: subtree
summary: Composes the cluster Node facade, authoritative routing, Slot proposals, Channel services, maintenance, and cluster lifecycle.
---

# pkg/cluster Flow

## Responsibility

This subtree exposes the Node lifecycle and cluster facade, with supporting
control, routing, transport, proposal, and Channel services. It coordinates
their background reconciliation and maintenance.

## Boundaries

- Controller owns topology intent; Slot owns replicated metadata; Channel owns
  replicated message logs.
- Internal business use cases and entry protocols stay outside this package.
- Facade methods distinguish node-local storage inspection from routed
  authoritative reads and mutations.
- A single-node cluster follows the same cluster ownership paths.

## Main Flows

1. Construct configured resources, start control/transport/data runtimes, and
   publish route and readiness observations.
2. Route keys to hash slots and their owning Slot Raft groups.
3. Submit metadata proposals through the Slot path and message operations
   through Channel services.
4. Serve committed proof, bounded scans, and current-body correction reads
   without changing original SEND/log identity.
5. Coordinate topology work, retention, snapshots, restore maintenance, and
   shutdown through their dedicated facades.

## Invariants and Failure Semantics

- Route, leader, epoch, and lifecycle observations fence the owning operation.
- Correction reads use quorum/durable-apply authority; missing authority is not
  an uncorrected-body fallback.
- Payload correction is create-only metadata and does not allocate a message
  ID or sequence.
- Maintenance separates foreground admission from restore-owned operations.
- Role and status snapshots are observations, not interchangeable substitutes
  for committed data or a read barrier.

## Read First

- [Node facade and dependencies](node.go)
- [Lifecycle](node_lifecycle.go)
- [Default Slot composition](default_slots.go)
- [Payload correction and claim authority](node_payload_correction.go)
- [Restore facade](node_restore.go)

## Update Triggers

Update this guide when Node composition, routing, proposal authority, Channel
integration, correction reads, topology, retention, or maintenance changes.
