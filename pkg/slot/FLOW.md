---
scope: subtree
summary: Runs Slot Raft groups, deterministically applies hash-slot metadata commands, and exposes routed metadata proxy operations.
---

# pkg/slot Flow

## Responsibility

This subtree implements distributed metadata storage: multiraft owns the local
Slot Raft groups, fsm applies metadata commands through pkg/db/meta, and proxy
offers routed metadata operations.

## Boundaries

- Controller/cluster decide placement, topology, and node lifecycle.
- Channel owns message logs and their replication; Slot does not produce
  SENDACK or perform online delivery.
- Entry adapters and product policy stay in their internal layers.
- Local reads, authoritative routed reads, and quorum read barriers have
  distinct contracts.

## Main Flows

1. Proxy resolves a stable entity key to its hash slot and owning Slot Raft group.
2. Encode and propose a typed metadata command through the cluster port.
3. Raft replicates committed work; the FSM checks hash-slot ownership and
   applies ordered commands in metadata batches.
4. Publish apply results after durability, preserving typed no-op/conflict
   outcomes where the command defines them.
   Subscriber-set results include the exact Channel mutation version assigned
   by the durable Slot commit so UID membership projection can use that fence.
5. Read barriers use safe ReadIndex and wait for durable FSM application.
6. Snapshot and restore the owned metadata spans through lifecycle and
   maintenance boundaries.

## Invariants and Failure Semantics

- Message storage is separate from replicated metadata state.
- Key routing and hash-slot ownership remain explicit during apply and moves.
- A fresh leader-role observation alone is not a quorum read proof.
- Cancellation does not retroactively erase an admitted proposal.
- Snapshot/restore and compaction retain durable apply and ownership evidence.

## Read First

- [Module boundary](BOUNDARY.md)
- [Proxy facade](proxy/store.go)
- [Raft runtime API](multiraft/api.go)
- [Read barrier](multiraft/read_barrier.go)
- [State-machine apply](fsm/statemachine.go)

## Update Triggers

Update this guide when Slot lifecycle, routing, command/application results,
read barriers, metadata ownership, snapshot/restore, or compaction changes.
