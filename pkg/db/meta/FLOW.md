---
scope: package
summary: Stores hash-slot metadata with typed tables, atomic batches, codecs, caches, inspection, and portable metadata snapshots.
---

# pkg/db/meta Flow

## Responsibility

This package owns node-local hash-slot metadata storage. Typed tables cover
identities, Channels, memberships, runtime metadata, tasks, event state, and
payload-correction records.

## Boundaries

- Slot FSM and cluster/proxy layers own replicated ordering and distributed
  read authority.
- MetaDB supplies local shards, table operations, batches, and pinned views;
  a local read alone is not a cluster-authority claim.
- Engine internals stay behind the storage abstraction.
- Table and codec changes follow the shared schema-compatibility document.

## Main Flows

1. Obtain a stable shard for a hash slot.
2. Validate typed values and encode primary rows, families, and indexes.
3. Stage related changes in an ordered hash-slot batch and publish them only
   after the owning durable commit.
4. Read local values or pinned snapshots and maintain applicable caches.
5. Export/import metadata row, index, and system spans; expose diagnostics
   through registered table descriptors.

## Invariants and Failure Semantics

- Durable table, key, column, and codec identities are compatibility-sensitive.
- Batch overlays preserve same-batch visibility and expected result semantics.
- Each versioned subscriber-set commit gets a strictly increasing Channel
  mutation version. The counted result reports that committed version, and
  unrelated Channel upserts cannot roll it back.
- Credential stale no-ops are per-command results; they must not fail another
  Slot's request sharing the physical commit. Hash-slot locks last through fsync.
- Correction bodies remain separate from original Channel message logs.
- Retention and terminal cleanup remove their owned correction rows.
- Ordinary tombstone-to-live transitions start a new visibility epoch only for
  rows without a Platform epoch. Platform-owned tombstones require a trusted
  service rejoin; the reducer cannot revive them through ordinary add.
- A trusted Platform rejoin sets the Platform epoch and joined floor after the
  Channel subscriber write, preserving higher user-owned read/hide floors.
  Exact same-epoch tombstone repair requires an explicit service flag and the
  original joined floor. The optional epoch tail is readable by new binaries
  on old rows; old binaries cannot decode rows after the first epoch write.
- Corrupt data, missing rows, expected conflicts, and storage failures retain
  their distinct meanings.

## Read First

- [MetaDB and shard ownership](db.go)
- [Batch behavior](batch.go)
- [Registered schemas](schema.go)
- [Metadata snapshots](snapshot.go)
- [Schema compatibility](../SCHEMA_COMPATIBILITY.md)

## Update Triggers

Update this guide when table ownership, codecs, batch visibility, indexes,
caches, correction retention, inspection, or snapshot behavior changes.
