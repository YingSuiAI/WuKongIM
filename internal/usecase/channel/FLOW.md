---
scope: package
summary: Orchestrates channel metadata, ordinary membership, derived member lists, versioning, projection, and bounded iteration.
---

# Channel Use Case Flow

## Responsibility

This package owns entry-independent channel metadata and membership policy,
including ordinary subscribers, temporary lists, allowlists, denylists,
mutation versions, bounded page or chunk iteration, and the trusted-service
committed-head, exact committed-message proof, and committed-message recovery
page reads.
It does not own entry protocols, concrete storage, cluster transport, or caches.

## Boundaries

- Storage ports represent cluster-authoritative Slot metadata; a single-node
  cluster uses the same path.
- Exact create, patch, membership, and non-empty operations fail closed without
  their narrow store capability and never scan a 100,000-member list as a
  fallback.
- The committed-head read grants no message-content permission and delegates to
  the cluster-authoritative Channel Leader through a narrow optional store port.
- The committed-message proof is a separate service-only point capability: it
  returns one exact provider tuple without current membership and cannot list
  history or fall back to a membership-backed reader.
- The committed-message recovery page is service-only, single-Channel, bounded
  to 100 rows, and fenced by the committed head returned on its first page. It
  is not a public history reader, client cursor, or mutation version.
- HTTP, gateway, cluster, concrete storage, and runtime cache types remain
  outside the use case.

## Main Flows

1. Metadata commands create, patch, upsert, or delete through conditional
   exact-store operations while preserving fields outside the requested patch.
2. Ordinary subscriber mutation updates durable membership, projects the same
   logical version into the UID membership index, refreshes the large-group
   flag, then notifies the observer with cloned final state.
3. Allowlist, denylist, and temporary members use stable derived channel IDs
   that preserve the legacy internal namespace; counted mutations require the
   parent and return exact requested and durable set-change counts.
4. A trusted service may read one scalar committed sequence for an exact
   Channel identity; an unknown or empty Channel has sequence zero, while an
   unavailable authority remains an error.
5. A trusted service may prove one exact committed `(message_id,message_seq)`
   tuple. Unknown, mismatched, uncommitted, or retained-away content is absent;
   authority uncertainty remains an error.
6. A trusted service may scan one Channel in ascending sequence order within a
   fixed committed-head snapshot. Sparse sequences advance normally; logical
   retention returns an explicit gap and first-available sequence even when no
   retained row remains. If retention advances beyond an older scan head, the
   terminal page is empty with `retention_gap=true`, not an unavailable error.

## Invariants and Failure Semantics

- Only ordinary subscribers create user-channel membership projection rows and
  observer events.
- Reset removes the old snapshot and adds the replacement under one mutation
  version.
- Observer notification occurs only after durable mutation, projection, and
  large-group refresh succeed.
- First allowlist or denylist add may create its derived channel; removal from
  a missing list is a zero-change no-op.

## Read First

- [Application service](app.go)
- [Committed-head port](committed_head.go)
- [Committed-message proof port](committed_message.go)
- [Committed-message recovery port](committed_messages.go)
- [Import boundary](import_boundary_test.go)

## Update Triggers

Update this file when channel commands, store extensions, member-list encoding,
versioning, membership projection, committed-head or recovery-scan semantics,
large-group policy, or observation changes.
