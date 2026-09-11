---
scope: package
summary: Orchestrates entry-independent message permission, append submission, ordinary history synchronization, and message-event projection.
---

# internal/usecase/message Flow

## Responsibility

This package coordinates message SEND admission and compatible ordinary history
reads through injected ports. It also owns message-event use-case handling,
without owning gateway frames, cluster transport, or durable storage engines.

## Boundaries

- Submitter owns Channel-authority routing and append admission.
- Permission and membership ports supply current business authority.
- The reader supplies committed message pages; the use case applies ordinary
  membership visibility and result semantics.
- Person-directory preparation and SEND hooks are explicit dependencies.
- CMD synchronization remains separate from ordinary history.

## Main Flows

1. Validate and authorize SEND items, including terminal Channel checks.
2. Prepare required person-directory state and invoke any accepted SEND hook.
   Mandatory application admission, when configured, follows payload-mutating
   plugins and cannot be skipped by plugin flags or fail-open behavior. Only a
   verified service entry may bypass content admission for command/transient
   notifications; device command/transient SEND is rejected in this mode.
3. Submit admitted items through the append port and return aligned results.
4. For history, authorize membership and visibility bounds before invoking the
   reader; preserve latest-page and bounded-cursor semantics.
5. Read or append message-event projections through the event store.
6. Restore reset discards permission-cache state owned by this facade.

## Invariants and Failure Semantics

- Trusted-system permission bypass does not bypass terminal disband.
- Committed identities come from the submitter, not a use-case sequence counter.
- Ordinary and one-shot command messages retain separate semantics.
- Missing authority and unavailable reads are not valid empty history.
- Payload-correction metadata remains outside application message content.
- Admission canonicalizes one logical request to stable bytes before append.
  Native idempotency, payload hashes, durable quorum, and sequence allocation
  continue to operate on those exact bytes; no asynchronous projection is needed
  for client display.

## Read First

- [Dependencies and facade](app.go)
- [SEND orchestration](send.go)
- [Permission decisions](permission.go)
- [History synchronization](sync.go)
- [Message-event handling](event.go)

## Update Triggers

Update this guide when submission, permission, membership, visibility, sync
cursor semantics, event handling, or restore-cache ownership change.
