---
scope: package
summary: Builds bounded UID-owned conversation pages and coordinates current membership, head hydration, badge floors, hide, and activation.
---

# internal/usecase/conversation Flow

## Responsibility

This package owns entry-independent conversation listing, retry hydration,
unread state, hiding, and explicit activation. The directory is derived from
UID-owned ordinary memberships and current Channel-head information.

## Boundaries

- Directory and mutation ports own durable per-user membership state.
- The live-membership authority confirms non-person candidates against
  Channel-owned subscriber facts.
- Head hydration is a separate port; the use case does not read message
  storage or choose concrete cluster routes.
- CMD discovery and message SEND belong to their own use cases.

## Main Flows

1. Read one bounded membership-index page for a UID and cursor.
2. Confirm live membership and reconcile stale revoked projections.
3. Hydrate authorized candidates with aligned Channel-head results.
4. Return visible conversations, deletes, unresolved identities, and coverage.
5. Retry a bounded unresolved set without rewinding directory coverage.
6. Clear/set unread, hide, and activate through the per-user mutation port.

## Invariants and Failure Semantics

- A completed pass, not a partial page, establishes directory coverage.
- Retryable hydration does not become false deletion or silent absence.
- Read, hide, join, and retention floors participate in visible-message and
  unread calculation.
- Mutation identity remains the selected UID and Channel; failures do not
  authorize changing the target.

## Read First

- [Application ports and list/retry](app.go)
- [Request and result types](types.go)
- [Unread, hide, and activation](unread.go)
- [Membership-directory regression cases](membership_list_test.go)

## Update Triggers

Update this guide when membership authority, pagination/coverage, hydration
outcomes, visibility floors, unread state, hide, or activation semantics change.
