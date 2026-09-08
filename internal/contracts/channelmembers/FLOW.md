---
scope: package
summary: Defines stable derived member-list identities and the narrow live-membership authority contract shared by use cases.
---

# internal/contracts/channelmembers Flow

## Responsibility

This package owns the dependency-light namespace for derived allowlist,
denylist, and temporary member rows, plus the live-membership authority port.

## Boundaries

- Namespace helpers derive identities without storage or network access.
- The authority interface is implemented elsewhere; these contracts do not
  decide permissions or mutate membership directly.
- Entry protocols, concrete cluster implementations, and storage remain
  outside the package.

## Main Flows

1. A logical Channel key selects the allowlist or denylist namespace.
2. Temporary membership uses its dedicated derived namespace.
3. A consumer submits aligned UID-owned membership candidates to the live
   authority port before exposing Channel data.
4. The implementation reports current subscriber facts and can tombstone a
   revoked stale projection through the separate mutation method.

## Invariants and Failure Semantics

- Derived Channel IDs preserve the existing namespace and encoded original
  identity.
- Membership results stay aligned with the submitted candidates.
- Channel existence, subscriber status, terminal state, mutation version, and
  read failure remain separate returned facts.

## Read First

- [Derived namespace](channelmembers.go)
- [Live authority contract](authority.go)

## Update Triggers

Update this guide when namespace encoding, candidate identity, result alignment,
authority facts, or stale-projection repair contracts change.
