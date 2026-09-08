---
scope: package
summary: Adapts product HTTP requests to injected use cases and exposes configured health, diagnostics, benchmark, and Demo surfaces.
---

# internal/access/api Flow

## Responsibility

This package owns the product HTTP listener, routes, request/response mappings,
and entry validation. It delegates message, Channel, user, conversation, and
maintenance operations to dependencies supplied by the composition root.

## Boundaries

- Business policy and durable mutation belong to the injected use cases.
- Gateway protocol handling, cluster routing, and storage are outside this
  package.
- Service-only committed reads and payload correction use service-token
  authentication; optional diagnostic and benchmark routes have their own
  configured admission.
- Restore maintenance middleware fences product requests before handlers run.

## Main Flows

1. Construct the server with its use cases, authentication configuration, and
   optional observability or benchmark providers.
2. Decode a route-specific request and map it to the corresponding use case.
3. Convert the result to the compatible HTTP representation, preserving
   committed identities and separating absence from unavailable authority.
4. Serve embedded Demo assets and configured operational endpoints through
   their dedicated handlers.

## Invariants and Failure Semantics

- Missing required capabilities fail rather than falling back to direct storage.
- Payload correction and committed-claim endpoints do not invoke SEND.
- Corrected bodies carry outer proof metadata; application payloads do not
  acquire provider control fields.
- Correction and claim failures use stable response categories rather than
  returning internal causes.

## Read First

- [Server composition and middleware](server.go)
- [Message SEND adapter](message_send.go)
- [Channel routes](channel_management.go)
- [History mapping](channel_messagesync.go)
- [Correction and claim entrypoints](message_payload_correction.go)

## Update Triggers

Update this guide when routes, authentication, maintenance admission, optional
endpoint exposure, request decoding, or response mappings change.
