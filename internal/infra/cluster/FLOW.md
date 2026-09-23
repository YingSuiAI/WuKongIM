---
scope: package
summary: Adapts internal ports to routed cluster and Channel capabilities while preserving authority, immutable identities, and typed results.
---

# internal/infra/cluster Flow

## Responsibility

This package implements internal ports using pkg/cluster and pkg/channel.
Its adapters cover append submission, metadata, history, conversation heads,
delivery/presence integration, and cluster-backed operational capabilities.

## Boundaries

- Business decisions remain in use cases; adapters map data, capabilities,
  routes, and errors.
- Concrete cluster and Channel types do not leak back into entry protocols.
- Node-local diagnostic reads and cluster-authoritative operations retain
  their distinct ownership.
- Missing required capabilities are failures, not permission to invent a
  storage or routing fallback.

## Main Flows

1. Translate an internal request into the narrow cluster or Channel operation.
2. Delegate append, metadata, and operational mutations through their existing
   authority paths.
3. For ordinary history, continue bounded routed committed reads past
   `SyncOnce` controls until a visible `limit+1` page or the requested bounds;
   preserve batch alignment and fail on scan budget exhaustion.
4. Apply the shared current-payload projection to visible messages before
   publishing bodies.
5. Map current messages, immutable references, correction proof, and aligned
   item outcomes back to the use case.
6. Operational adapters delegate retention, topology, diagnostics, and lifecycle
   work to their owning cluster capabilities.

## Invariants and Failure Semantics

- Current-body reads do not fall back to original bytes when correction
  authority is unavailable.
- Original append and idempotency semantics remain below the projection seam.
- Batch adapters preserve request/result alignment and item-scoped failures
  where the owning port supports them.
- Hidden control rows advance the raw history cursor but do not consume the
  visible page limit or prove `more=false`.
- Adapters do not allocate replacement committed message identities.
- Configured application ACK identity is extracted from the durable idempotency
  hit through an injected metadata reader, without reinterpreting the existing
  persisted payload hash as an original pre-admission request hash.

## Read First

- [Append adapter](appender.go)
- [Channel metadata adapter](channel_metadata.go)
- [History adapter](message_reader.go)
- [Current-payload projection](payload_correction.go)
- [Conversation hydration](conversation.go)

## Update Triggers

Update this guide when ports, concrete capability mappings, authority routing,
message projection, error translation, or operational adapter ownership change.
