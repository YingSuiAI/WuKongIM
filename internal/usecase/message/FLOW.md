# internal/usecase/message Flow

## Responsibility

`internal/usecase/message` owns the entry-agnostic message facade,
legacy-compatible send permission checks, and compatible channel message sync.
Allowed SEND work is delegated to a configured `channelappend.Submitter`; this
package does not own channel authority routing, durable append, message ID
allocation, or post-commit delivery effects. It knows about permission metadata
ports, the channel message read port for sync, and the message event projection
store port, but not gateway frames, wire protocols, HTTP JSON, or concrete
cluster runtimes.

## SendBatch Flow

```text
SendBatch(items)
  -> group commands with identical permission-relevant sender/channel scope
  -> for ordinary group commands, when PermissionBatchStore is available and PermissionCacheTTL is zero:
       deduplicate sender metadata, group metadata, denylist, subscriber, and allowlist facts across the batch
       issue one raw authoritative permission batch while keeping policy evaluation in this usecase
       evaluate the legacy reason precedence independently for every permission scope
  -> check remaining independent permission scopes with at most 16 workers:
       normalize command-channel IDs to their source channel for permission checks
       normalize person-channel IDs when requested by the entry adapter
       if PermissionStore is nil, allow
       if sender is a system UID, authoritatively check source-channel Disband then bypass nonterminal checks
       check sender SendBan through the sender's person metadata row
       if DeviceID matches SystemDeviceID, authoritatively check source-channel Disband then bypass remaining nonterminal checks
       authoritatively reject Disband for every source channel type
       enforce group metadata, ban, subscriber, denylist, and allowlist checks
       enforce person receiver denylist and optional receiver allowlist/AllowStranger checks
       enforce agent participant and visitors/customer-service membership checks
       reject denied items with item-aligned Reason values
  -> coalesce permission-accepted persistent person commands by canonical channel ID
  -> establish each distinct person directory with at most 16 workers
       map one failed establishment back to every aligned item for that channel
  -> for each remaining permission-accepted item in original order:
       if SendHook is configured, run it before append admission
       reject hook-denied items with item-aligned Reason values
  -> if Submitter is nil, return ErrRouteNotReady for remaining allowed items
  -> delegate allowed items to Submitter.SendBatch
  -> copy delegated results back to original item indexes
```

`Send(ctx, cmd)` runs the same permission check and delegates allowed commands
directly to `Submitter.Send`. When configured, `SendHook.BeforeSend` runs after
permission success and before the submitter. It may mutate the command payload
or reject with a usecase `Reason`; it does not run for permission-rejected
items. Plugin-origin sends carry `Origin`/`HookDepth` recursion controls, and
trusted internal paths may set `SkipPluginHooks`.

`PermissionStore` implementations must support concurrent calls. Equivalent
permission scopes are evaluated once per `SendBatch` without merging their
commands, payloads, identities, or results. The optional `PermissionBatchStore`
returns only raw authoritative facts; deny/subscribe/allow and trusted-sender
policy order remains here. Its implementation groups facts by physical Slot so
one gateway batch uses at most one RPC per represented Slot. There is no
cross-batch cache or stale window. A configured positive `PermissionCacheTTL`
keeps the existing read-through cache path and disables this batch path. The
fixed 16-worker permission fallback prevents non-group and unsupported modes
from serializing independent reads. Person-directory establishment has its own
16-worker bound and coalesces a canonical person channel once per batch; hooks,
append admission, and final results retain original item order.

`SendBatchObserver` emits one low-cardinality latency observation for each
`permission`, `pre_append`, and `submitter` stage. It carries only result class,
item count, and duration—never UID, Channel, Slot, or entry-protocol data. The
stages distinguish authoritative permission fanout from hooks/directory work
and channelappend routing without changing synchronous ordering.
Prometheus attributes each stage duration to every input item so its quantiles
have the same per-message weighting as gateway handler and SENDACK latency;
slow large batches must not be underweighted as one sample.

The configured submitter is normally the app-level channel append router, which
resolves channel append authority and admits work into the authority node's
channel append reactor. Validation, request-scoped command-channel derivation,
message ID allocation, append retries, committed cursors, subscriber scan,
NoPersist realtime dispatch, PersistAfter hooks, and
online delivery are all owned by `internal/runtime/channelappend`.

Permission checks preserve legacy reason semantics while staying
entry-agnostic: `internal/access/gateway` and `internal/access/api` map the
usecase `Reason` values back to protocol reason codes at their boundaries.
`PermissionCacheTTL` optionally wraps the permission metadata port with a
bounded read-through cache for channel rows, subscriber point lookups,
subscriber-set non-emptiness, and missing channel rows. Terminal source-channel
checks bypass this cache, and the normal group path reuses that authoritative
channel read, so a cached live row cannot revive a disbanded channel and group
SEND does not add a second channel metadata read.

## SyncChannelMessages Flow

```text
SyncChannelMessages(query)
  -> validate login_uid, channel_id, and channel_type with legacy error strings
  -> canonicalize person-channel IDs using login_uid
  -> require a live UID-owned membership
  -> for non-person memberships, authoritatively batch-read Channel metadata
     plus subscriber membership before any committed-message read
     -> authority errors fail closed
     -> missing membership is rejected; when the authoritative subscriber
        mutation version is newer than membership source_version, best-effort
        tombstone the stale projection using that exact version
  -> for person memberships, require the canonical pair to contain login_uid
  -> clamp visibility to join_seq and deleted_to_seq
  -> reject terminally disbanded channels
  -> cap limit to the legacy maximum
  -> call ChannelMessageReader.SyncMessages with a normalized ChannelID
  -> treat missing channel runtime/storage as an empty page
  -> clone payloads before returning SyncedMessage values to access adapters
  -> when include_event_meta is set, batch-read MessageEventStore states by
     (channel_id, channel_type, client_msg_no) and attach compact event_meta
```

`SyncChannelMessagesBatch` accepts at most 200 items. It prepares and validates
every membership before issuing any Channel read, then calls the optional batch
reader once. The cluster implementation groups those reads by exact Channel
Leader and returns item-aligned pages/errors; one unavailable Channel need not
discard successful siblings. Neither single nor batch pull performs a
subscriber lookup or changes membership state.

The sync usecase returns `SyncedMessage` DTOs with the fields needed by the HTTP
response. Message event enrichment is page-batched for requested anchors with a
non-empty `client_msg_no` and capped per message so a high-limit sync request
does not return unbounded lane state. Removed legacy stream fields no longer
gate or duplicate event projection state.

## AppendMessageEvent Flow

```text
AppendMessageEvent(event)
  -> validate required channel, anchor, run, fence, sequence, event, and sender fields
  -> trim fields, lower-case event_type, default empty event_key to "main"
  -> canonicalize person-channel and agent-channel IDs using from_uid
  -> read the committed anchor and match channel/message/client/sender/run/fence
  -> stamp updated_at when absent
  -> call MessageEventStore.AppendMessageEvent
  -> return the original lane key and Platform authority sequence result
```

`AppendMessageEvent` leaves buffering policy to the configured store. The
cluster-backed store keeps `open`, `delta`, and `snapshot` updates in a bounded
Slot-leader cache. A `finish` event atomically flushes each run lane's compact
snapshot and advances the durable run cursor to the Platform-provided authority
sequence. If Slot leadership changes before finish and the new leader has
no cached lanes, the cluster store fails the finish closed instead of writing a
completed projection that silently drops cache-only deltas; callers must replay
the deltas or provide a complete finish snapshot before retrying finish.
Cache pressure is reported as typed backpressure rather than silently evicting
active streams.

The projection state key is `(channel_id, channel_type, client_msg_no, run_id,
event_key)`. `run_id` and the raw lane `event_key` remain independent values;
neither is concatenated or parsed with a delimiter.

## Message Event Projection Ports

`MessageEventStore` is the usecase boundary for message-scoped event
projections. It accepts one event update, may satisfy in-flight stream updates
from cache, returns the assigned message-level `msg_event_seq` once durable, and
reads compact event lane states in batch for `/channel/messagesync` enrichment.
The usecase DTOs are independent from the concrete Slot/metadb storage types;
cluster adapters perform mapping and payload cloning. Recovery reads compact
snapshots through message sync; raw event history is not stored.

## Import Boundary

The usecase package must remain independent from concrete entries and cluster
adapters. The import-boundary test rejects imports of:

- `pkg/gateway`
- `pkg/protocol/frame`
- `pkg/cluster`
- `pkg/channel`
- `internal/access`
- `internal/app`
