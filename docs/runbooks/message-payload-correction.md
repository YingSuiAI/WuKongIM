# Immutable message payload corrections

This provider capability repairs the current application body of one already
committed ordinary message. It is not public message editing, SEND replay, or an
Agent stream event. All requests use the configured API service bearer token.
No deployment or production repair is authorized by this document.

## Deployment boundary

The durable addition is metadata table 18 (key: Channel ID/type/message sequence,
rowcodec version 1) and Slot FSM command 66. Existing Channel log records, message
indexes, FNV payload hashes, proposal identities and HW are unchanged. Registered
metadata snapshot/restore includes corrections and their retention deletions.

Before the first correction, upgrade every replica and every public read ingress
to a binary supporting this table/command and current-body projection. Do not
submit command 66 to a mixed-version Slot quorum. After enabling corrections,
rollback to a binary that omits the projection is not a safe content rollback;
remain on a correction-aware binary. A backup restored to a pre-correction point
in time naturally contains the pre-correction body and must not be mistaken for
the accepted post-repair state.

## Create and exact replay

`POST /channel/message-payload-correction` requires `operation_id`, `channel_id`,
`channel_type`, `message_id`, `message_seq`, `from_uid`, `client_msg_no`,
`expected_original_payload_sha256`, `corrected_payload_sha256`, and base64
`corrected_payload`. Operation IDs are opaque nonempty UTF-8 up to 256 bytes;
the nonempty corrected body is at most 256 KiB. Digests are lowercase SHA-256 hex.

The provider verifies the original committed row and exact sender/client tuple,
then serializes a create-only Slot row. HTTP 200 returns the same immutable tuple,
`operation_id`, `original_payload_sha256`, `corrected_payload_sha256` and
`applied` (true only for creation). Lost responses may retry the identical request;
never mint a new operation or substitute another message. HTTP 400 is malformed
input, 404 is absent/uncommitted/retained-away original, 409 is immutable identity
or existing-correction conflict, and 503 is unavailable authority.

## Original claim and current reads

`POST /channel/committed-message-claim` requires `channel_id`, `channel_type`,
`from_uid`, `client_msg_no`, and `expected_original_payload_sha256`. It looks up
the original durable SEND index at the current Channel Leader and verifies HW.
It neither invokes SEND nor allocates an ID/sequence. It returns
`{"state":"committed","message":...}` or `{"state":"not_committed"}`. The latter
does not authorize resending a previously ambiguous request. Original-digest
conflict returns 409; uncertain authority returns 503.

Current service point/scans and user history return only the corrected body,
with optional outer `payload_correction` containing operation/original/corrected
digests. The body itself receives no provider control fields. Conversation heads
and manager body reads use the same projection. Failure to read correction
authority fails the body read; it must never return the stale original as fallback.
Raw replication, SEND idempotency, Agent anchor validation and backup logs retain
the original bytes. Retention and terminal Channel deletion remove corrected
content in their owning metadata commit; snapshots cannot resurrect it.

Correction reads use Raft's quorum `ReadIndex` and wait for durable FSM apply,
not a cached leader role. Permission facts, ordinary membership/history floors,
and gateway device-credential reads use the same Slot barrier. A network-isolated
prior leader returns an error instead of an older acknowledged value. Correction
metadata, retention and terminal flags are read from one storage snapshot, so a
concurrent purge cannot be mistaken for a message that was never corrected.

An accepted correction remains owned by the Slot proposal lifecycle after an
HTTP waiter cancels. Cancellation is not proof of rollback: retry only its exact
immutable operation. Restore admission cannot pass an accepted correction still
waiting for FSM apply; ordinary restore's cohort and log-reload rules still apply.

## Local verification

- `GOWORK=off go test -tags=integration ./pkg/slot/fsm -run '^TestPayloadCorrection' -count=1`
- `GOWORK=off go test -tags=integration ./pkg/cluster -run '^(TestPayloadCorrection|TestAuthoritativeMetadataRead)' -count=1`
- `GOWORK=off go test -race -tags=integration ./pkg/slot/multiraft -run '^TestReadBarrier' -count=1`
- `GOWORK=off go test -tags=e2e ./test/e2e/message/payload_correction -count=1 -timeout=3m -p=1`

The process test uses a real three-node cluster with 256 hash slots, public SEND,
protected correction/claim requests, every published point/scan/history body,
actual Slot leader transfer, node restart and original SEND receipt replay.
The lower-level integration checks independently verify raw index FNV and payload,
unchanged committed head, real Slot compaction/restart, and retention deletion.
