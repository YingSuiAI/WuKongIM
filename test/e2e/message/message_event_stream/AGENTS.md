# message_event_stream Scenario

This scenario proves typed message-event buffering, follower forwarding, and
snapshot recovery after a Slot leader change through public `cmd/wukongim` HTTP APIs.

## Rules

- Start real single-node and static three-node clusters through `test/e2e/suite`.
- Use only public HTTP APIs and public `/metrics` samples.
- Do not import `internal/*` packages or inspect local storage directly.
- Use the service-only `/message/events:append` endpoint with typed
  `delta`, `snapshot`, and `finish` payloads.
- Before `finish`, cache-only events must not advance the Slot FSM cursor; after
  a Slot leader change, each lane must be restored from a compact snapshot.
- Recovery reads the compact event snapshot attached to the committed anchor message.

## Run

```bash
GOWORK=off go test -tags=e2e ./test/e2e/message/message_event_stream -count=1 -timeout 2m
```
