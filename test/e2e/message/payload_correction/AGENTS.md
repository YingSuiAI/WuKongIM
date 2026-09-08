# Payload correction scenario

This scenario owns real three-node process coverage for service-only immutable
message payload corrections. Use public SEND, correction/claim, history, point,
scan and manager endpoints; do not import application or storage internals.

Run `GOWORK=off go test -tags=e2e ./test/e2e/message/payload_correction -count=1 -timeout=3m -p=1`.

Verify correction replay/conflict, unchanged SEND identity/head, Slot leader
transfer, node restart and deletion. Diagnostics must exclude service tokens and
corrected content. The fixture uses only isolated loopback processes.
