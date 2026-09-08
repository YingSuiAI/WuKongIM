# Latest history page scenario

This scenario exercises three real `cmd/wukongim` processes with 256 hash slots.
Create a subscriber-owned transport channel and commit more messages than the
requested page size. Query the latest page and bounded older pages through each
HTTP ingress, using SEND receipts as the independent sequence/identity oracle.

Run: `GOWORK=off go test -tags=e2e ./test/e2e/message/history_latest_page -count=1 -timeout=3m -p=1`.

Keep it black-box. Do not inspect storage or replace the production reader.
The shared suite owns process cleanup; keep failure diagnostics bounded.
