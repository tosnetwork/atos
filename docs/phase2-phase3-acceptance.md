# Phase 2 / Phase 3 Acceptance Gates

The implementation is accepted only when the exact proposed revision passes:

- `gofmt` verification;
- `go vet ./...`;
- `go test -race ./...`;
- clean PostgreSQL 16 migrations;
- real PostgreSQL failure-injection and restart/reconciliation tests;
- resumable/malicious streaming tests;
- earnings maturation and exact-once payout tests;
- dispute freeze/refund tests;
- provider adapter/schema/certification tests;
- signer rotation/revocation replay tests;
- open-task publish/apply/accept recovery tests.

Phase 0 and Phase 1 invariants remain unchanged and are part of the same regression suite.
