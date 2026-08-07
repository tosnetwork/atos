# ATOS

Implementation of the ATOS gateway described in `~/atos-spec` and
`~/TOS_ARCHITECTURE_V2.md`. Public contracts (REST, MCP, A2A, Agent Card)
are real and wired end to end; execution (tos-ai) and trust/settlement
(tos-core) are in-process mocks — not a real provider network or a real
settlement chain yet. See `review.codex.md` for an independent review of
an earlier pass and the fixes applied since.

## What's real vs. mocked

| Layer | Status |
|---|---|
| REST API (`docs/API.md`, `api/openapi.yaml`) | Implemented — capabilities, quotes, invocations, jobs, account/usage/receipts, taxonomy, network status, provider agent cards, device auth |
| MCP (`docs/MCP.md`) | Implemented — 10 default tools, 2 admin tools (unlisted but callable), 5 `atos://` resources, over JSON-RPC 2.0. Session resumability and true MRTR elicitation round trips are simplified; `input_required` + a `confirmed`-reissue of the same idempotency_key stands in for it |
| A2A (`docs/A2A.md`) | Implemented — `message/send`/`tasks/get`/`tasks/cancel` over JSON-RPC 2.0 at `POST /a2a`, sharing the same JobService as REST/MCP. Commerce fields ride in the `https://atos.im/extensions/commerce/v1` message metadata extension |
| Escrow / settlement state machine (`docs/SETTLEMENT.md`) | Implemented — reserve → verify → settle/release, enforced in code (`internal/adapters/toscore/mock`), not just documented |
| Persistence | Both real: `internal/store/memory` (Phase 0, zero setup) and `internal/store/postgres` (Phase 1, set `ATOS_DATABASE_URL`) implement the identical `store.Store` interface — swapping one for the other doesn't touch `internal/service` |
| tos-ai execution | Mocked (`internal/adapters/tosai/mock`) — echoes input back as output; swap for a real provider network in Phase 2 |
| tos-core trust/settlement | Mocked (`internal/adapters/toscore/mock`) — real state machine (escrow reserve/verify/settle/release), no chain commitment; swap in Phase 4 |
| Device Auth | Stubbed — always succeeds immediately, no real human verification step |
| Semantic search / ranking | Naive substring/ILIKE match — real ranking is a Phase 1+ concern |
| Provider job queue (`GET /provider/jobs`, `atos_provider_jobs`, ...) | Not implemented — Phase 0/1's only adapter type executes synchronously in-process; there is no real queued-provider model to back these yet |

None of the above requires changing `internal/service` or the public
REST/MCP/A2A contracts to fix later — that boundary is the point of this
codebase.

## Layout

```text
cmd/api/             entrypoint: wires store, adapters, services, HTTP mux
cmd/migrate/          applies migrations/*.sql to ATOS_DATABASE_URL
api/openapi.yaml      REST API spec, matches internal/httpapi exactly
migrations/           Postgres schema (Phase 1)
internal/domain/      Capability, Quote, Escrow, Receipt, Job, Account
internal/money/       fixed-point minor-unit arithmetic (no floats for money)
internal/store/       persistence interface + memory and postgres implementations
internal/adapters/
  tosai/              ATOS -> tos-ai interface (execution) + mock impl
  toscore/            ATOS/tos-ai -> tos-core interface (trust/economy) + mock impl
internal/service/     business logic: capability/quote/job/account/receipt
internal/httpapi/     REST handlers (docs/API.md)
internal/mcp/         MCP JSON-RPC handlers (docs/MCP.md)
internal/a2a/         A2A JSON-RPC handlers (docs/A2A.md)
```

This mirrors the plane separation in `atos-spec/docs/ARCHITECTURE.md`:
`internal/service` never imports a TOS Network client directly, only
`internal/adapters/tosai` and `internal/adapters/toscore`.

## Run it

```bash
go run ./cmd/api
```

Starts on `:8080` with one seeded sandbox capability (`Echo Sandbox`,
`$1.00` fixed price), using the in-memory store by default.

To use Postgres instead:

```bash
createdb atos
export ATOS_DATABASE_URL="postgres://$(whoami)@localhost:5432/atos?sslmode=disable"
go run ./cmd/migrate
go run ./cmd/api
```

```bash
# Device auth (always succeeds in this Phase 0 stub)
curl -s -X POST localhost:8080/v1/auth/device/token -d '{"device_code":"dc_demo"}'
# -> {"access_token": "prn_dc_demo", ...} — use "prn_dc_demo" as the Bearer token below

TOKEN=prn_dc_demo

curl -s "localhost:8080/v1/capabilities?q=echo" -H "Authorization: Bearer $TOKEN"
curl -s -X POST localhost:8080/v1/quotes -H "Authorization: Bearer $TOKEN" \
  -d '{"capability_id":"<id from search>"}'
curl -s -X POST localhost:8080/v1/invocations -H "Authorization: Bearer $TOKEN" \
  -d '{"capability_id":"<id>","quote_id":"<id>","input":{"hello":"world"},"idempotency_key":"demo-1"}'
```

MCP is exposed at `POST /mcp`, A2A at `POST /a2a` (both JSON-RPC 2.0).

## Tests

```bash
go test -race ./...
```

Postgres integration tests are skipped unless `ATOS_TEST_DATABASE_URL` is
set:

```bash
createdb atos_test
export ATOS_TEST_DATABASE_URL="postgres://$(whoami)@localhost:5432/atos_test?sslmode=disable"
go run ./cmd/migrate  # against the same URL, or set ATOS_DATABASE_URL to it
go test -race ./internal/store/postgres/...
```

## Next steps (see atos-spec/docs/IMPLEMENTATION_ROADMAP.md)

- Phase 1: real semantic search/ranking, real Device Auth verification step.
- Phase 2: replace `internal/adapters/tosai/mock` with a real tos-ai
  provider/worker runtime; add the provider job-queue endpoints once a
  real queued-provider model exists.
- Phase 4: replace `internal/adapters/toscore/mock` with real tos-core
  calls (identity, escrow, receipt verification, settlement).
