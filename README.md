# ATOS

Implementation of the ATOS gateway described in `~/atos-spec` and
`~/TOS_ARCHITECTURE_V2.md`. This is the **Phase 0 — Contract First**
skeleton from `atos-spec/docs/IMPLEMENTATION_ROADMAP.md`: the public
REST + MCP contracts are real and wired end to end, but persistence is
in-memory and tos-ai/tos-core are mocked, in-process implementations —
not a real provider network or a real trust/settlement chain yet.

## What's real vs. mocked

| Layer | Status |
|---|---|
| REST API (`docs/API.md`) | Implemented — capabilities, quotes, invocations, jobs, account, device auth, agent card |
| MCP (`docs/MCP.md`) | Implemented — all 10 default tools over JSON-RPC 2.0 `tools/list`/`tools/call`. Session resumability and true MRTR elicitation round trips are not yet implemented; `input_required` is returned as a normal result. |
| Escrow / settlement state machine (`docs/SETTLEMENT.md`) | Implemented — reserve → verify → settle/release, enforced in code (`internal/adapters/toscore/mock`), not just documented |
| tos-ai execution | Mocked (`internal/adapters/tosai/mock`) — echoes input back as output; swap for a real provider network in Phase 2 |
| tos-core trust/settlement | Mocked (`internal/adapters/toscore/mock`) — real state machine, no chain commitment; swap in Phase 4 |
| Persistence | In-memory (`internal/store/memory`) — a Postgres implementation drops in behind the same `store.Store` interface in Phase 1 |
| Device Auth | Stubbed — always succeeds immediately, no real human verification step |
| Semantic search / ranking | Naive substring match — real ranking is a Phase 1+ concern |

None of the above requires changing `internal/service` or the public
REST/MCP contracts to fix later — that boundary is the point of this
skeleton.

## Layout

```text
cmd/api/            entrypoint: wires store, adapters, services, HTTP mux
internal/domain/     Capability, Quote, Escrow, Receipt, Job, Account
internal/money/      fixed-point minor-unit arithmetic (no floats for money)
internal/store/      persistence interfaces + in-memory implementation
internal/adapters/
  tosai/             ATOS -> tos-ai interface (execution) + mock impl
  toscore/           ATOS/tos-ai -> tos-core interface (trust/economy) + mock impl
internal/service/    business logic: capability/quote/job/account
internal/httpapi/    REST handlers (docs/API.md)
internal/mcp/        MCP JSON-RPC handlers (docs/MCP.md)
```

This mirrors the plane separation in `atos-spec/docs/ARCHITECTURE.md`:
`internal/service` never imports a TOS Network client directly, only
`internal/adapters/tosai` and `internal/adapters/toscore`.

## Run it

```bash
go run ./cmd/api
```

Starts on `:8080` with one seeded sandbox capability (`Echo Sandbox`,
`$1.00` fixed price) so the full contract is exercisable immediately.

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

MCP is exposed at `POST /mcp` (JSON-RPC 2.0, `tools/list` / `tools/call`).

## Next steps (see atos-spec/docs/IMPLEMENTATION_ROADMAP.md)

- Phase 1: Postgres-backed `store.Store` implementation, real semantic
  search, real Device Auth verification step.
- Phase 2: replace `internal/adapters/tosai/mock` with a real tos-ai
  provider/worker runtime.
- Phase 4: replace `internal/adapters/toscore/mock` with real tos-core
  calls (identity, escrow, receipt verification, settlement).
