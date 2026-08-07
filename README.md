# ATOS

Go implementation of the ATOS Agent Internet gateway defined in
[`tosnetwork/atos-spec`](https://github.com/tosnetwork/atos-spec).

ATOS exposes one REST + MCP + A2A business protocol for discovering, quoting,
invoking and settling third-party **Capabilities** under three concrete trust
modes:

```text
managed   centralized atos.im execution/accounting
verified  managed UX with TOS-verifiable trust/economic checkpoints
native    TOS-backed trust plus gateway-independent canonical resolution
```

Clients may ask for `requested_trust_mode=auto`, but every Quote freezes one
concrete `trust_mode`. Jobs, escrows, execution receipts and settlement inherit
that mode; stronger-mode failure never silently falls back to Managed.

## Current delivery status

This branch implements the Phase 0/1 contract and a complete **Managed Mode**
happy path. Verified and Native types, RPC boundaries, schemas and fail-closed
selection rules are implemented, but their modes remain unavailable until the
real `tos-protocol`/TOS adapters replace the mocks.

| Layer | Status |
|---|---|
| REST API | Implemented with scoped Bearer authorization |
| MCP | Stateless Streamable HTTP JSON-RPC; authorization-derived deterministic `tools/list` |
| A2A | `message/send`, `tasks/get`, `tasks/cancel`, sharing the canonical Job pipeline |
| Managed execution | End-to-end Quote → escrow → execution → signed receipt → verify → settle |
| Artifact transport | Signed HTTP PUT/GET URLs; bytes never travel through MCP/A2A business calls |
| PostgreSQL | Indexed relational projection + complete v0.2 JSONB payload persistence |
| `tos-ai` | Phase 0 in-process Managed mock; final topology is ATOS → `tos-protocol` Edge Core → private `tos-ai` Worker RPC |
| `tos-core` / `tos-protocol` | Fail-closed mock state machine; no fabricated TOS proof |
| Device Authorization | Scoped in-memory Phase 0 implementation with immediate approval; production UI/consent remains to be wired |
| Verified / Native | Contract implemented, runtime availability intentionally `unavailable` until real TOS integration |

## MCP surface

A token carrying the recommended ordinary consumer scopes sees **9 tools** in
stable order:

```text
atos_search
atos_get_capability
atos_quote
atos_invoke
atos_create_job
atos_get_job
atos_cancel_job
atos_account
atos_artifact
```

`atos_artifact` uses one operation-dispatched tool for:

```text
create_upload | complete_upload | get_download_url
```

Capability-management tools appear only with `capabilities:write`; every
`tools/call` still re-checks scope and object ownership. `tools/list` is derived
from authorization on the current request, never session history, and returns:

```json
{
  "ttlMs": 30000,
  "cacheScope": "private"
}
```

## Repository layout

```text
cmd/api/                    gateway entrypoint
cmd/migrate/                PostgreSQL migration command
api/openapi.yaml            REST contract
migrations/                 ordered SQL migrations
internal/auth/              scoped device/access/refresh tokens
internal/domain/            v0.2 business objects and trust-mode types
internal/service/           capability, quote, execution and settlement logic
internal/httpapi/           REST handlers
internal/mcp/               MCP catalog, resources and tool handlers
internal/a2a/               A2A Task/Message mapping
internal/adapters/tosai/    execution boundary + Phase 0 mock
internal/adapters/toscore/  trust/economy/proof boundary + Phase 0 mock
internal/adapters/storage/  signed-URL Artifact storage
internal/store/             memory and PostgreSQL implementations
```

## Run locally

The module uses Go 1.25.

```bash
go run ./cmd/api
```

The gateway listens on `:8080` and seeds one Managed `Echo Sandbox`
Capability. The in-memory store is used unless `ATOS_DATABASE_URL` is set.

### Device Authorization

Start a scoped Phase 0 device flow:

```bash
DEVICE=$(curl -s -X POST http://localhost:8080/v1/auth/device \
  -H 'content-type: application/json' \
  -d '{
    "client_type":"codex",
    "client_name":"Codex",
    "requested_scopes":[
      "capabilities:read",
      "quotes:read",
      "invocations:create",
      "jobs:create",
      "jobs:read",
      "jobs:cancel",
      "account:read"
    ]
  }')

echo "$DEVICE"
```

Exchange its `device_code`:

```bash
TOKEN_RESPONSE=$(curl -s -X POST http://localhost:8080/v1/auth/device/token \
  -H 'content-type: application/json' \
  -d '{"device_code":"<device_code>"}')

echo "$TOKEN_RESPONSE"
export TOKEN='<access_token>'
```

Discover, quote and invoke:

```bash
curl -s 'http://localhost:8080/v1/capabilities?q=echo' \
  -H "Authorization: Bearer $TOKEN"

curl -s -X POST http://localhost:8080/v1/quotes \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "capability_id":"<capability_id>",
    "requested_trust_mode":"auto"
  }'

curl -s -X POST http://localhost:8080/v1/invocations \
  -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' \
  -d '{
    "capability_id":"<capability_id>",
    "quote_id":"<quote_id>",
    "input":{"hello":"world"},
    "idempotency_key":"demo-1"
  }'
```

MCP is exposed at `POST /mcp`; A2A is exposed at `POST /a2a`.

## PostgreSQL

```bash
createdb atos
export ATOS_DATABASE_URL="postgres://$(whoami)@localhost:5432/atos?sslmode=disable"
go run ./cmd/migrate
go run ./cmd/api
```

Migration `003_v02_payloads.sql` keeps query-critical relational columns while
persisting the complete protocol object in JSONB. This prevents new mode,
proof, signer, settlement and Artifact fields from disappearing after restart.

## Tests and CI

```bash
gofmt -w .
go vet ./...
go test -race ./...
```

PostgreSQL integration tests are opt-in:

```bash
createdb atos_test
export ATOS_TEST_DATABASE_URL="postgres://$(whoami)@localhost:5432/atos_test?sslmode=disable"
ATOS_DATABASE_URL="$ATOS_TEST_DATABASE_URL" go run ./cmd/migrate
go test -race ./internal/store/postgres/...
```

GitHub Actions enforces formatting, vet and race tests.

## Security invariants already enforced

- `auto` is never a committed transaction mode.
- Quote mode/profile are immutable and propagate to Job, Escrow and Receipt.
- Explicit Verified/Native requests fail closed while their infrastructure is unavailable.
- Provider-requested modes are not public active modes until certification.
- Tool visibility does not substitute for call-time authorization.
- Artifact IDs and upload IDs are not bearer credentials.
- Uploaded byte count must match the declared size.
- Bulk/private payloads stay off-chain; durable proof uses commitments.
- Job completion and cancellation cannot overwrite each other inside one gateway process.

## Next implementation boundaries

1. Replace the Phase 0 authorization approval shortcut with production OAuth/device consent and persistent token storage.
2. Implement `ExecutionGatewayService` in `tos-protocol` and replace direct in-process `tos-ai` mock execution.
3. Implement the v0.2 trust/settlement/proof protobuf services against TOS Network.
4. Activate `tos_verified_v1`, then `tos_native_v1`, only after their full guarantee checks pass.
5. Add provider queueing, webhook callbacks, disputes and federated indexers in their roadmap phases.
