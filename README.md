<img src="ATOS.png" alt="ATOS" width="220">

Go implementation of the ATOS Agent Internet gateway.

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
happy path in two explicit deployments:

```text
ATOS_TOS_BACKEND=mock  local development/test adapter
ATOS_TOS_BACKEND=rpc   real ConnectRPC path through tos-protocol and its private tos-ai Worker RPC
```

The RPC path binds an Edge/provider Service Execution Quote into the client-facing
ATOS Commercial Quote, then carries the same principal, capability version,
trust mode, proof profile, deadline and commitments through execution, receipt
verification and settlement. RPC configuration or readiness failure stops
startup; it never falls back to the mock backend.

Verified and Native protocol logic remains fail-closed because the current
`tos-protocol` runtime still uses its Managed-only `tos-local` Authority rather
than a final TOS Network trust anchor.

| Layer | Status |
|---|---|
| REST API | Implemented with scoped Bearer authorization |
| MCP | Stateless Streamable HTTP JSON-RPC; authorization-derived deterministic `tools/list` |
| A2A | `message/send`, `tasks/get`, `tasks/cancel`, sharing the canonical Job pipeline |
| Managed execution | End-to-end Quote → escrow → execution → signed receipt → verify → settle |
| Artifact transport | Signed HTTP PUT/GET URLs; bytes never travel through MCP/A2A business calls |
| PostgreSQL | Indexed relational projection + complete v0.2 JSONB payload persistence |
| `tos-ai` | Explicit mock for local development, or real private Unix-socket Worker execution behind `tos-protocol` in RPC mode |
| `tos-core` / `tos-protocol` | Real typed ConnectRPC clients for Identity, Capability, Trust, Settlement, Proof and ExecutionGateway services; current Authority is Managed-only `tos-local` |
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
cmd/api/                       gateway entrypoint
cmd/migrate/                   PostgreSQL migration command
api/openapi.yaml               REST contract
migrations/                    ordered SQL migrations
internal/auth/                 scoped device/access/refresh tokens
internal/domain/               v0.2 business objects and trust-mode types
internal/service/              capability, quote, execution and settlement logic
internal/httpapi/              REST handlers
internal/mcp/                  MCP catalog, resources and tool handlers
internal/a2a/                  A2A Task/Message mapping
internal/adapters/tosai/       execution boundary + Phase 0 mock
internal/adapters/toscore/     trust/economy/proof boundary + Phase 0 mock
internal/adapters/tosprotocol/ real ConnectRPC execution/trust/economy/proof client
internal/adapters/storage/     signed-URL Artifact storage
internal/store/                memory and PostgreSQL implementations
```

## Run locally

The module uses Go 1.25.

```bash
go run ./cmd/api
```

The gateway listens on `:8080` and seeds one Managed `Echo Sandbox`
Capability. The in-memory store is used unless `ATOS_DATABASE_URL` is set.
When `ATOS_TOS_BACKEND` is omitted, the development default is `mock`.

### Real tos-protocol backend

Run `tos-protocol` with its ATOS RPC server and private `tos-ai` Worker, then
start ATOS with an explicit RPC backend:

```bash
export ATOS_TOS_BACKEND=rpc
export ATOS_TOS_RPC_URL=http://127.0.0.1:8090
export ATOS_TOS_RPC_TOKEN='<shared-internal-token>'
export ATOS_TOS_RPC_INSECURE=true   # local plaintext only

go run ./cmd/api
```

For non-loopback deployments use HTTPS. Optional trust and mTLS settings are:

```text
ATOS_TOS_RPC_SERVER_NAME
ATOS_TOS_RPC_CA_FILE
ATOS_TOS_RPC_CLIENT_CERT_FILE
ATOS_TOS_RPC_CLIENT_KEY_FILE
ATOS_TOS_RPC_TIMEOUT
ATOS_TOS_RPC_MAX_MESSAGE_BYTES
```

Client certificate and key must be configured together. Plaintext HTTP is
rejected unless `ATOS_TOS_RPC_INSECURE=true`. A configured RPC backend must pass
its startup readiness probe; connection failure never selects the mock backend.

The default inline execution output bound is 1 MiB, below the private Worker's
2 MiB default message policy. Larger outputs should be returned through the
Artifact flow rather than embedded in an RPC/MCP/A2A response.

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

GitHub Actions enforces formatting, vet and race tests. The RPC integration
test starts a real `tos-protocol` ConnectRPC server and a private Unix-socket
Worker, then verifies Quote → Escrow → execution → Receipt → verification →
settlement and idempotent replay through the ATOS service layer.

## Security invariants already enforced

- `auto` is never a committed transaction mode.
- Quote mode/profile are immutable and propagate to Job, Escrow and Receipt.
- Explicit Verified/Native requests fail closed while their infrastructure is unavailable.
- Provider-requested modes are not public active modes until certification.
- A configured RPC backend never falls back to mock after configuration or readiness failure.
- Tool visibility does not substitute for call-time authorization.
- Artifact IDs and upload IDs are not bearer credentials.
- Uploaded byte count must match the declared size.
- Bulk/private payloads stay off-chain; durable proof uses commitments.
- Job completion and cancellation cannot overwrite each other inside one gateway process.

## Next implementation boundaries

1. Replace `tos-protocol`'s Managed-only `tos-local` Authority with a configurable chain-backed Authority that reuses the repository's existing TOS chain/finality primitives.
2. Add durable ATOS ↔ tos-protocol reconciliation for cross-process crash windows and repair the PostgreSQL `in_progress` idempotency lease gap identified by the security review.
3. Replace the Phase 0 authorization approval shortcut with production OAuth/device consent and persistent token storage.
4. Activate `tos_verified_v1`, then `tos_native_v1`, only after their complete network guarantees and conformance tests pass.
5. Add provider queueing, webhook callbacks, disputes, canonical commitment encoding and federated indexers in their roadmap phases.
