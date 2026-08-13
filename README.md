<img src="ATOS.png" alt="ATOS" width="220">

# ATOS Native Gateway

This repository implements the stateless public gateway for `atos_native_v1`.
Finalized TOS Network state is the sole authority for Agent and Capability
state. The gateway authenticates transport access and forwards Native actions
and resolution requests; it does not keep a canonical business database or
define alternate trust modes.

The product and protocol specification lives in
[`tosnetwork/atos-spec`](https://github.com/tosnetwork/atos-spec).

## Runtime surface

- `atos.native.v1.NativeService/SubmitNativeAction` requires the
  `ATOS_NATIVE_RELAY_TOKEN`.
- `atos.native.v1.NativeService/ResolveNativeState` accepts the read or relay
  token.
- `GET /livez` reports process liveness.
- `GET /readyz` verifies that the authoritative `tos-protocol` boundary is
  reachable.

These tokens grant transport permissions only. They cannot create or change
canonical state without a valid signed Native action accepted by TOS.

## Run locally

The module uses Go 1.26.5. Start a configured Native-only `tos-protocol` RPC
server, then run:

```bash
export ATOS_NATIVE_READ_TOKEN='<read-token>'
export ATOS_NATIVE_RELAY_TOKEN='<distinct-relay-token>'
export ATOS_TOS_RPC_URL='http://127.0.0.1:8090'
export ATOS_TOS_RPC_TOKEN='<private-backend-token>'
export ATOS_TOS_RPC_INSECURE=true

GOWORK=off go run ./cmd/api
```

Plaintext backend RPC is rejected unless `ATOS_TOS_RPC_INSECURE=true`. Use
HTTPS outside local development. Optional TLS settings are:

```text
ATOS_TOS_RPC_SERVER_NAME
ATOS_TOS_RPC_CA_FILE
ATOS_TOS_RPC_CLIENT_CERT_FILE
ATOS_TOS_RPC_CLIENT_KEY_FILE
ATOS_TOS_RPC_TIMEOUT
ATOS_TOS_RPC_MAX_MESSAGE_BYTES
```

Client certificate and key must be configured together. Startup and readiness
fail closed when `tos-protocol` is unavailable; there is no mock or Managed
fallback.

## Repository layout

```text
cmd/api/                              Native gateway process
internal/adapters/tosprotocol/        private Native Connect client
internal/config/                      Native-only configuration
internal/nativegateway/               transport auth and pass-through service
```

Run the test suite with:

```bash
GOWORK=off go test ./...
```
