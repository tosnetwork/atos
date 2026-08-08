# ATOS v0.2 Implementation Status

This document tracks the implementation boundary of the Go gateway against
`tosnetwork/atos-spec` v0.2.

## Implemented in this branch

- one REST + MCP + A2A business model;
- `requested_trust_mode` separated from concrete `trust_mode`;
- standard `tos_verified_v1` and `tos_native_v1` profile types;
- provider-requested modes separated from active/quotable modes;
- immutable Quote mode/profile propagation through Job, Escrow and Receipt;
- Managed Quote -> reserve -> execute -> verify -> settle lifecycle;
- authorized execution-signer receipt fields and content commitments;
- fail-closed Verified/Native selection while TOS-backed infrastructure is unavailable;
- scoped Device Authorization, access-token refresh and revocation;
- authorization-derived deterministic MCP `tools/list` with private caching;
- 9-tool ordinary consumer surface and scoped provider management tools;
- operation-discriminated `atos_artifact` with signed HTTP PUT/GET URLs;
- artifact ownership, declared-size and expiry enforcement;
- PostgreSQL v0.2 payload persistence through embedded migrations;
- v0.2 Agent Card, provider cards, REST OpenAPI and A2A commerce extension;
- a real `atos -> tos-protocol` ConnectRPC client covering all six ATOS/TOS v0.2 services;
- an explicit `ATOS_TOS_BACKEND=mock|rpc` switch with no failure fallback;
- TLS/custom CA/mTLS, bearer authentication, bounded messages, deadlines and readiness checks;
- two-layer Service Execution Quote -> ATOS Commercial Quote binding;
- real Managed execution through `tos-protocol` -> private Unix-socket `tos-ai` Worker RPC;
- cross-service integration coverage for Quote -> Escrow -> Worker -> Receipt -> Verify -> Settle and idempotent replay;
- formatting, OpenAPI parsing, vet and race-test CI.

## Current runtime boundary

The cross-repository Managed path now exists:

```text
ATOS -> ConnectRPC -> tos-protocol ExecutionGatewayService
     -> private Unix-socket tos-ai Worker
     -> signed Execution Receipt -> verification -> settlement
```

The current `tos-protocol` binary still supplies `NewLocalAuthority("tos-local")`,
which supports Managed mode only. Therefore the gateway does not claim Verified
or Native availability merely because their RPC types and verification logic
exist. Remaining activation requirements are:

1. a configurable TOS Network-backed Authority rather than the local Authority;
2. TOS-backed identity and Capability ownership/finality;
3. enforceable network escrow/release/settlement and portable proof references;
4. Native global resolution, independent index reconstruction and federation tests.

Until those guarantees exist, explicit `verified` or `native` requests fail
instead of falling back to Managed.
