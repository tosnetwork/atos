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
- formatting, OpenAPI parsing, vet and race-test CI.

## Intentionally unavailable until cross-repository integration

The gateway does not claim Verified or Native availability merely because their
types exist. Activation requires the real implementations described by the
ATOS/TOS protobuf contract:

1. `tos-protocol` Edge/Core `ExecutionGatewayService`;
2. private Edge Core -> `tos-ai` Worker execution;
3. TOS-backed identity and Capability ownership resolution;
4. enforceable TOS escrow/release/settlement;
5. execution-signer authorization and receipt proof commitment;
6. portable Proof-of-Service evidence;
7. Native global resolution and independent index reconstruction.

Until those guarantees exist, explicit `verified` or `native` requests fail
instead of falling back to Managed.
