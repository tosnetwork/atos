# ATOS v0.2 Phase 2 / Phase 3 Roadmap Coverage

This document is maintained on the implementation branch while the corresponding roadmap items are validated. The pull request description is the authoritative review summary.

## Phase 2

- resumable `StreamJob` delivery;
- provider earnings and maturation;
- user-visible Managed disputes;
- signed-usage metered billing;
- crash-safe, idempotent recovery under PostgreSQL.

## Phase 3

- HTTP, MCP, and A2A provider adapters;
- provider health and per-mode availability;
- schema validation and sandbox certification;
- mode-support lifecycle management;
- execution-signer rotation and revocation;
- open task publish/apply/accept marketplace lifecycle.

All concrete trust modes remain frozen by Quote, and all Account-changing transitions continue to use the existing atomic Job/Account boundary.
