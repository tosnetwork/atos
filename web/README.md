# ATOS task marketplace

The standalone Web surface for `/tasks`, `/tasks/new`, and `/tasks/:task_id`.
It consumes the frozen `/v1/open-tasks` REST contract and does not duplicate
server-side ownership or redaction logic.

## Local development

```sh
npm install
npm run dev
```

Vite proxies `/v1` and `/activate` to `http://localhost:8080`. Set
`VITE_API_ROOT` at build time only when the API is not exposed under the same
origin. Production should route SPA requests under `/tasks` back to
`index.html` and proxy `/v1` to the ATOS API.

## Authentication

The marketplace is a Device Authorization client, not a replacement consent
page. It requests requester scopes (`open_tasks:read`, `open_tasks:write`,
`jobs:read`, `quotes:read`) plus the explicit Provider scopes
`open_task_proposals:write` and `capabilities:write`, opens the server-issued
`verification_uri_complete`, and polls the token endpoint at the server-provided
interval. The existing trusted login/reverse-proxy boundary on `/activate`
authenticates the human and injects the approval headers.

Tokens are kept in `sessionStorage`, so they are scoped to the current browser
tab and are not made into a long-lived Web credential. Expired access tokens
are refreshed using the frozen refresh endpoint. Sign out revokes the current
token and clears the session.
