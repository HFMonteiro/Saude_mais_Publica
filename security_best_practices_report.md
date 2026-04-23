# Security Best Practices Report

## Executive summary
This project is a functional local data-explorer for public SNS transparency datasets, with a clean UI flow and useful cross-link analysis. The current implementation is usable, but it mixes trusted and untrusted data in DOM sinks and exposes backend/API surface broadly. Main high-priority work is to remove unsafe HTML sinks, add conservative CORS/header policy, and add request hardening (validation + quotas + endpoint boundaries).

## High severity

### SBP-101 — DOM XSS via unsanitized `innerHTML` in main UI
- **Location(s):** `app.js:227`, `app.js:275`, `app.js:390`, `app.js:493`, `app.js:549`, `app.js:561`
- **Impact:** If field names, titles or shared-field labels contain markup, injected HTML/JS can execute in user browsers.
- **Why it matters:** Cross-page data comes from an external API and is used directly in rendering.
- **Fix:** Replace these sinks with `textContent` nodes or explicit `createElement` construction. If markup is needed, add allowlist sanitization before insertion and enforce CSP (`script-src` without unsafe-inline/unsafe-eval).
- **Suggested code path:** Build rows/cells with `textContent`; keep `innerHTML` only for trusted constant templates.

### SBP-102 — DOM XSS in crosswalk table/detail rendering
- **Location(s):** `crosswalk.js:474`, `crosswalk.js:477`, `crosswalk.js:500-503`, `crosswalk.js:561`, `crosswalk.js:661`
- **Impact:** Same DOM execution class in crosswalk page; can compromise analyst browser and any local API state.
- **Why it matters:** Same dataset payloads are reused and rendered with `cell.innerHTML` and list builders.
- **Fix:** Centralize safe rendering helpers (`setText`, `renderCellText`) and remove HTML string concatenation for untrusted values.

### SBP-103 — Public wildcard CORS on API responses
- **Location(s):** `server.py:482-483`
- **Impact:** Any web origin can read responses from this local API if exposed.
- **Why it matters:** Even with public data, this increases cross-site data scraping and policy bypass opportunities.
- **Fix:** Restrict `Access-Control-Allow-Origin` to explicit trusted origins (or remove CORS entirely when not needed).

## Medium severity

### SBP-201 — Missing input bounds for `/api/records` `limit`
- **Location(s):** `server.py:562-573`
- **Impact:** Large/unbounded limits can force large payloads and increase latency/CPU.
- **Fix:** Parse and clamp `limit` with explicit min/max (same pattern as `_recent_records`).

### SBP-202 — Weak `dataset_id` path handling
- **Location(s):** `server.py:551-552`, `server.py:556`, `server.py:562-568`, `server.py:579-584`
- **Impact:** `quote()` keeps `/` by default, so malformed IDs can alter path composition.
- **Fix:** validate `dataset_id` with strict regex, and use `quote(dataset_id, safe="")` when interpolating into path segments.

### SBP-203 — Open static file surface via `SimpleHTTPRequestHandler`
- **Location(s):** `server.py:480`
- **Impact:** Any file in the working directory may be exposed by static route logic.
- **Fix:** serve only from explicit static folder (e.g., `public/`), block dotfiles and config paths.

### SBP-204 — No security headers on responses
- **Location(s):** `server.py:480-500`, `server.py:482-484`
- **Impact:** Weaker defense-in-depth against DOM/script abuse and MIME confusion.
- **Fix:** add `X-Content-Type-Options: nosniff`, CSP, `Referrer-Policy`, `Permissions-Policy`, and frame-control suitable for embedding intent.

### SBP-205 — Third-party script without SRI
- **Location(s):** `index.html:8`
- **Impact:** External dependency can be replaced or altered by CDN without integrity check.
- **Fix:** pin exact version (already mostly pinned) and add `integrity` + `crossorigin="anonymous"`, or self-host D3.

### SBP-206 — DoS resilience
- **Location(s):** `server.py:503-589`, `server.py:284-295`
- **Impact:** No rate limiting and no per-client quotas across endpoints.
- **Fix:** add basic throttling middleware or front-proxy rate limiting; set request concurrency caps and timeouts for long operations.

## Low severity / hardening

### SBP-301 — Public deployment assumptions not enforced
- **Location(s):** `server.py:597-600`
- **Impact:** Running `ThreadingHTTPServer` without environment-specific config encourages dev-style deployment.
- **Fix:** keep local-only launch path documented; production should run behind reverse proxy + worker process.

### SBP-302 — Missing explicit error-budget and observability
- **Location(s):** `server.py:492-501`
- **Impact:** Error handling is functional but no structured logs, no correlation IDs, no alerting.
- **Fix:** add minimal request logging and structured errors for operational monitoring without logging sensitive payloads.

### SBP-303 — Public data size and memory pressure in analysis
- **Location(s):** `server.py:309-329`, `server.py:331-440`, `app.js:302-333`
- **Impact:** O(n²)-like join logic can grow quadratically as corpus grows.
- **Fix:** implement top-k pruning before full pairwise expansion, and background precompute for cached analysis snapshots.

## Security posture checklist
- [ ] Add CSP and remove remaining `innerHTML` sinks that can carry attacker-controlled strings.
- [ ] Add strict CORS allowlist and remove wildcard.
- [ ] Add path and numeric input validation for all API routes.
- [ ] Add rate limiting + basic abuse telemetry.
- [ ] Restrict static serving root and hidden file exposure.
- [ ] Add `integrity` to third-party scripts or self-host dependencies.
- [ ] Define local vs public deployment profiles.

## Priority execution plan
1. **Patch DOM sinks first** (`app.js`, `crosswalk.js`) — highest user-impact security risk.
2. **Harden API boundaries** (`server.py` CORS/input validation/rate limiting).
3. **Add response hardening headers + static serving restrictions**.
4. **Operational controls** (`SRI`, logs, deployment profile).

## Evidence notes
- All findings below include line anchors to current files.
- `python -m py_compile server.py` and `node --check app.js crosswalk.js` pass syntax checks.

