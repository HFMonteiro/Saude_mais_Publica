# Security Best Practices Report

## Executive summary
The app is now a local-first explorer with a narrower public surface. The processing/cache page and `/api/processing` diagnostic endpoint were removed because they exposed runtime internals without adding user value. CSP no longer allows inline styles, upstream ODS error bodies are not returned to clients, static assets are served through an allowlist, and the server version header no longer advertises Python.

## Fixed in this pass

### SBP-101 - Processing diagnostics removed
- **Status:** Fixed.
- **Location:** `server.py`, `index.html`, `crosswalk.html`, `analytics.html`
- **Evidence:** `/api/processing` route was removed, `processing.html` and `processing.js` were deleted, and navigation links to processing were removed.
- **Impact reduced:** Runtime cache keys, process id, cache ages and byte sizes are no longer exposed through the web UI/API.
- **Verification:** `GET /processing.html` and `GET /api/processing` return `404`.

### SBP-102 - CSP tightened
- **Status:** Fixed.
- **Location:** `server.py`
- **Evidence:** CSP is now `style-src 'self'` instead of `style-src 'self' 'unsafe-inline'`.
- **Impact reduced:** The browser no longer allows inline style blocks/attributes by policy. The UI still uses CSS files and constrained CSSOM updates for dynamic visualization values.

### SBP-103 - Upstream error details no longer returned
- **Status:** Fixed.
- **Location:** `server.py`
- **Evidence:** Non-validation exceptions now return `{"error":"Upstream data service unavailable"}` instead of `str(exc)`.
- **Impact reduced:** Upstream response bodies, request details and transport exception text are not sent to clients.

### SBP-104 - Server fingerprinting reduced
- **Status:** Fixed.
- **Location:** `server.py`
- **Evidence:** `TransparenciaHandler.server_version` and `sys_version` override the default `SimpleHTTP/Python` banner.
- **Impact reduced:** HTTP responses expose less implementation detail.

## Previously fixed controls still in place
- Static files are served only from `ALLOWED_STATIC_PATHS`; dotfiles and markdown reports are not exposed.
- Cache is bounded by entry count and bytes.
- Raw record responses are not stored in backend cache.
- Analytics variants are computed from the cached base analysis and not cached per slider value.
- Cold analysis generation uses a lock to avoid duplicate concurrent builds.
- Local rate limiting is active.
- D3 is pinned to `7.9.0` with SHA-384 SRI.
- Frontend rendering avoids dangerous `innerHTML` sinks for API-derived data.

## Remaining findings

### SBP-201 - Production deployment profile is still intentionally local
- **Severity:** Low.
- **Evidence:** The app binds to `127.0.0.1:8000` and uses Python `ThreadingHTTPServer`.
- **Impact:** This is correct for local use. It should not be treated as a production web server if exposed publicly.
- **Fix:** For production, run behind a reverse proxy or managed platform, keep the allowlist, and move TLS/rate limiting/logging to the edge.

### SBP-202 - CDN dependency remains allowed by CSP
- **Severity:** Low.
- **Evidence:** `index.html` loads D3 from jsDelivr with SRI.
- **Impact:** SRI protects against content drift/tampering, but a stricter public deployment can remove the CDN dependency entirely.
- **Fix:** Vendor `d3.min.js` locally and remove `https://cdn.jsdelivr.net` from `script-src`.

## Verification performed
- `python -m py_compile server.py`
- `node --check app.js`
- `node --check crosswalk.js`
- `node --check analytics.js`
- `git diff --check`
- HTTP validation: `/processing.html` returns `404`
- HTTP validation: `/api/processing` returns `404`
- Header validation: CSP no longer contains `unsafe-inline`
