# Security Best Practices Report

## Executive summary
The current codebase is materially safer than the older report suggested: static serving is allowlisted, CORS is limited to localhost origins, dataset IDs and numeric limits are validated, D3 is pinned with SRI, API errors are generic, and the server binds to `127.0.0.1`. The remaining security work is mostly deployment hardening and CSP/style tightening. This pass removed the last frontend HTML sink and added an automated QA guard against common dangerous frontend sinks.

## High priority findings
No high or critical exploitable issue was confirmed in the current local-only code path.

## Medium priority findings

### SBP-201 - Expensive analytics endpoint throttling added
- **Status:** Fixed in this pass.
- **Location:** `server.py`
- **Evidence:** Expensive routes now have a separate per-route rate limit and a bounded semaphore for concurrent analysis requests.
- **Impact reduced:** Local/public accidental exposure has less ability to saturate the process or generate upstream load.
- **Residual risk:** This is still an in-process local server; public deployment still needs reverse proxy/edge controls.

### SBP-202 - Local development server is not a production deployment profile
- **Severity:** Medium if publicly exposed; Low if local-only.
- **Location:** `server.py:24`, `server.py:3701`
- **Evidence:** The runtime uses `ThreadingHTTPServer(("127.0.0.1", 8000), TransparenciaHandler)`.
- **Impact:** `ThreadingHTTPServer` has limited production hardening, no worker pool controls, no TLS, and in-memory rate/cache state.
- **Fix:** For public deployment, place behind a reverse proxy/managed platform with TLS, request size/time limits, access logs, edge rate limiting and health checks.
- **Mitigation:** Keep loopback bind as the safe default and fail closed if an environment variable attempts non-loopback without explicit production config.
- **False positive notes:** This is acceptable for local tooling.

### SBP-203 - CSP still permits inline styles
- **Severity:** Medium as defense-in-depth, not an active exploit.
- **Location:** `server.py:3357`
- **Evidence:** CSP includes `style-src 'self' 'unsafe-inline'`.
- **Impact:** If future DOM injection occurs, inline style injection remains easier. Script execution is still constrained by `script-src 'self' https://cdn.jsdelivr.net`.
- **Fix:** Remove inline `style` attributes from HTML/JS and switch dynamic values to CSS classes or CSS custom properties set through a constrained helper; then change CSP to `style-src 'self'`.
- **Mitigation:** Keep `object-src 'none'`, `base-uri 'self'`, `frame-ancestors 'none'`, and avoid `unsafe-eval`/inline scripts.
- **False positive notes:** Current code uses dynamic/inline style patterns for charts and progress bars, so removing this immediately may break UI.

### SBP-204 - Frontend dangerous-sink guard added
- **Status:** Fixed in this pass.
- **Location:** `analytics.js`, `scripts/qa.py`
- **Evidence:** The remaining constant `innerHTML` card was replaced with DOM node creation, and QA now scans app JS for common dangerous sinks.
- **Impact reduced:** New `innerHTML`, `outerHTML`, `insertAdjacentHTML`, `eval`, `new Function`, `document.write`, browser storage and `postMessage` usage should fail the standard QA gate.

## Low priority findings

### SBP-301 - CDN dependency remains in the script policy
- **Severity:** Low.
- **Location:** `index.html:9`, `server.py:3357`
- **Evidence:** D3 is loaded from jsDelivr with SHA-384 SRI; CSP allows `https://cdn.jsdelivr.net`.
- **Impact:** SRI protects against content tampering for the exact asset, but the app still depends on a third-party CDN being reachable.
- **Fix:** Vendor `d3.min.js` locally and remove jsDelivr from `script-src` if this becomes a public/reliable deployment.
- **Mitigation:** Keep the pinned version and SRI while CDN remains.

### SBP-302 - CORS allowlist is safe for public data but too broad for future sensitive data
- **Severity:** Low now; Medium if private/internal data is added.
- **Location:** `server.py:51`, `server.py:486`, `server.py:3361`
- **Evidence:** Allowed origins include localhost/127.0.0.1 variants.
- **Impact:** Other local web apps matching those origins could read API responses if the service is running. Current responses are public/open-data oriented.
- **Fix:** If private data is ever introduced, require a per-run random token or disable CORS entirely unless the exact UI origin needs it.
- **Mitigation:** Keep no credentials/cookies and no sensitive datasets in this local explorer.

### SBP-303 - Reflected path in JSON 404 is harmless locally but noisy if public
- **Severity:** Low.
- **Location:** `server.py:3692`
- **Evidence:** Unknown static paths return `Static asset not found: {path}`.
- **Impact:** Reflected text is JSON-encoded and not HTML, so XSS is unlikely; public deployments should still avoid echoing scanner-controlled paths.
- **Fix:** Return a generic `Static asset not found` message if exposed publicly.

## Positive controls observed
- Static allowlist: `ALLOWED_STATIC_PATHS` blocks arbitrary file serving and directory listing is disabled.
- Dataset ID validation: `_parse_dataset_id` uses a strict regex and length cap.
- Numeric limits: `_parse_int_param` applies minimum/maximum bounds to request parameters.
- Security headers: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, CSP.
- CORS is not wildcard; it is restricted by `_normalize_origin`.
- Upstream base URL is fixed; query parameters use `urlencode`; dataset IDs are quoted into ODS paths.
- API error bodies sent to clients are generic for upstream failures.
- D3 dependency is version-pinned and protected with SRI.
- No `eval`, `new Function`, `document.write`, `postMessage`, `localStorage`, or `sessionStorage` usage was found in the reviewed app files.

## Recommended next fixes
1. Continue tightening CSP by removing inline style dependencies.
2. Add route-specific tests for expensive endpoint throttling.
3. Start removing inline style attributes/patterns, then tighten CSP to `style-src 'self'`.
4. If public deployment is planned, vendor D3 locally and remove CDN from CSP.

## Verification performed for this report
- Read `security-threat-model` and `security-best-practices` skill instructions.
- Read frontend JS security reference for vanilla browser JavaScript.
- Inspected `server.py`, HTML entrypoints, JS files, existing reports and project docs using PowerShell searches.
- Ran `python scripts\qa.py` after the security hardening changes.
