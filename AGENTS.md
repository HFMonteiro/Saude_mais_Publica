# Project Design Rules

Act as a pragmatic senior technical partner. Keep code changes focused, maintainable, and validated.

## Product Voice
- Use European Portuguese for public-sector, health, policy, methodology, and interface copy.
- Avoid AI-sounding placeholders, long explanatory blocks, and claims that exceed the data.
- Prefer short labels, concrete verbs, and explicit caveats: fonte, denominador, granularidade, cobertura, período.

## Interface
- Preserve the existing vanilla HTML/CSS/JS architecture and visual identity.
- Keep pages dense but readable: compact cards, small labels, strong wrapping, no horizontal overflow.
- Use `styles.css` variables for spacing and typography before adding new local values.
- When showing analytic suitability, use the shared fit language: `Pronto`, `Rever`, `Frágil`.
- Empty states must offer a useful next action or nearest opportunity, not blank panels.

## Analytics
- Treat healthcare and administrative data as sensitive.
- Never present heuristic scores, correlations, PCA, or projections as official or causal results.
- Before interpreting ratios or cost/unit signals, require denominator, unit, geography, entity, and period validation.
- Flag missing values, truncated samples, incompatible granularities, and weak temporal coverage.

## Validation
- For frontend changes, smoke test `index.html`, `crosswalk.html`, `analytics.html`, and `metodologia.html` in the in-app browser when feasible.
- Run `python scripts\qa.py` after backend, API, or analytics logic changes.
- Do not claim browser, API, or test validation unless it actually ran.
