import json
import sys
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


BASE_URL = "http://127.0.0.1:8000"


def get_json(path: str) -> dict:
    try:
        with urlopen(f"{BASE_URL}{path}", timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"{path} returned HTTP {exc.code}: {body[:240]}") from exc
    except URLError as exc:
        raise AssertionError(f"Cannot reach local server at {BASE_URL}: {exc}") from exc
    return payload


def assert_not_fallback(label: str, payload: dict) -> None:
    if payload.get("fallback"):
        detail = {
            "warning": payload.get("warning"),
            "error_kind": payload.get("error_kind"),
            "upstream_status": payload.get("upstream_status"),
            "upstream_path": payload.get("upstream_path"),
        }
        raise AssertionError(f"{label} is in fallback mode: {detail}")


def main() -> int:
    health = get_json("/api/health")
    if health != {"status": "ok"}:
        raise AssertionError(f"Unexpected /api/health payload: {health}")

    status = get_json("/api/status")
    if status.get("status") != "ok":
        raise AssertionError(f"/api/status is degraded: {status.get('catalog')}")

    analysis = get_json("/api/analysis?min_score=4")
    assert_not_fallback("/api/analysis", analysis)
    if len(analysis.get("datasets", []) or []) < 10:
        raise AssertionError(f"/api/analysis returned too few datasets: {len(analysis.get('datasets', []) or [])}")

    analytics = get_json("/api/analytics?min_score=4")
    assert_not_fallback("/api/analytics", analytics)
    if (analytics.get("summary") or {}).get("dataset_count", 0) < 10:
        raise AssertionError(f"/api/analytics summary looks wrong: {analytics.get('summary')}")

    print(
        "live api smoke ok:",
        f"datasets={len(analysis.get('datasets', []) or [])}",
        f"links={analysis.get('link_count')}",
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as exc:
        print(f"live api smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
