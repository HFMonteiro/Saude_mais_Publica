#!/usr/bin/env python3
"""
Transparent local explorer for the Opendatasoft Explore API used by
transparencia.sns.gov.pt.

This server serves a static frontend and exposes API helper endpoints with a local
proxy so the browser does not hit CORS issues.
"""

from __future__ import annotations

import json
import logging
import math
import random
import re
import threading
import time
import unicodedata
from datetime import date
from email.utils import parsedate_to_datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse, urlencode
from urllib.request import Request, urlopen

ODS_BASE = "https://transparencia.sns.gov.pt/api/explore/v2.1"
CACHE_TTL_SECONDS = 60 * 5
MAX_CACHE_ENTRIES = 80
MAX_CACHE_BYTES = 12 * 1024 * 1024
MAX_CACHE_ENTRY_BYTES = 2 * 1024 * 1024
ODS_MAX_RETRIES = 3
ODS_BACKOFF_BASE_SECONDS = 0.45
ODS_BACKOFF_MAX_SECONDS = 4.0
MAX_DATASET_LIMIT = 100
ANALYSIS_DATASET_LIMIT = 600
MAX_ANALYSIS_LINKS = 12000
MAX_SHARED_FIELDS_PER_LINK = 12
MAX_OPPORTUNITY_DATASETS = 80
MAX_RECENT_LIMIT = 100
DEFAULT_DATA_ANALYTICS_LIMIT = 80
MAX_DATA_ANALYTICS_LIMIT = 100
DEFAULT_MIN_SCORE = 1
MAX_MIN_SCORE = 10
DEFAULT_RECENT_LIMIT = 60
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 180
DEFAULT_ORIGINS = {
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost",
    "http://127.0.0.1",
}
FIELD_STOP_WORDS = {
    "a",
    "aos",
    "aqui",
    "as",
    "at",
    "da",
    "de",
    "das",
    "do",
    "dos",
    "e",
    "em",
    "en",
    "na",
    "nao",
    "no",
    "nos",
    "por",
    "que",
    "se",
    "sem",
    "to",
}

MEGA_THEMES = {
    "Acesso & Produção": {
        "description": "Consultas, urgências, cirurgias, referenciações, tempos de espera e atividade assistencial.",
        "terms": [
            "acesso",
            "admitidos",
            "atendimento",
            "atendimentos",
            "cirurgia",
            "cirurgias",
            "consulta",
            "consultas",
            "episodios",
            "inscritos",
            "lic",
            "referenciados",
            "referenciacoes",
            "tempo",
            "tmrg",
            "urgencia",
            "urgencias",
        ],
    },
    "Recursos Humanos": {
        "description": "Profissionais, vínculos, formação, ausências, internato e capacidade operacional.",
        "terms": [
            "ausencia",
            "ausencias",
            "enfermeiros",
            "formacao",
            "grupo profissional",
            "medicos",
            "pessoal",
            "profissionais",
            "trabalhadores",
            "vagas",
            "vinculacao",
        ],
    },
    "Finanças & Compras": {
        "description": "Despesa, dívida, pagamentos, contratos, medicamentos e agregados económico-financeiros.",
        "terms": [
            "acordos",
            "agregados",
            "compras",
            "conta",
            "despesa",
            "divida",
            "financeiros",
            "fornecedores",
            "medicamentos",
            "pagamento",
            "pagamentos",
            "prescricao",
        ],
    },
    "Qualidade & Resultados": {
        "description": "Indicadores clínicos, mortalidade, morbilidade, segurança, satisfação e resultados.",
        "terms": [
            "asma",
            "avc",
            "diabetes",
            "dpoc",
            "fraturas",
            "hipertensao",
            "mortalidade",
            "morbilidade",
            "qualidade",
            "satisfacao",
        ],
    },
    "Saúde Pública & Emergência": {
        "description": "Epidemiologia, emergência médica, sazonalidade, rastreios, sangue e vigilância.",
        "terms": [
            "acionamentos",
            "antibioticos",
            "certificados",
            "covid",
            "dadores",
            "emergencia",
            "gripe",
            "legionella",
            "oncologicos",
            "rastreios",
            "sangue",
            "sns24",
        ],
    },
    "Rede & Território": {
        "description": "Entidades, regiões, localização, unidades funcionais, RNCCI e organização da rede.",
        "terms": [
            "ars",
            "entidade",
            "geografica",
            "hospital",
            "hospitais",
            "instituicao",
            "localizacao",
            "rede",
            "regiao",
            "rncci",
            "uls",
            "unidades",
        ],
    },
}

DEFAULT_THEME = "Outros"
DATASET_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,120}$")
ALLOWED_STATIC_PATHS = {
    "/",
    "/index.html",
    "/app.js",
    "/styles.css",
    "/crosswalk.html",
    "/crosswalk.js",
    "/analytics.html",
    "/analytics.js",
    "/metodologia.html",
    "/visual_assets/logo.jpg",
    "/visual_assets/background.jpg",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("transparencia_connect")

_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
_fetch_locks: dict[str, threading.Lock] = {}
_fetch_locks_lock = threading.Lock()
_analysis_lock = threading.Lock()
_rate_limit_lock = threading.Lock()
_rate_limit_hits: dict[str, list[float]] = {}


def _now() -> float:
    return time.time()


def _cache_entry_is_fresh(entry: dict) -> bool:
    return _now() - float(entry.get("timestamp", 0)) <= CACHE_TTL_SECONDS


def _cache_get_entry(key: str, *, fresh_only: bool = True) -> dict | None:
    with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        if fresh_only and not _cache_entry_is_fresh(entry):
            return None
        return dict(entry)


def _cache_get(key: str):
    entry = _cache_get_entry(key, fresh_only=True)
    return entry.get("payload") if entry else None


def _payload_size_bytes(payload: dict) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _cache_set(
    key: str,
    payload: dict,
    *,
    cacheable: bool = True,
    etag: str | None = None,
    last_modified: str | None = None,
    status: str = "fresh",
):
    if not cacheable:
        return
    payload_size = _payload_size_bytes(payload)
    if payload_size > MAX_CACHE_ENTRY_BYTES:
        return
    with _cache_lock:
        if key in _cache:
            del _cache[key]
        while _cache and (
            len(_cache) >= MAX_CACHE_ENTRIES
            or sum(int(entry.get("size", 0)) for entry in _cache.values()) + payload_size > MAX_CACHE_BYTES
        ):
            oldest_key = min(_cache.items(), key=lambda item: item[1].get("timestamp", 0))[0]
            del _cache[oldest_key]
        if len(_cache) >= MAX_CACHE_ENTRIES:
            oldest_key = min(_cache.items(), key=lambda item: item[1].get("timestamp", 0))[0]
            del _cache[oldest_key]
        _cache[key] = {
            "timestamp": _now(),
            "fetched_at": int(_now()),
            "size": payload_size,
            "payload": payload,
            "etag": etag,
            "last_modified": last_modified,
            "cache_status": status,
        }


def _cache_mark_revalidated(key: str) -> dict | None:
    with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        entry["timestamp"] = _now()
        entry["fetched_at"] = int(_now())
        entry["cache_status"] = "revalidated"
        return dict(entry)


def _cache_meta_public(key: str, *, default_status: str = "bypass") -> dict:
    entry = _cache_get_entry(key, fresh_only=False)
    if not entry:
        return {
            "cache_status": default_status,
            "source_headers": {"etag": False, "last_modified": False},
        }
    return {
        "cache_status": entry.get("cache_status", default_status),
        "source_headers": {
            "etag": bool(entry.get("etag")),
            "last_modified": bool(entry.get("last_modified")),
        },
    }


def _cache_key(path: str, params: dict[str, str] | None = None) -> str:
    return path + "?" + urlencode(params or {}, doseq=True)


def _fetch_lock_for(key: str) -> threading.Lock:
    with _fetch_locks_lock:
        if len(_fetch_locks) > MAX_CACHE_ENTRIES * 4:
            cached_keys = set(_cache.keys())
            for stale_key in list(_fetch_locks):
                if stale_key not in cached_keys:
                    _fetch_locks.pop(stale_key, None)
        lock = _fetch_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _fetch_locks[key] = lock
        return lock


def _rate_limit_retry_after(client_id: str) -> int | None:
    now = _now()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    with _rate_limit_lock:
        hits = [timestamp for timestamp in _rate_limit_hits.get(client_id, []) if timestamp >= cutoff]
        if len(hits) >= RATE_LIMIT_MAX_REQUESTS:
            _rate_limit_hits[client_id] = hits
            return max(1, int(RATE_LIMIT_WINDOW_SECONDS - (now - hits[0])))
        hits.append(now)
        _rate_limit_hits[client_id] = hits
        if len(_rate_limit_hits) > 256:
            for key in list(_rate_limit_hits):
                _rate_limit_hits[key] = [timestamp for timestamp in _rate_limit_hits[key] if timestamp >= cutoff]
                if not _rate_limit_hits[key]:
                    del _rate_limit_hits[key]
        return None


def _normalize_token(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(value.split())


def _normalize_origin(origin: str | None) -> str | None:
    if not origin:
        return None
    candidate = origin.rstrip("/")
    if candidate in DEFAULT_ORIGINS:
        return candidate
    return None


def _parse_int_param(value: str | None, default: int, minimum: int, maximum: int, field_name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field_name}: must be an integer")
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"Invalid {field_name}: expected {minimum}..{maximum}")
    return parsed


def _parse_dataset_id(value: str | None) -> str:
    if not value:
        raise ValueError("dataset_id required")
    dataset_id = unquote(value).strip()
    if len(dataset_id) < 1 or len(dataset_id) > 120:
        raise ValueError("Invalid dataset_id length")
    if not DATASET_ID_RE.fullmatch(dataset_id):
        raise ValueError("Invalid dataset_id format")
    return dataset_id


def _field_tokens(field: dict) -> list[str]:
    tokens: set[str] = set()
    for key in ("name", "label"):
        raw = _normalize_token(str(field.get(key, "")))
        if not raw:
            continue
        tokens.add(raw)
        tokens.update(raw.split())
    return [
        token
        for token in tokens
        if len(token) > 2 and token not in {"id", "ids", "the", "and", "of"} and token not in FIELD_STOP_WORDS
    ]


def _get_dataset_title(meta: dict) -> str:
    if not isinstance(meta, dict):
        return ""
    fallback = meta.get("default", {}).get("title") if isinstance(meta.get("default", {}), dict) else ""
    return fallback or ""


def _classify_dataset(dataset_id: str, title: str, fields: list[str]) -> str:
    text = _normalize_token(" ".join([dataset_id, title, *fields]))
    best_theme = DEFAULT_THEME
    best_score = 0
    for theme, config in MEGA_THEMES.items():
        score = 0
        for term in config["terms"]:
            normalized = _normalize_token(term)
            if normalized and normalized in text:
                score += 2 if " " in normalized else 1
        if score > best_score:
            best_score = score
            best_theme = theme
    return best_theme


def _extract_year(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        year = int(value)
        return year if 1900 <= year <= 2200 else None
    text = str(value)
    match = re.search(r"(20\d{2}|19\d{2})", text)
    if match:
        return int(match.group(1))
    return None


def _extract_period_key(value, field_name: str | None = None) -> tuple[str, str] | None:
    """Return stable sortable period key and compact display label."""
    year = _extract_year(value)
    if year is None:
        return None
    text = _normalize_token(str(value))
    field_text = _normalize_token(field_name or "")

    month = None
    match = re.search(r"(?:^|\D)(20\d{2}|19\d{2})[-/._ ](0?[1-9]|1[0-2])(?:\D|$)", text)
    if match:
        month = int(match.group(2))
    if month is None:
        match = re.search(r"(?:^|\D)(0?[1-9]|1[0-2])[-/._ ](20\d{2}|19\d{2})(?:\D|$)", text)
        if match:
            month = int(match.group(1))
    if month is None:
        match = re.search(r"(20\d{2}|19\d{2})(0[1-9]|1[0-2])", text)
        if match:
            month = int(match.group(2))
    if month is not None and ("mes" in field_text or "mensal" in text or "mes" in text or re.search(r"[-/._ ]", text)):
        return (f"{year:04d}-{month:02d}", f"{year:04d}-{month:02d}")

    quarter = None
    match = re.search(r"(?:q|t|trimestre)[-/._ ]?([1-4])", text)
    if match:
        quarter = int(match.group(1))
    elif "trimestre" in field_text:
        match = re.search(r"(?:^|\D)([1-4])(?:\D|$)", text)
        if match:
            quarter = int(match.group(1))
    if quarter is not None:
        return (f"{year:04d}-T{quarter}", f"{year:04d} T{quarter}")

    return (f"{year:04d}", f"{year:04d}")


def _is_recent_record(record: dict, temporal_field: str | None, min_year: int) -> bool:
    candidate_values = []
    if temporal_field and temporal_field in record:
        candidate_values.append(record.get(temporal_field))
    candidate_values.extend(record.get(key) for key in ("tempo", "periodo", "data", "trimestre", "ultimo_dia") if key in record)
    for value in candidate_values:
        year = _extract_year(value)
        if year and year >= min_year:
            return True
    return False


def _pick_temporal_field(fields: list[dict]) -> str | None:
    preferred = ["tempo", "periodo", "data", "trimestre", "ultimo_dia", "detailled_quarter", "quarter"]
    names = [field.get("name") for field in fields if field.get("name")]
    for preferred_name in preferred:
        if preferred_name in names:
            return preferred_name
    for field in fields:
        name = field.get("name", "")
        field_type = field.get("type", "")
        if field_type in {"date", "datetime"} or any(token in name for token in preferred):
            return name
    return None


def _pick_stable_order_field(fields: list[dict]) -> str | None:
    preferred = ["datasetid", "recordid", "id", "codigo", "cod", "entidade", "regiao", "periodo"]
    names = [field.get("name") for field in fields if field.get("name")]
    normalized_by_name = {_normalize_token(name): name for name in names}
    for preferred_name in preferred:
        for normalized, original in normalized_by_name.items():
            if normalized == preferred_name or normalized.endswith(f" {preferred_name}"):
                return original
    return names[0] if names else None


def _canonical_field_type(field: dict) -> str:
    raw_type = _normalize_token(str(field.get("type") or field.get("annotations", {}).get("type") or ""))
    name = _normalize_token(str(field.get("name") or field.get("label") or ""))
    if raw_type in {"date", "datetime"} or re.search(r"\b(data|date|tempo|periodo|ano|mes|trimestre)\b", name):
        return "date"
    if raw_type in {"int", "integer", "long"}:
        return "integer"
    if raw_type in {"double", "float", "decimal", "number"}:
        return "float"
    if raw_type in {"geo point", "geo shape", "geopoint", "geoshape"} or re.search(r"\b(latitude|longitude|geo|geograf)\b", name):
        return "geo"
    if raw_type in {"text", "string"} and re.search(r"\b(tipo|grupo|categoria|regiao|ars|uls|hospital|entidade|sexo|idade)\b", name):
        return "category"
    return "text"


def _is_identifier_or_contact_field(field: dict) -> bool:
    name = _normalize_token(str(field.get("name") or field.get("label") or ""))
    return bool(
        re.search(
            r"\b(telefone|telemovel|fax|email|mail|codigo|cod|postal|nif|nipc|niss|id|url|link|latitude|longitude)\b",
            name,
        )
    )


def _is_measure_candidate(field: dict) -> bool:
    if _is_identifier_or_contact_field(field):
        return False
    canonical_type = _canonical_field_type(field)
    if canonical_type in {"date", "geo", "category"}:
        return False
    return canonical_type in {"integer", "float"} or canonical_type == "text"


def _schema_quality(fields: list[dict], observed_columns: list[str] | None = None) -> dict:
    observed = set(observed_columns or [])
    scoped_fields = [field for field in fields if not observed or field.get("name") in observed]
    typed = [_canonical_field_type(field) for field in scoped_fields]
    return {
        "field_count": len(scoped_fields),
        "typed_fields": sum(1 for field_type in typed if field_type != "text"),
        "temporal_fields": sum(1 for field_type in typed if field_type == "date"),
        "territorial_fields": sum(
            1
            for field in scoped_fields
            if re.search(r"\b(regiao|ars|uls|hospital|concelho|distrito|entidade|unidade|territorio)\b", _normalize_token(str(field.get("name") or field.get("label") or "")))
        ),
        "ignored_identifier_fields": sum(1 for field in scoped_fields if _is_identifier_or_contact_field(field)),
    }


def _build_ods_url(path: str, params: dict[str, str] | None = None) -> str:
    query = ""
    if params:
        normalized = {key: str(value) for key, value in params.items()}
        query = "?" + urlencode(normalized, doseq=True)
    return f"{ODS_BASE}{path}{query}"


def _retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, min(float(value), ODS_BACKOFF_MAX_SECONDS))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        delay = parsed.timestamp() - _now()
        return max(0.0, min(delay, ODS_BACKOFF_MAX_SECONDS))


def _backoff_delay(attempt: int, retry_after: str | None = None) -> float:
    explicit = _retry_after_seconds(retry_after)
    if explicit is not None and explicit > 0:
        return explicit
    cap = min(ODS_BACKOFF_MAX_SECONDS, ODS_BACKOFF_BASE_SECONDS * (2**attempt))
    return random.uniform(0, cap)


def _ods_fetch(path: str, params: dict[str, str] | None = None, *, cacheable: bool = True) -> dict:
    cache_key = _cache_key(path, params)
    if cacheable:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached

    lock = _fetch_lock_for(cache_key)
    with lock:
        if cacheable:
            cached = _cache_get(cache_key)
            if cached is not None:
                return cached
        stale_entry = _cache_get_entry(cache_key, fresh_only=False) if cacheable else None
        last_error: Exception | None = None

        for attempt in range(ODS_MAX_RETRIES):
            request = Request(_build_ods_url(path, params))
            request.add_header("Accept", "application/json")
            request.add_header("User-Agent", "transparencia-connect/1.0 (+https://github.com/hfmonteiro)")
            if stale_entry and stale_entry.get("etag"):
                request.add_header("If-None-Match", str(stale_entry["etag"]))
            if stale_entry and stale_entry.get("last_modified"):
                request.add_header("If-Modified-Since", str(stale_entry["last_modified"]))

            started_at = _now()
            try:
                with urlopen(request, timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    headers = response.headers
                    _cache_set(
                        cache_key,
                        payload,
                        cacheable=cacheable,
                        etag=headers.get("ETag"),
                        last_modified=headers.get("Last-Modified"),
                        status="fresh" if cacheable else "bypass",
                    )
                    LOGGER.info(
                        "ODS %s status=%s cache=%s attempt=%s latency_ms=%s",
                        path,
                        getattr(response, "status", 200),
                        "fresh" if cacheable else "bypass",
                        attempt + 1,
                        round((_now() - started_at) * 1000),
                    )
                    return payload
            except HTTPError as exc:
                last_error = exc
                if exc.code == 304 and stale_entry and stale_entry.get("payload") is not None:
                    revalidated = _cache_mark_revalidated(cache_key)
                    LOGGER.info("ODS %s status=304 cache=revalidated attempt=%s", path, attempt + 1)
                    return revalidated["payload"] if revalidated else stale_entry["payload"]
                if exc.code in {429, 503} and attempt < ODS_MAX_RETRIES - 1:
                    delay = _backoff_delay(attempt, exc.headers.get("Retry-After"))
                    LOGGER.info("ODS %s status=%s retry_after=%s attempt=%s", path, exc.code, round(delay, 2), attempt + 1)
                    time.sleep(delay)
                    continue
                break
            except URLError as exc:
                last_error = exc
                if attempt < ODS_MAX_RETRIES - 1:
                    delay = _backoff_delay(attempt)
                    LOGGER.info("ODS %s url_error retry_after=%s attempt=%s", path, round(delay, 2), attempt + 1)
                    time.sleep(delay)
                    continue
                break

        if stale_entry and stale_entry.get("payload") is not None:
            with _cache_lock:
                if cache_key in _cache:
                    _cache[cache_key]["cache_status"] = "stale_fallback"
            LOGGER.warning("ODS %s cache=stale_fallback error=%s", path, type(last_error).__name__ if last_error else "unknown")
            return stale_entry["payload"]
        if isinstance(last_error, HTTPError):
            raise RuntimeError(f"ODS HTTP {last_error.code} on {path}") from last_error
        if isinstance(last_error, URLError):
            raise RuntimeError("Cannot reach ODS API") from last_error
        raise RuntimeError("Cannot reach ODS API")


def _get_datasets() -> dict:
    limit = MAX_DATASET_LIMIT
    offset = 0
    all_results = []
    total_count = None

    while True:
        page = _ods_fetch(
            "/catalog/datasets",
            {
                "limit": str(limit),
                "offset": str(offset),
                "select": "dataset_id,fields,metas",
            },
        )
        results = page.get("results", []) or []
        all_results.extend(results)
        if total_count is None:
            total_count = page.get("total_count")
        if not results or len(all_results) >= ANALYSIS_DATASET_LIMIT:
            break
        if total_count is not None and len(all_results) >= min(total_count, ANALYSIS_DATASET_LIMIT):
            break
        offset += len(results)

    if total_count is None:
        return {"total_count": len(all_results), "results": all_results}
    return {"total_count": total_count, "results": all_results}


def _analyze_datasets(datasets_payload: dict) -> dict:
    results = datasets_payload.get("results", []) or []
    items = []
    key_to_datasets: dict[str, set[str]] = {}
    dataset_theme_by_id: dict[str, str] = {}
    dataset_id_to_fields: dict[str, list[str]] = {}

    for entry in results:
        dataset_id = entry.get("dataset_id") or ""
        if not dataset_id:
            continue

        metas = entry.get("metas", {}) or {}
        title = _get_dataset_title(metas).strip() or dataset_id
        records_count = ((metas.get("default") or {}).get("records_count") or 0)

        field_names = []
        metric_candidate_count = 0
        canonical_keys: set[str] = set()
        for field in entry.get("fields", []) or []:
            field_name = field.get("name") or ""
            field_label = field.get("label") or ""
            if not field_name:
                continue
            field_names.append(field_name)
            if re.search(r"\b(valor|total|taxa|numero|n_|count|volume|quantidade|dias|encargos|custo|pvp|percentagem)\b", _normalize_token(f"{field_name} {field_label}")):
                metric_candidate_count += 1
            for token in _field_tokens({"name": field_name, "label": field_label}):
                canonical_keys.add(token)
                key_to_datasets.setdefault(token, set()).add(dataset_id)

        dataset_id_to_fields[dataset_id] = sorted(canonical_keys)
        mega_theme = _classify_dataset(dataset_id, title, field_names)
        dataset_theme_by_id[dataset_id] = mega_theme
        items.append(
            {
                "dataset_id": dataset_id,
                "title": title,
                "mega_theme": mega_theme,
                "records_count": records_count,
                "field_count": len(field_names),
                "metric_candidate_count": metric_candidate_count,
                "fields": sorted({str(f) for f in field_names}),
            }
        )

    links: list[dict] = []
    for idx, ds1 in enumerate(items):
        id1 = ds1["dataset_id"]
        keys1 = set(dataset_id_to_fields.get(id1, []))
        for ds2 in items[idx + 1 :]:
            id2 = ds2["dataset_id"]
            keys2 = set(dataset_id_to_fields.get(id2, []))
            shared = sorted(keys1 & keys2)
            if not shared:
                continue
            links.append(
                {
                    "source": id1,
                    "target": id2,
                    "score": len(shared),
                    "shared_fields": shared[:MAX_SHARED_FIELDS_PER_LINK],
                }
            )

    opportunities = []
    for key, ds_list in key_to_datasets.items():
        unique_datasets = sorted(ds_list)
        if len(unique_datasets) < 2:
            continue
        opportunities.append(
            {
                "key": key,
                "dataset_ids": unique_datasets[:MAX_OPPORTUNITY_DATASETS],
                "dataset_count": len(unique_datasets),
                "truncated": len(unique_datasets) > MAX_OPPORTUNITY_DATASETS,
            }
        )

    opportunities.sort(key=lambda item: (-item["dataset_count"], item["key"]))

    links.sort(key=lambda item: (-item["score"], item["source"], item["target"]))
    links = links[:MAX_ANALYSIS_LINKS]
    theme_counts: dict[str, int] = {}
    theme_link_scores: dict[str, int] = {}
    for item in items:
        theme_counts[item["mega_theme"]] = theme_counts.get(item["mega_theme"], 0) + 1
    for link in links:
        source_theme = dataset_theme_by_id.get(link["source"], DEFAULT_THEME)
        target_theme = dataset_theme_by_id.get(link["target"], DEFAULT_THEME)
        if source_theme == target_theme:
            theme_link_scores[source_theme] = theme_link_scores.get(source_theme, 0) + link["score"]
        else:
            key = f"{source_theme} ↔ {target_theme}"
            theme_link_scores[key] = theme_link_scores.get(key, 0) + link["score"]

    theme_rankings = [
        {
            "theme": theme,
            "dataset_count": count,
            "link_score": theme_link_scores.get(theme, 0),
            "description": MEGA_THEMES.get(theme, {}).get("description", "Datasets não classificados nos temas principais."),
        }
        for theme, count in theme_counts.items()
    ]
    theme_rankings.sort(key=lambda item: (-item["dataset_count"], -item["link_score"], item["theme"]))

    return {
        "datasets": items,
        "links": links,
        "opportunities": opportunities[:150],
        "themes": theme_rankings,
        "total": len(items),
        "generated_at": int(_now()),
    }


def _get_analysis_catalog() -> dict:
    cache_key = "analysis:catalog"
    cached = _cache_get(cache_key)
    if cached is None:
        with _analysis_lock:
            cached = _cache_get(cache_key)
            if cached is None:
                catalog = _get_datasets()
                cached = _analyze_datasets(catalog)
                _cache_set(cache_key, cached)
    return cached


def _dimension_kind(field: str) -> str:
    normalized = _normalize_token(field)
    if re.search(r"\b(tempo|periodo|ano|data|trimestre|semana|mes|dia)\b", normalized):
        return "temporal"
    if re.search(r"\b(regiao|ars|uls|localizacao|geografica|concelho|distrito|postal|hospital)\b", normalized):
        return "territorial"
    if re.search(r"\b(entidade|instituicao|unidade|servico|grupo|fornecedor|utente|doente)\b", normalized):
        return "entidade"
    if re.search(r"\b(valor|total|taxa|numero|contagem|volume|quantidade|dias|encargos|custo|pvp)\b", normalized):
        return "medida"
    return "generico"


def _dimension_weight(kind: str) -> float:
    return {
        "temporal": 1.15,
        "territorial": 1.2,
        "entidade": 1.25,
        "medida": 0.78,
        "generico": 0.62,
    }.get(kind, 0.7)


def _confidence_label(score: float) -> str:
    if score >= 8:
        return "alta"
    if score >= 5:
        return "media"
    return "exploratoria"


def _risk_flags(kinds: set[str], fields: list[str]) -> list[str]:
    flags = []
    if "generico" in kinds:
        flags.append("campo_generico")
    if "medida" in kinds and not ({"temporal", "territorial", "entidade"} & kinds):
        flags.append("medida_sem_granularidade")
    if len(fields) < 2:
        flags.append("poucas_chaves")
    return flags


def _matched_area_types(source: dict, target: dict, fields: list[str]) -> list[str]:
    text = " ".join(
        [
            source.get("title") or "",
            target.get("title") or "",
            source.get("mega_theme") or "",
            target.get("mega_theme") or "",
            *fields,
        ]
    )
    normalized = _normalize_token(text)
    areas = []
    patterns = [
        ("hospital", r"\b(hospital|hospitais|centro hospitalar|ch|epe)\b"),
        ("ULS", r"\b(uls|unidade local de saude)\b"),
        ("ACES/Cuidados primarios", r"\b(aces|cuidados primarios|centro de saude|usf|ucsp)\b"),
        ("ARS/Regiao", r"\b(ars|regiao|regional|norte|centro|lisboa|alentejo|algarve)\b"),
        ("Concelho/Distrito", r"\b(concelho|distrito|municipio|localizacao)\b"),
        ("Farmacia/Medicamento", r"\b(farmacia|medicamento|medicamentos|farmaceutic)\b"),
        ("Nacional", r"\b(nacional|pais|sns)\b"),
    ]
    for label, pattern in patterns:
        if re.search(pattern, normalized):
            areas.append(label)
    return areas or ["Area nao especificada"]


def _likelihood_assessment(semantic_score: float, risk_flags: list[str]) -> dict:
    adjusted = semantic_score - (len(risk_flags) * 0.75)
    if adjusted >= 8:
        level = "alta"
    elif adjusted >= 5:
        level = "media"
    else:
        level = "baixa"
    return {"level": level, "score": round(max(0, adjusted), 2)}


def _public_health_impact(source: dict, target: dict, fields: list[str], kinds: set[str]) -> dict:
    text = _normalize_token(
        " ".join(
            [
                source.get("title") or "",
                target.get("title") or "",
                source.get("mega_theme") or "",
                target.get("mega_theme") or "",
                *fields,
            ]
        )
    )
    drivers = []
    score = 1.0
    impact_patterns = [
        ("outcomes clinicos", r"\b(mortalidade|morbilidade|obito|doenca|diagnostico|infeccao|surto)\b", 2.2),
        ("acesso e tempos de resposta", r"\b(urgencia|espera|tempo|consulta|cirurgia|internamento|episodio)\b", 1.8),
        ("capacidade assistencial", r"\b(cama|lotacao|profissional|medico|enfermeiro|unidade|servico)\b", 1.4),
        ("medicamento e terapêutica", r"\b(medicamento|farmacia|stock|reserva|consumo|prescricao)\b", 1.3),
        ("financeiro com impacto operacional", r"\b(custo|encargo|despesa|pagamento|contrato)\b", 0.8),
    ]
    for label, pattern, weight in impact_patterns:
        if re.search(pattern, text):
            score += weight
            drivers.append(label)
    if "territorial" in kinds:
        score += 1.0
        drivers.append("estratificacao territorial")
    if "entidade" in kinds:
        score += 0.9
        drivers.append("comparacao por entidade")
    if "temporal" in kinds:
        score += 0.7
        drivers.append("leitura temporal")
    if score >= 5.4:
        level = "alto"
    elif score >= 3.2:
        level = "medio"
    else:
        level = "baixo"
    return {"level": level, "score": round(score, 2), "drivers": drivers[:5] or ["sinal exploratorio"]}


def _build_analytics(analysis: dict, min_score: int) -> dict:
    cache_key = f"analytics:{analysis.get('generated_at')}:{min_score}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    datasets = analysis.get("datasets", []) or []
    dataset_by_id = {dataset["dataset_id"]: dataset for dataset in datasets}
    links = [link for link in (analysis.get("links", []) or []) if link.get("score", 0) >= min_score]

    dimensions: dict[str, dict] = {}
    theme_pairs: dict[str, dict] = {}
    dimension_pairs: dict[str, dict] = {}
    correlations = []

    for link in links:
        source = dataset_by_id.get(link.get("source"), {"dataset_id": link.get("source"), "title": link.get("source"), "mega_theme": DEFAULT_THEME})
        target = dataset_by_id.get(link.get("target"), {"dataset_id": link.get("target"), "title": link.get("target"), "mega_theme": DEFAULT_THEME})
        fields = link.get("shared_fields", []) or []
        kinds = {_dimension_kind(field) for field in fields}
        weighted_score = round(sum(_dimension_weight(_dimension_kind(field)) for field in fields), 2)
        semantic_score = round((link.get("score", 0) * 0.65) + weighted_score, 2)
        risk_flags = _risk_flags(kinds, fields)
        likelihood = _likelihood_assessment(semantic_score, risk_flags)
        impact = _public_health_impact(source, target, fields, kinds)
        matched_areas = _matched_area_types(source, target, fields)

        for field in fields:
            kind = _dimension_kind(field)
            entry = dimensions.setdefault(
                field,
                {
                    "field": field,
                    "kind": kind,
                    "dataset_ids": set(),
                    "themes": set(),
                    "link_count": 0,
                    "score_sum": 0,
                },
            )
            entry["dataset_ids"].update([source["dataset_id"], target["dataset_id"]])
            entry["themes"].update([source.get("mega_theme") or DEFAULT_THEME, target.get("mega_theme") or DEFAULT_THEME])
            entry["link_count"] += 1
            entry["score_sum"] += link.get("score", 0)

        sorted_kinds = sorted(kinds)
        for idx, first in enumerate(sorted_kinds):
            for second in sorted_kinds[idx:]:
                key = f"{first}|{second}"
                pair = dimension_pairs.setdefault(key, {"source": first, "target": second, "count": 0, "score_sum": 0})
                pair["count"] += 1
                pair["score_sum"] += link.get("score", 0)

        theme_a = source.get("mega_theme") or DEFAULT_THEME
        theme_b = target.get("mega_theme") or DEFAULT_THEME
        theme_key = " ↔ ".join(sorted([theme_a, theme_b]))
        theme_entry = theme_pairs.setdefault(
            theme_key,
            {
                "source_theme": sorted([theme_a, theme_b])[0],
                "target_theme": sorted([theme_a, theme_b])[1],
                "link_count": 0,
                "score_sum": 0,
                "dimensions": {},
            },
        )
        theme_entry["link_count"] += 1
        theme_entry["score_sum"] += link.get("score", 0)
        for kind in kinds:
            theme_entry["dimensions"][kind] = theme_entry["dimensions"].get(kind, 0) + 1

        correlations.append(
            {
                "source": source["dataset_id"],
                "target": target["dataset_id"],
                "source_title": source.get("title") or source["dataset_id"],
                "target_title": target.get("title") or target["dataset_id"],
                "source_theme": theme_a,
                "target_theme": theme_b,
                "score": link.get("score", 0),
                "semantic_score": semantic_score,
                "confidence": _confidence_label(semantic_score),
                "shared_fields": fields,
                "dimension_kinds": sorted(kinds),
                "risk_flags": risk_flags,
                "public_health_model": {
                    "likelihood": likelihood,
                    "impact": impact,
                    "matched_areas": matched_areas,
                    "matrix_cell": f"{likelihood['level']}|{impact['level']}",
                },
                "join_recipe": {
                    "suggested_keys": fields[:6],
                    "preferred_join": "left" if "entidade" in kinds or "territorial" in kinds else "inner",
                    "needs_validation": bool(risk_flags),
                },
            }
        )

    dimension_rows = []
    for entry in dimensions.values():
        dataset_count = len(entry["dataset_ids"])
        theme_count = len(entry["themes"])
        link_count = entry["link_count"]
        dimension_rows.append(
            {
                "field": entry["field"],
                "kind": entry["kind"],
                "dataset_count": dataset_count,
                "theme_count": theme_count,
                "link_count": link_count,
                "score_sum": entry["score_sum"],
                "coverage_score": round((dataset_count * 0.55) + (theme_count * 1.4) + (link_count * 0.08), 2),
            }
        )

    dimension_rows.sort(key=lambda item: (-item["coverage_score"], -item["dataset_count"], item["field"]))
    correlations.sort(key=lambda item: (-item["semantic_score"], -item["score"], item["source_title"], item["target_title"]))
    theme_matrix = []
    for entry in theme_pairs.values():
        theme_matrix.append(
            {
                "source_theme": entry["source_theme"],
                "target_theme": entry["target_theme"],
                "link_count": entry["link_count"],
                "score_sum": entry["score_sum"],
                "avg_score": round(entry["score_sum"] / entry["link_count"], 2) if entry["link_count"] else 0,
                "dimensions": sorted(entry["dimensions"].items(), key=lambda item: (-item[1], item[0])),
            }
        )
    theme_matrix.sort(key=lambda item: (-item["score_sum"], -item["link_count"], item["source_theme"], item["target_theme"]))

    dimension_matrix = list(dimension_pairs.values())
    dimension_matrix.sort(key=lambda item: (-item["score_sum"], -item["count"], item["source"], item["target"]))

    public_health_matrix: dict[str, dict] = {}
    for item in correlations:
        model = item.get("public_health_model", {})
        likelihood = (model.get("likelihood") or {}).get("level") or "baixa"
        impact = (model.get("impact") or {}).get("level") or "baixo"
        key = f"{likelihood}|{impact}"
        cell = public_health_matrix.setdefault(
            key,
            {
                "likelihood": likelihood,
                "impact": impact,
                "count": 0,
                "semantic_score_sum": 0,
                "area_counts": {},
                "examples": [],
            },
        )
        cell["count"] += 1
        cell["semantic_score_sum"] += item.get("semantic_score", 0)
        for area in model.get("matched_areas") or ["Area nao especificada"]:
            cell["area_counts"][area] = cell["area_counts"].get(area, 0) + 1
        if len(cell["examples"]) < 4:
            cell["examples"].append(
                {
                    "source_title": item["source_title"],
                    "target_title": item["target_title"],
                    "matched_areas": model.get("matched_areas") or ["Area nao especificada"],
                    "drivers": (model.get("impact") or {}).get("drivers", []),
                    "score": item.get("semantic_score", 0),
                }
            )

    public_health_rows = []
    for cell in public_health_matrix.values():
        public_health_rows.append(
            {
                "likelihood": cell["likelihood"],
                "impact": cell["impact"],
                "count": cell["count"],
                "avg_semantic_score": round(cell["semantic_score_sum"] / cell["count"], 2) if cell["count"] else 0,
                "area_counts": sorted(cell["area_counts"].items(), key=lambda entry: (-entry[1], entry[0])),
                "examples": cell["examples"],
            }
        )

    high_confidence = sum(1 for item in correlations if item["confidence"] == "alta")
    result = {
        "summary": {
            "dataset_count": len(datasets),
            "link_count": len(links),
            "dimension_count": len(dimension_rows),
            "theme_pair_count": len(theme_matrix),
            "high_confidence_count": high_confidence,
            "min_score": min_score,
        },
        "dimensions": dimension_rows[:120],
        "correlations": correlations[:220],
        "theme_matrix": theme_matrix[:80],
        "dimension_matrix": dimension_matrix[:40],
        "public_health_matrix": public_health_rows,
        "datasets": [
            {
                "dataset_id": item["dataset_id"],
                "title": item["title"],
                "mega_theme": item["mega_theme"],
                "records_count": item["records_count"],
                "field_count": item.get("field_count", 0),
                "metric_candidate_count": item.get("metric_candidate_count", 0),
            }
            for item in datasets
        ],
        "themes": analysis.get("themes", []),
        "generated_at": analysis.get("generated_at"),
    }
    _cache_set(cache_key, result)
    return result


def _recent_records(dataset_id: str, limit: int) -> dict:
    cache_key = f"recent:{dataset_id}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    dataset_path = f"/catalog/datasets/{quote(dataset_id)}"
    dataset = _ods_fetch(dataset_path, None)
    fields = dataset.get("fields", []) or []
    temporal_field = _pick_temporal_field(fields)
    min_year = date.today().year - 2
    safe_limit = min(max(limit, 1), MAX_RECENT_LIMIT)
    params = {"limit": str(safe_limit)}
    ordering = "api_default"
    if temporal_field:
        params["order_by"] = f"{temporal_field} desc"
        ordering = "temporal_desc"
    else:
        stable_field = _pick_stable_order_field(fields)
        if stable_field:
            params["order_by"] = f"{stable_field} asc"
            ordering = "stable_field"

    records_path = f"/catalog/datasets/{quote(dataset_id)}/records"
    try:
        payload = _ods_fetch(records_path, params, cacheable=True)
    except Exception:
        params = {"limit": str(safe_limit)}
        ordering = "api_default"
        payload = _ods_fetch(records_path, params, cacheable=True)

    records = payload.get("results", []) or []
    recent = [record for record in records if _is_recent_record(record, temporal_field, min_year)]
    if not recent:
        recent = records[: min(len(records), limit)]

    columns = []
    for field in fields:
        name = field.get("name")
        if name and any(name in record for record in recent[:20]):
            columns.append({"name": name, "label": field.get("label") or name, "type": _canonical_field_type(field)})
        if len(columns) >= 12:
            break

    meta = _cache_meta_public(_cache_key(records_path, params))
    result = {
        "dataset_id": dataset_id,
        "temporal_field": temporal_field,
        "min_year": min_year,
        "ordering": ordering,
        "schema_quality": _schema_quality(fields, [column["name"] for column in columns]),
        **meta,
        "columns": columns,
        "records": recent[:safe_limit],
        "returned_count": len(recent[:safe_limit]),
        "source_count": len(records),
    }
    _cache_set(cache_key, result)
    return result


def _coerce_number(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        return number if number == number and abs(number) != float("inf") else None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\u00a0", " ").replace("%", "").strip()
    text = re.sub(r"\s+", "", text)
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")
    text = re.sub(r"[^0-9+\-.]", "", text)
    if text in {"", "+", "-", ".", "+.", "-."}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number == number and abs(number) != float("inf") else None


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0
    avg = _mean(values)
    return (sum((value - avg) ** 2 for value in values) / (len(values) - 1)) ** 0.5


def _pearson(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 8:
        return None
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    avg_x = _mean(xs)
    avg_y = _mean(ys)
    num = sum((x - avg_x) * (y - avg_y) for x, y in pairs)
    den_x = sum((x - avg_x) ** 2 for x in xs)
    den_y = sum((y - avg_y) ** 2 for y in ys)
    if den_x <= 0 or den_y <= 0:
        return None
    return num / ((den_x * den_y) ** 0.5)


def _safe_label(field: dict) -> str:
    return field.get("label") or field.get("name") or ""


def _entropy_ratio(counts: dict[str, int]) -> float:
    total = sum(counts.values())
    if total <= 0 or len(counts) <= 1:
        return 0
    entropy = 0.0
    for count in counts.values():
        probability = count / total
        if probability > 0:
            entropy -= probability * (math.log(probability) / math.log(len(counts)))
    return max(0, min(1, entropy))


def _power_iteration(matrix: list[list[float]], iterations: int = 28) -> tuple[float, list[float]]:
    size = len(matrix)
    if size == 0:
        return 0, []
    vector = [1 / (size**0.5)] * size
    for _ in range(iterations):
        next_vector = [sum(matrix[row][col] * vector[col] for col in range(size)) for row in range(size)]
        norm = sum(value * value for value in next_vector) ** 0.5
        if norm <= 0:
            break
        vector = [value / norm for value in next_vector]
    eigenvalue = sum(vector[row] * sum(matrix[row][col] * vector[col] for col in range(size)) for row in range(size))
    return max(0, eigenvalue), vector


def _build_pca_summary(
    numeric_profiles: list[dict],
    numeric_by_column: dict[str, dict[int, float]],
    sample_size: int,
) -> dict:
    fields = [profile["field"] for profile in numeric_profiles[:8]]
    if len(fields) < 2 or sample_size < 8:
        return {"available": False, "reason": "Amostra sem medidas numericas suficientes."}

    columns = []
    usable_fields = []
    for field in fields:
        values_by_idx = numeric_by_column.get(field, {})
        values = [values_by_idx.get(idx) for idx in range(sample_size)]
        non_null = [value for value in values if value is not None]
        if len(non_null) < 5:
            continue
        avg = _mean(non_null)
        stddev = _stddev(non_null) or 1
        columns.append([((value - avg) / stddev) if value is not None else 0 for value in values])
        usable_fields.append(field)

    size = len(usable_fields)
    if size < 2:
        return {"available": False, "reason": "Amostra sem medidas numericas suficientes."}

    covariance = [[0.0 for _ in range(size)] for _ in range(size)]
    denominator = max(sample_size - 1, 1)
    for row in range(size):
        for col in range(size):
            covariance[row][col] = sum(columns[row][idx] * columns[col][idx] for idx in range(sample_size)) / denominator

    eigen1, vector1 = _power_iteration(covariance)
    deflated = [
        [covariance[row][col] - eigen1 * vector1[row] * vector1[col] for col in range(size)]
        for row in range(size)
    ]
    eigen2, vector2 = _power_iteration(deflated)
    total_variance = sum(max(covariance[idx][idx], 0) for idx in range(size)) or 1
    profile_by_field = {profile["field"]: profile for profile in numeric_profiles}
    loadings = []
    for idx, field in enumerate(usable_fields):
        profile = profile_by_field.get(field, {"label": field})
        loadings.append(
            {
                "field": field,
                "label": profile.get("label") or field,
                "pc1": round(vector1[idx], 4),
                "pc2": round(vector2[idx] if vector2 else 0, 4),
                "magnitude": round(((vector1[idx] ** 2) + ((vector2[idx] if vector2 else 0) ** 2)) ** 0.5, 4),
            }
        )
    loadings.sort(key=lambda item: (-item["magnitude"], item["field"]))
    return {
        "available": True,
        "explained_variance": {
            "pc1": round(eigen1 / total_variance, 4),
            "pc2": round(eigen2 / total_variance, 4),
        },
        "loadings": loadings,
    }


def _build_feature_importance(
    numeric_profiles: list[dict],
    categorical_profiles: list[dict],
    correlations: list[dict],
    sample_size: int,
    temporal_field: str | None,
) -> list[dict]:
    if sample_size <= 0:
        return []
    correlation_centrality: dict[str, float] = {}
    for correlation in correlations:
        strength = correlation.get("abs_correlation", 0)
        correlation_centrality[correlation["field_a"]] = correlation_centrality.get(correlation["field_a"], 0) + strength
        correlation_centrality[correlation["field_b"]] = correlation_centrality.get(correlation["field_b"], 0) + strength

    max_stddev = max([profile.get("stddev", 0) for profile in numeric_profiles] or [1]) or 1
    rows = []
    for profile in numeric_profiles:
        completeness = profile["count"] / sample_size
        variance_signal = min(1, (profile.get("stddev", 0) / max_stddev))
        centrality = min(1, correlation_centrality.get(profile["field"], 0) / 2)
        score = (completeness * 30) + (variance_signal * 32) + (centrality * 26)
        if profile["field"] == temporal_field:
            score += 8
        rows.append(
            {
                "field": profile["field"],
                "label": profile["label"] or profile["field"],
                "kind": "medida",
                "score": round(min(100, score), 2),
                "drivers": [
                    f"completude {round(completeness * 100)}%",
                    f"variacao {round(variance_signal * 100)}%",
                    f"centralidade {round(centrality * 100)}%",
                ],
            }
        )

    for profile in categorical_profiles:
        top_total = sum(value["count"] for value in profile.get("top_values", []))
        counts = {value["value"]: value["count"] for value in profile.get("top_values", [])}
        completeness = profile["count"] / sample_size
        entropy = _entropy_ratio(counts)
        cardinality = min(1, math.log(profile.get("unique", 1) + 1) / math.log(max(sample_size, 2)))
        score = (completeness * 26) + (entropy * 28) + (cardinality * 22)
        if re.search(r"\b(hospital|uls|ars|regiao|concelho|distrito|entidade|unidade)\b", _normalize_token(profile["field"])):
            score += 14
        if profile["field"] == temporal_field:
            score += 10
        rows.append(
            {
                "field": profile["field"],
                "label": profile["label"] or profile["field"],
                "kind": "dimensao",
                "score": round(min(100, score), 2),
                "drivers": [
                    f"completude {round(completeness * 100)}%",
                    f"diversidade {round(entropy * 100)}%",
                    f"categorias {profile.get('unique', 0)}",
                ],
                "dominant_sample": profile.get("top_values", [{}])[0].get("value") if top_total else None,
            }
        )

    if not rows:
        return []
    shadow_baseline = sorted(row["score"] for row in rows)[max(0, len(rows) // 2 - 1)] * 0.82
    for row in rows:
        row["selection"] = "forte" if row["score"] >= max(42, shadow_baseline) else "exploratoria"
    rows.sort(key=lambda item: (-item["score"], item["kind"], item["field"]))
    return rows[:14]


def _build_data_analytics(dataset_id: str, limit: int) -> dict:
    cache_key = f"data_analytics:{dataset_id}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    dataset_path = f"/catalog/datasets/{quote(dataset_id)}"
    dataset = _ods_fetch(dataset_path)
    fields = dataset.get("fields", []) or []
    field_by_name = {field.get("name"): field for field in fields if field.get("name")}
    temporal_field = _pick_temporal_field(fields)
    safe_limit = min(max(limit, 1), MAX_DATA_ANALYTICS_LIMIT)
    params = {"limit": str(safe_limit)}
    ordering = "api_default"
    if temporal_field:
        params["order_by"] = f"{temporal_field} desc"
        ordering = "temporal_desc"
    else:
        stable_field = _pick_stable_order_field(fields)
        if stable_field:
            params["order_by"] = f"{stable_field} asc"
            ordering = "stable_field"

    records_path = f"/catalog/datasets/{quote(dataset_id)}/records"
    try:
        payload = _ods_fetch(records_path, params, cacheable=True)
    except Exception:
        params = {"limit": str(safe_limit)}
        ordering = "api_default"
        payload = _ods_fetch(records_path, params, cacheable=True)

    records = payload.get("results", []) or []
    observed_columns = []
    for field in fields:
        name = field.get("name")
        if name and any(name in record for record in records[:30]):
            observed_columns.append(name)
        if len(observed_columns) >= 32:
            break

    numeric_profiles = []
    numeric_by_column: dict[str, dict[int, float]] = {}
    categorical_profiles = []
    for column in observed_columns:
        field = field_by_name.get(column, {"name": column})
        canonical_type = _canonical_field_type(field)
        raw_values = [record.get(column) for record in records]
        non_empty = [value for value in raw_values if value not in (None, "")]
        numeric_pairs = [(idx, _coerce_number(value)) for idx, value in enumerate(raw_values)]
        numeric_values_by_idx = {idx: value for idx, value in numeric_pairs if value is not None}
        numeric_values = list(numeric_values_by_idx.values())
        numeric_ratio = len(numeric_values) / len(non_empty) if non_empty else 0

        if _is_measure_candidate(field) and len(numeric_values) >= 5 and numeric_ratio >= 0.65:
            avg = _mean(numeric_values)
            numeric_by_column[column] = numeric_values_by_idx
            numeric_profiles.append(
                {
                    "field": column,
                    "label": _safe_label(field),
                    "type": canonical_type if canonical_type in {"integer", "float"} else "float",
                    "count": len(numeric_values),
                    "missing": len(raw_values) - len(numeric_values),
                    "min": round(min(numeric_values), 4),
                    "max": round(max(numeric_values), 4),
                    "avg": round(avg, 4),
                    "stddev": round(_stddev(numeric_values), 4),
                }
            )
            continue

        counts: dict[str, int] = {}
        for value in non_empty:
            key = str(value).strip()
            if not key:
                continue
            counts[key] = counts.get(key, 0) + 1
        if counts:
            top_values = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:8]
            categorical_profiles.append(
                {
                    "field": column,
                    "label": _safe_label(field),
                    "type": canonical_type,
                    "count": len(non_empty),
                    "missing": len(raw_values) - len(non_empty),
                    "unique": len(counts),
                    "top_values": [{"value": value[:80], "count": count} for value, count in top_values],
                }
            )

    numeric_profiles.sort(key=lambda item: (-item["count"], -item["stddev"], item["field"]))
    categorical_profiles.sort(key=lambda item: (-item["count"], item["unique"], item["field"]))

    correlations = []
    numeric_columns = [profile["field"] for profile in numeric_profiles[:12]]
    for idx, left in enumerate(numeric_columns):
        for right in numeric_columns[idx + 1 :]:
            left_values = numeric_by_column.get(left, {})
            right_values = numeric_by_column.get(right, {})
            shared_indexes = sorted(set(left_values) & set(right_values))
            pairs = [(left_values[index], right_values[index]) for index in shared_indexes]
            corr = _pearson(pairs)
            if corr is None or abs(corr) < 0.25:
                continue
            correlations.append(
                {
                    "field_a": left,
                    "field_b": right,
                    "label_a": _safe_label(field_by_name.get(left, {"name": left})),
                    "label_b": _safe_label(field_by_name.get(right, {"name": right})),
                    "correlation": round(corr, 4),
                    "abs_correlation": round(abs(corr), 4),
                    "samples": len(pairs),
                }
            )
    correlations.sort(key=lambda item: (-item["abs_correlation"], -item["samples"], item["field_a"], item["field_b"]))

    trends = []
    if temporal_field:
        for profile in numeric_profiles[:4]:
            buckets: dict[str, dict[str, object]] = {}
            values_by_idx = numeric_by_column.get(profile["field"], {})
            for idx, record in enumerate(records):
                value = values_by_idx.get(idx)
                if value is None:
                    continue
                period = _extract_period_key(record.get(temporal_field), temporal_field)
                if period is None:
                    continue
                key, label = period
                bucket = buckets.setdefault(key, {"label": label, "values": []})
                bucket["values"].append(value)
            points = [
                {"period": bucket["label"], "avg": round(_mean(bucket["values"]), 4), "count": len(bucket["values"])}
                for _, bucket in sorted(buckets.items())
                if bucket["values"]
            ]
            if len(points) >= 2:
                trends.append({"field": profile["field"], "label": profile["label"], "points": points[-24:]})

    insights = []
    if correlations:
        top = correlations[0]
        insights.append(
            {
                "label": "Correlação mais forte",
                "value": f"{top['correlation']:.2f}",
                "detail": f"{top['label_a']} / {top['label_b']} · {top['samples']} pares",
            }
        )
    if numeric_profiles:
        widest = max(numeric_profiles, key=lambda item: item["stddev"])
        insights.append({"label": "Maior variação", "value": widest["label"], "detail": f"desvio {widest['stddev']}"})
    if categorical_profiles:
        dominant = max(categorical_profiles, key=lambda item: item["top_values"][0]["count"] if item["top_values"] else 0)
        top_value = dominant["top_values"][0] if dominant["top_values"] else {"value": "-", "count": 0}
        insights.append({"label": "Categoria dominante", "value": dominant["label"], "detail": f"{top_value['value']} · {top_value['count']} registos"})

    feature_importance = _build_feature_importance(numeric_profiles, categorical_profiles, correlations, len(records), temporal_field)
    pca_summary = _build_pca_summary(numeric_profiles, numeric_by_column, len(records))
    if feature_importance:
        top_feature = feature_importance[0]
        insights.append(
            {
                "label": "Dado-chave",
                "value": top_feature["label"],
                "detail": f"{top_feature['kind']} · score {top_feature['score']}",
            }
        )

    meta = _cache_meta_public(_cache_key(records_path, params))
    result = {
        "dataset_id": dataset_id,
        "title": _get_dataset_title(dataset.get("metas", {}) or {}) or dataset_id,
        "sample_size": len(records),
        "requested_limit": safe_limit,
        "temporal_field": temporal_field,
        "ordering": ordering,
        "schema_quality": _schema_quality(fields, observed_columns),
        **meta,
        "numeric_profiles": numeric_profiles[:12],
        "categorical_profiles": categorical_profiles[:10],
        "correlations": correlations[:20],
        "trends": trends[:4],
        "feature_importance": feature_importance,
        "pca_summary": pca_summary,
        "insights": insights[:4],
        "generated_at": int(_now()),
    }
    _cache_set(cache_key, result)
    return result


def _pick_finprod_trend(trends: list[dict], *, mode: str) -> dict | None:
    if not trends:
        return None
    keywords = {
        "financial": r"\b(custo|despesa|encargo|pagamento|contrato|valor|orcamento|orçamento)\b",
        "production": r"\b(producao|produção|consulta|urgencia|urgência|cirurgia|episodio|episódio|atendimento|atividade)\b",
    }.get(mode, r"$^")
    for trend in trends:
        label = _normalize_token(str(trend.get("label") or trend.get("field") or ""))
        if re.search(keywords, label):
            return trend
    return trends[0]


def _finprod_period_key(label: str) -> str | None:
    value = str(label or "").strip()
    if not value:
        return None
    match = re.search(r"(20\d{2}|19\d{2})\s*[-/ ]?\s*(q([1-4])|t([1-4])|trimestre\s*([1-4]))", value, re.IGNORECASE)
    if match:
        quarter = match.group(3) or match.group(4) or match.group(5)
        return f"{match.group(1)}-Q{quarter}"
    match = re.search(r"(20\d{2}|19\d{2})\s*[-/ ]\s*(0?[1-9]|1[0-2])", value)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}"
    match = re.search(r"(20\d{2}|19\d{2})", value)
    if match:
        return match.group(1)
    normalized = _normalize_token(value)
    return normalized or None


def _normalize_measure_scale(values: list[float], *, mode: str) -> tuple[float, str]:
    if not values:
        return 1.0, "valor"
    sorted_values = sorted(abs(value) for value in values if value is not None)
    if not sorted_values:
        return 1.0, "valor"
    median = sorted_values[len(sorted_values) // 2]
    if mode == "financial":
        if median >= 1_000_000:
            return 1_000_000.0, "M€"
        if median >= 1_000:
            return 1_000.0, "mil €"
        return 1.0, "€"
    if median >= 1_000_000:
        return 1_000_000.0, "milhões"
    if median >= 1_000:
        return 1_000.0, "mil"
    return 1.0, "unidade"


def _spearman(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 8:
        return None
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    rank_x = {value: index + 1 for index, value in enumerate(sorted(set(xs)))}
    rank_y = {value: index + 1 for index, value in enumerate(sorted(set(ys)))}
    ranked = [(rank_x[x], rank_y[y]) for x, y in pairs]
    return _pearson([(float(rx), float(ry)) for rx, ry in ranked])


def _robustness_band(pairs: list[tuple[float, float]], pearson: float | None, spearman: float | None) -> str:
    sample = len(pairs)
    if sample < 8 or pearson is None:
        return "insuficiente"
    coherence = abs((spearman or 0) - pearson)
    if sample >= 16 and abs(pearson) >= 0.55 and coherence <= 0.25:
        return "alta"
    if sample >= 10 and abs(pearson) >= 0.35:
        return "moderada"
    return "baixa"


def _detect_unit_cost_outliers(rows: list[dict]) -> list[dict]:
    values = [row["unit_cost"] for row in rows if isinstance(row.get("unit_cost"), (int, float))]
    if len(values) < 5:
        return []
    sorted_values = sorted(values)
    median = sorted_values[len(sorted_values) // 2]
    deviations = [abs(value - median) for value in values]
    mad = sorted(deviations)[len(deviations) // 2] or 1e-9
    outliers = []
    for row in rows:
        value = row.get("unit_cost")
        if not isinstance(value, (int, float)):
            continue
        robust_z = abs(value - median) / (1.4826 * mad)
        if robust_z >= 3:
            outliers.append(
                {
                    "period": row.get("period"),
                    "unit_cost": round(value, 4),
                    "robust_z": round(robust_z, 2),
                }
            )
    outliers.sort(key=lambda item: -item["robust_z"])
    return outliers[:5]


def _entity_benchmark(financial: dict, production: dict) -> list[dict]:
    pattern = re.compile(r"\b(uls|hospital|entidade|unidade|ars|regiao|região|concelho|distrito)\b")
    fin_fields = [row for row in financial.get("categorical_profiles", []) if pattern.search(_normalize_token(row.get("field", "")))]
    prod_fields = [row for row in production.get("categorical_profiles", []) if pattern.search(_normalize_token(row.get("field", "")))]
    if not fin_fields or not prod_fields:
        return []
    fin_top = fin_fields[0]
    prod_top = prod_fields[0]
    fin_count = max(fin_top.get("count", 1), 1)
    prod_count = max(prod_top.get("count", 1), 1)
    fin_share = {str(item.get("value")): (item.get("count", 0) / fin_count) for item in fin_top.get("top_values", [])}
    prod_share = {str(item.get("value")): (item.get("count", 0) / prod_count) for item in prod_top.get("top_values", [])}
    shared = sorted(set(fin_share) & set(prod_share))
    rows = []
    for key in shared:
        rows.append(
            {
                "entity": key,
                "financial_share": round(fin_share[key], 4),
                "production_share": round(prod_share[key], 4),
                "balance_gap": round(fin_share[key] - prod_share[key], 4),
            }
        )
    rows.sort(key=lambda item: -abs(item["balance_gap"]))
    return rows[:10]


def _build_finance_production(financial_dataset: str, production_dataset: str, limit: int) -> dict:
    cache_key = f"finprod:{financial_dataset}:{production_dataset}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    financial = _build_data_analytics(financial_dataset, limit)
    production = _build_data_analytics(production_dataset, limit)
    financial_trend = _pick_finprod_trend(financial.get("trends", []), mode="financial")
    production_trend = _pick_finprod_trend(production.get("trends", []), mode="production")

    financial_points = {
        _finprod_period_key(point.get("period")): float(point.get("avg"))
        for point in (financial_trend or {}).get("points", [])
        if _finprod_period_key(point.get("period")) is not None and _coerce_number(point.get("avg")) is not None
    }
    production_points = {
        _finprod_period_key(point.get("period")): float(point.get("avg"))
        for point in (production_trend or {}).get("points", [])
        if _finprod_period_key(point.get("period")) is not None and _coerce_number(point.get("avg")) is not None
    }
    shared_periods = sorted(set(financial_points) & set(production_points))
    rows = []
    pairs = []
    for period in shared_periods:
        expense_value = financial_points[period]
        output_value = production_points[period]
        unit_cost = expense_value / output_value if output_value > 0 else None
        rows.append(
            {
                "period": period,
                "financial_value": round(expense_value, 4),
                "production_value": round(output_value, 4),
                "unit_cost": round(unit_cost, 4) if unit_cost is not None else None,
            }
        )
        if output_value > 0:
            pairs.append((expense_value, output_value))

    expense_scale, expense_unit = _normalize_measure_scale([row["financial_value"] for row in rows], mode="financial")
    production_scale, production_unit = _normalize_measure_scale([row["production_value"] for row in rows], mode="production")
    for row in rows:
        row["financial_normalized"] = round(row["financial_value"] / expense_scale, 4) if expense_scale else row["financial_value"]
        row["production_normalized"] = round(row["production_value"] / production_scale, 4) if production_scale else row["production_value"]

    unit_cost_values = [row["unit_cost"] for row in rows if isinstance(row.get("unit_cost"), (int, float))]
    unit_cost_avg = round(_mean(unit_cost_values), 4) if unit_cost_values else None
    unit_cost_median = round(sorted(unit_cost_values)[len(unit_cost_values) // 2], 4) if unit_cost_values else None
    correlation = _pearson(pairs)
    spearman = _spearman(pairs)
    correlation_label = "insuficiente"
    if correlation is not None:
        if abs(correlation) >= 0.7:
            correlation_label = "forte"
        elif abs(correlation) >= 0.4:
            correlation_label = "moderada"
        else:
            correlation_label = "fraca"
    outliers = _detect_unit_cost_outliers(rows)
    benchmark_rows = _entity_benchmark(financial, production)
    robustness = _robustness_band(pairs, correlation, spearman)
    comparability_checks = [
        {
            "label": "Eixo temporal nos dois datasets",
            "status": "ok" if financial.get("temporal_field") and production.get("temporal_field") else "warning",
            "detail": f"{financial.get('temporal_field') or '-'} / {production.get('temporal_field') or '-'}",
        },
        {
            "label": "Períodos em comum",
            "status": "ok" if len(rows) >= 6 else ("warning" if len(rows) >= 3 else "error"),
            "detail": f"{len(rows)} períodos",
        },
        {
            "label": "Produção com valores válidos",
            "status": "ok" if len(pairs) >= 6 else ("warning" if pairs else "error"),
            "detail": f"{len(pairs)} períodos com produção > 0",
        },
        {
            "label": "Benchmark territorial/entidade",
            "status": "ok" if benchmark_rows else "warning",
            "detail": "Disponível" if benchmark_rows else "Sem interseção de entidades nas top categorias",
        },
    ]

    result = {
        "financial_dataset": {
            "dataset_id": financial.get("dataset_id"),
            "title": financial.get("title"),
            "temporal_field": financial.get("temporal_field"),
            "trend_label": (financial_trend or {}).get("label"),
        },
        "production_dataset": {
            "dataset_id": production.get("dataset_id"),
            "title": production.get("title"),
            "temporal_field": production.get("temporal_field"),
            "trend_label": (production_trend or {}).get("label"),
        },
        "summary": {
            "matched_periods": len(rows),
            "avg_unit_cost": unit_cost_avg,
            "median_unit_cost": unit_cost_median,
            "expense_output_correlation": round(correlation, 4) if correlation is not None else None,
            "spearman_correlation": round(spearman, 4) if spearman is not None else None,
            "correlation_strength": correlation_label,
            "robustness": robustness,
            "sample_pairs": len(pairs),
        },
        "comparability": {"checks": comparability_checks},
        "normalization": {
            "financial": {"scale": expense_scale, "unit_label": expense_unit},
            "production": {"scale": production_scale, "unit_label": production_unit},
        },
        "rows": rows[-24:],
        "outliers": outliers,
        "entity_benchmark": benchmark_rows,
        "diagnostics": {
            "financial_trends": len(financial.get("trends", [])),
            "production_trends": len(production.get("trends", [])),
            "financial_samples": financial.get("sample_size", 0),
            "production_samples": production.get("sample_size", 0),
            "warnings": [
                warning
                for warning in [
                    "Sem períodos em comum entre tendências selecionadas." if not rows else None,
                    "Correlação com baixa robustez estatística." if robustness in {"baixa", "insuficiente"} else None,
                    "Outliers detetados no custo unitário." if outliers else None,
                ]
                if warning
            ],
        },
        "generated_at": int(_now()),
    }
    _cache_set(cache_key, result)
    return result


class TransparenciaHandler(SimpleHTTPRequestHandler):
    server_version = "SaudeMaisPublica/1.0"
    sys_version = ""

    extensions_map = SimpleHTTPRequestHandler.extensions_map.copy()
    extensions_map.update({
        ".html": "text/html; charset=utf-8",
        ".js": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8",
    })

    def _set_security_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=(), usb=(), payment=()")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; connect-src 'self'; img-src * data: blob:; script-src 'self' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'none'",
        )
        origin = _normalize_origin(self.headers.get("Origin"))
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")

    def end_headers(self):
        if not self.path.startswith("/api/") and (self.path == "/" or self.path.endswith(".html")):
            self.send_header("Cache-Control", "no-store")
        self._set_security_headers()
        super().end_headers()

    def do_OPTIONS(self):
        if self._rate_limited():
            return
        self.send_response(200)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Accept")
        origin = _normalize_origin(self.headers.get("Origin"))
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.end_headers()

    def _json(self, status: int, payload: dict, cache_control: str = "no-store, no-cache, must-revalidate, max-age=0"):
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Cache-Control", cache_control)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str):
        self._json(status, {"error": message})

    def _send_exception_json(self, exc: Exception, *, status: int = 502):
        if isinstance(exc, ValueError):
            self._send_error_json(400, str(exc))
            return
        self._send_error_json(status, "Upstream data service unavailable")

    def _rate_limited(self) -> bool:
        retry_after = _rate_limit_retry_after(self.client_address[0])
        if retry_after is None:
            return False
        self.send_response(429)
        self.send_header("Retry-After", str(retry_after))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(b'{"error":"Too many requests"}')
        return True

    def do_GET(self):
        if self._rate_limited():
            return

        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path.startswith("/api/"):
            if path == "/api/health":
                self._json(200, {"status": "ok"}, cache_control="no-store")
                return

            if path == "/api/analytics":
                try:
                    min_score = _parse_int_param((query.get("min_score") or [None])[0], 4, DEFAULT_MIN_SCORE, MAX_MIN_SCORE, "min_score")
                    payload = _build_analytics(_get_analysis_catalog(), min_score)
                    self._json(200, payload, cache_control="public, max-age=45")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path == "/api/data-analytics":
                try:
                    dataset_id = _parse_dataset_id((query.get("dataset_id") or [None])[0])
                    limit = _parse_int_param(
                        (query.get("limit") or [None])[0],
                        DEFAULT_DATA_ANALYTICS_LIMIT,
                        20,
                        MAX_DATA_ANALYTICS_LIMIT,
                        "limit",
                    )
                    self._json(200, _build_data_analytics(dataset_id, limit), cache_control="no-store")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path == "/api/finprod":
                try:
                    financial_dataset = _parse_dataset_id((query.get("financial_dataset") or [None])[0])
                    production_dataset = _parse_dataset_id((query.get("production_dataset") or [None])[0])
                    limit = _parse_int_param(
                        (query.get("limit") or [None])[0],
                        DEFAULT_DATA_ANALYTICS_LIMIT,
                        20,
                        MAX_DATA_ANALYTICS_LIMIT,
                        "limit",
                    )
                    payload = _build_finance_production(financial_dataset, production_dataset, limit)
                    self._json(200, payload, cache_control="no-store")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path == "/api/datasets":
                try:
                    limit = _parse_int_param((query.get("limit") or [None])[0], MAX_DATASET_LIMIT, 1, MAX_DATASET_LIMIT, "limit")
                    ods_params = {"limit": str(limit)}
                    payload = dict(_ods_fetch("/catalog/datasets", ods_params))
                    payload.update(_cache_meta_public(_cache_key("/catalog/datasets", ods_params)))
                    self._json(200, payload, cache_control="public, max-age=60")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path == "/api/analysis":
                try:
                    min_score = _parse_int_param((query.get("min_score") or [None])[0], DEFAULT_MIN_SCORE, DEFAULT_MIN_SCORE, MAX_MIN_SCORE, "min_score")
                    cached = _get_analysis_catalog()
                    filtered = {
                        "datasets": cached["datasets"],
                        "opportunities": cached["opportunities"],
                        "themes": cached.get("themes", []),
                        "generated_at": cached["generated_at"],
                    }
                    if min_score > DEFAULT_MIN_SCORE:
                        filtered["links"] = [l for l in cached["links"] if l["score"] >= min_score]
                    else:
                        filtered["links"] = cached["links"]
                    filtered["total"] = len(filtered["datasets"])
                    filtered["link_count"] = len(filtered["links"])
                    self._json(200, filtered, cache_control="public, max-age=45")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path.startswith("/api/dataset/"):
                try:
                    dataset_id = _parse_dataset_id(path.split("/api/dataset/", 1)[1])
                    dataset_path = f"/catalog/datasets/{quote(dataset_id)}"
                    dataset = dict(_ods_fetch(dataset_path))
                    dataset.update(_cache_meta_public(_cache_key(dataset_path)))
                    dataset["schema_quality"] = _schema_quality(dataset.get("fields", []) or [])
                    self._json(200, dataset, cache_control="public, max-age=300")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path.startswith("/api/records/"):
                try:
                    dataset_id = _parse_dataset_id(path.split("/api/records/", 1)[1])
                    limit = _parse_int_param((query.get("limit") or [None])[0], 50, 1, MAX_RECENT_LIMIT, "limit")
                    records_path = f"/catalog/datasets/{quote(dataset_id)}/records"
                    records_params = {"limit": str(limit)}
                    records = _ods_fetch(
                        records_path,
                        records_params,
                        cacheable=True,
                    )
                    records = dict(records)
                    records.update(_cache_meta_public(_cache_key(records_path, records_params)))
                    self._json(200, records, cache_control="no-store")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path.startswith("/api/recent/"):
                try:
                    dataset_id = _parse_dataset_id(path.split("/api/recent/", 1)[1])
                    limit = _parse_int_param((query.get("limit") or [None])[0], DEFAULT_RECENT_LIMIT, 1, MAX_RECENT_LIMIT, "limit")
                    self._json(200, _recent_records(dataset_id, limit), cache_control="no-store")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            self._send_error_json(404, f"Unknown API route: {path}")
            return

        if path == "/":
            path = "/index.html"

        clean_path = "/" + path.lstrip("/")
        if clean_path in ALLOWED_STATIC_PATHS:
            self.path = clean_path.lstrip("/")
            LOGGER.info("Serving static: %s -> %s", clean_path, self.path)
            return super().do_GET()

        LOGGER.warning("404 Static not found: %s", path)
        self._send_error_json(404, f"Static asset not found: {path}")
        return

    def list_directory(self, path):
        self.send_error(404, "Directory listing disabled")
        return None


def run() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8000), TransparenciaHandler)
    print("http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
