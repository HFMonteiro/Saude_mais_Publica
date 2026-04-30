#!/usr/bin/env python3
"""
Transparent local explorer for the Opendatasoft Explore API used by
transparencia.sns.gov.pt.

This server serves a static frontend and exposes API helper endpoints with a local
proxy so the browser does not hit CORS issues.
"""

from __future__ import annotations

import copy
import hashlib
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
ANALYTICS_METHOD_VERSION = "2026-04-29.validation-v1"
DEFAULT_MIN_SCORE = 1
MAX_MIN_SCORE = 10
DEFAULT_RECENT_LIMIT = 60
RATE_LIMIT_WINDOW_SECONDS = 60
RATE_LIMIT_MAX_REQUESTS = 180
EXPENSIVE_RATE_LIMIT_WINDOW_SECONDS = 60
EXPENSIVE_RATE_LIMIT_MAX_REQUESTS = 36
MAX_CONCURRENT_EXPENSIVE_REQUESTS = 3
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
            "csp",
            "episodios",
            "faltas",
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
        "description": "Despesa, dívida, pagamentos, compras, medicamentos e agregados económico-financeiros.",
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
            "diagnostico",
            "diagnóstico",
            "dpoc",
            "fraturas",
            "gra",
            "hipertensao",
            "icpc",
            "idra",
            "mortalidade",
            "morbilidade",
            "multimorbilidade",
            "polimedicacao",
            "polimedicação",
            "problema",
            "problemas",
            "qualidade",
            "satisfacao",
            "utente",
            "utentes",
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
            "pic",
            "plano individual",
            "rastreios",
            "sangue",
            "sns24",
        ],
    },
    "Rede & Território": {
        "description": "Entidades, regiões, localização, unidades funcionais, RNCCI e organização da rede.",
        "terms": [
            "ars",
            "aces",
            "entidade",
            "geografica",
            "hospital",
            "hospitais",
            "instituicao",
            "localizacao",
            "rede",
            "regiao",
            "rncci",
            "ucsp",
            "uls",
            "unidade funcional",
            "unidades",
            "usf",
        ],
    },
}

DEFAULT_THEME = "Outros"
DATASET_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,120}$")


class UpstreamAPIError(RuntimeError):
    """External ODS API failed after retries or could not be reached."""

    kind = "upstream_unavailable"

    def __init__(self, message: str, *, status_code: int | None = None, path: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.path = path


class UpstreamContractError(UpstreamAPIError):
    """The request shape we sent to ODS was rejected; this is usually our contract bug."""

    kind = "upstream_contract_error"

FACET_DEFINITIONS = {
    "uls": {
        "label": "ULS",
        "pattern": r"\b(uls|unidade local de saude|unidade local de saúde)\b",
        "scope": "uls",
        "institution": "uls",
        "dimension": "territorial",
    },
    "regiao": {
        "label": "Região/ARS",
        "pattern": r"\b(regiao|região|ars|administracao regional|administração regional|norte|centro|lisboa|tejo|alentejo|algarve|acores|açores|madeira)\b",
        "scope": "regiao",
        "institution": "",
        "dimension": "territorial",
    },
    "hospital": {
        "label": "Hospital",
        "pattern": r"\b(hospital|hospitais|centro hospitalar|chuc|chul|chusj|ipo)\b",
        "scope": "hospital",
        "institution": "hospital",
        "dimension": "territorial",
    },
    "uf": {
        "label": "UF/CSP",
        "pattern": r"\b(uf|unidade funcional|unidades funcionais|usf|ucsp|aces|csp|cuidados de saude primarios|cuidados de saúde primários|centro de saude|centro de saúde)\b",
        "scope": "uf",
        "institution": "uf",
        "dimension": "entidade",
    },
    "concelho": {
        "label": "Concelho/Distrito",
        "pattern": r"\b(concelho|distrito|municipio|município|freguesia|localizacao|localização|territorio|território|geograf)\b",
        "scope": "concelho",
        "institution": "",
        "dimension": "territorial",
    },
    "instituicao": {
        "label": "Instituição/Entidade",
        "pattern": r"\b(instituicao|instituição|entidade|unidade|servico|serviço|rncci|fornecedor)\b",
        "scope": "entidade",
        "institution": "entidade",
        "dimension": "entidade",
    },
    "temporal": {
        "label": "Temporal",
        "pattern": r"\b(tempo|periodo|período|data|ano|mes|mês|trimestre|semana|dia)\b",
        "scope": "",
        "institution": "",
        "dimension": "temporal",
    },
    "medida": {
        "label": "Medida",
        "pattern": r"\b(valor|total|taxa|numero|número|n_|count|volume|quantidade|dias|percentagem|indicador)\b",
        "scope": "",
        "institution": "",
        "dimension": "medida",
    },
    "financeiro": {
        "label": "Financeiro",
        "pattern": r"\b(despesa|divida|dívida|pagamento|custo|encargo|financeiro|receita|orcamento|orçamento|pvp|preco|preço)\b",
        "scope": "",
        "institution": "",
        "dimension": "medida",
    },
    "producao": {
        "label": "Produção",
        "pattern": r"\b(producao|produção|consulta|consultas|urgencia|urgência|cirurgia|episodio|episódio|atividade|actividade)\b",
        "scope": "",
        "institution": "",
        "dimension": "medida",
    },
    "saude_publica": {
        "label": "Saúde Pública",
        "pattern": r"\b(saude publica|saúde pública|mortalidade|morbilidade|rastreio|vigilancia|vigilância|epidem|vacina|gripe|covid|emergencia|emergência)\b",
        "scope": "",
        "institution": "",
        "dimension": "",
    },
    "icpc": {
        "label": "ICPC/Problemas",
        "pattern": r"\b(icpc|problema|problemas|diagnostico|diagnóstico|diagnosticos|diagnósticos|episodio clinico|episódio clínico|utente|utentes)\b",
        "scope": "",
        "institution": "",
        "dimension": "clinico",
    },
    "fragilidade": {
        "label": "Fragilidade",
        "pattern": r"\b(multimorbilidade|polimedicacao|polimedicação|fragilidade|idra|gra|risco clinico|risco clínico|plano individual de cuidados|pic)\b",
        "scope": "",
        "institution": "",
        "dimension": "clinico",
    },
}
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
    "/research.html",
    "/research.js",
    "/visual_assets/analytics_hero.png",
    "/visual_assets/empty_state.png",

    "/visual_assets/logo.jpg",
    "/visual_assets/background.jpg",
}
EXPENSIVE_API_PATHS = {
    "/api/data-analytics",
    "/api/finprod",
    "/api/finprod/recommendations",
    "/api/predictive/recommendations",
    "/api/deep-research",
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
_expensive_rate_limit_lock = threading.Lock()
_expensive_rate_limit_hits: dict[str, list[float]] = {}
_expensive_request_slots = threading.BoundedSemaphore(MAX_CONCURRENT_EXPENSIVE_REQUESTS)


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


def _route_rate_limit_retry_after(client_id: str, route: str) -> int | None:
    now = _now()
    cutoff = now - EXPENSIVE_RATE_LIMIT_WINDOW_SECONDS
    key = f"{client_id}:{route}"
    with _expensive_rate_limit_lock:
        hits = [timestamp for timestamp in _expensive_rate_limit_hits.get(key, []) if timestamp >= cutoff]
        if len(hits) >= EXPENSIVE_RATE_LIMIT_MAX_REQUESTS:
            _expensive_rate_limit_hits[key] = hits
            return max(1, int(EXPENSIVE_RATE_LIMIT_WINDOW_SECONDS - (now - hits[0])))
        hits.append(now)
        _expensive_rate_limit_hits[key] = hits
        if len(_expensive_rate_limit_hits) > 512:
            for stale_key in list(_expensive_rate_limit_hits):
                _expensive_rate_limit_hits[stale_key] = [
                    timestamp for timestamp in _expensive_rate_limit_hits[stale_key] if timestamp >= cutoff
                ]
                if not _expensive_rate_limit_hits[stale_key]:
                    del _expensive_rate_limit_hits[stale_key]
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


def _facet_tags_from_text(text: str) -> list[str]:
    normalized = _normalize_token(text)
    tags = [
        key
        for key, config in FACET_DEFINITIONS.items()
        if re.search(config["pattern"], normalized, flags=re.IGNORECASE)
    ]
    return sorted(set(tags))


def _classify_facets(dataset_id: str, title: str, fields: list[str]) -> dict:
    """Classify catalogue metadata only; this deliberately avoids scanning records."""
    field_facets = {}
    tags = set(_facet_tags_from_text(" ".join([dataset_id, title])))
    for field in fields:
        field_tags = _facet_tags_from_text(field)
        if field_tags:
            field_facets[field] = field_tags
            tags.update(field_tags)

    local_scopes = sorted({FACET_DEFINITIONS[tag]["scope"] for tag in tags if FACET_DEFINITIONS[tag].get("scope")})
    institution_types = sorted({FACET_DEFINITIONS[tag]["institution"] for tag in tags if FACET_DEFINITIONS[tag].get("institution")})
    dimension_types = sorted({FACET_DEFINITIONS[tag]["dimension"] for tag in tags if FACET_DEFINITIONS[tag].get("dimension")})
    quality_flags = []
    if not ({"temporal"} & tags):
        quality_flags.append("sem_dimensao_temporal_explicita")
    if len(set(local_scopes) & {"uls", "uf", "regiao", "hospital", "concelho", "entidade"}) > 1:
        quality_flags.append("mistura_granularidade_local")
    if "medida" not in tags and "financeiro" not in tags and "producao" not in tags:
        quality_flags.append("sem_medida_obvia")

    return {
        "tags": sorted(tags),
        "labels": [FACET_DEFINITIONS[tag]["label"] for tag in sorted(tags)],
        "local_scopes": local_scopes,
        "institution_types": institution_types,
        "dimension_types": dimension_types,
        "field_facets": field_facets,
        "inferred": True,
        "quality_flags": quality_flags,
    }


def _readiness_band(score: int) -> str:
    if score >= 75:
        return "pronto"
    if score >= 52:
        return "rever"
    return "fragil"


def _catalog_readiness(facets: dict, field_count: int, metric_count: int, link_count: int = 0) -> dict:
    flags = facets.get("quality_flags", []) or []
    dimensions = set(facets.get("dimension_types", []) or [])
    score = 32
    if "temporal" in dimensions:
        score += 16
    if "medida" in dimensions or metric_count:
        score += 18
    if {"territorial", "entidade"} & dimensions:
        score += 10
    score += min(10, field_count)
    score += min(8, metric_count * 2)
    score += min(6, link_count)
    score -= len(flags) * 9
    score = max(0, min(100, round(score)))
    gaps = []
    if "sem_dimensao_temporal_explicita" in flags:
        gaps.append("tempo")
    if "sem_medida_obvia" in flags:
        gaps.append("medida")
    if "mistura_granularidade_local" in flags:
        gaps.append("granularidade")
    if not gaps:
        gaps.append("validar fonte")
    return {
        "score": score,
        "band": _readiness_band(score),
        "label": {"pronto": "Pronto", "rever": "Rever", "fragil": "Frágil"}[_readiness_band(score)],
        "gaps": gaps[:3],
    }


def _analysis_readiness(
    *,
    sample_size: int,
    total_records: int,
    temporal_field: str | None,
    numeric_count: int,
    categorical_count: int,
    correlation_count: int,
    trend_count: int,
    warning_count: int,
    max_missing_ratio: float = 0.0,
) -> dict:
    coverage_ratio = (sample_size / total_records) if total_records else None
    score = 26
    if sample_size >= 80:
        score += 14
    elif sample_size >= 40:
        score += 9
    elif sample_size >= 12:
        score += 4
    if coverage_ratio is not None:
        if coverage_ratio >= 0.8:
            score += 16
        elif coverage_ratio >= 0.2:
            score += 9
        elif coverage_ratio >= 0.05:
            score += 4
    if temporal_field:
        score += 12
    score += min(12, numeric_count * 3)
    score += min(8, categorical_count * 2)
    score += min(10, correlation_count * 2)
    score += min(8, trend_count * 4)
    score -= warning_count * 8
    if max_missing_ratio >= 0.5:
        score -= 18
    elif max_missing_ratio >= 0.25:
        score -= 8
    hard_cap = 100
    if coverage_ratio is not None and coverage_ratio < 0.05:
        hard_cap = min(hard_cap, 45)
    elif coverage_ratio is not None and coverage_ratio < 0.2:
        hard_cap = min(hard_cap, 64)
    if not temporal_field:
        hard_cap = min(hard_cap, 68)
    if warning_count:
        hard_cap = min(hard_cap, 72)
    if max_missing_ratio >= 0.25:
        hard_cap = min(hard_cap, 70)
    score = min(score, hard_cap)
    score = max(0, min(100, round(score)))
    gaps = []
    if coverage_ratio is not None and coverage_ratio < 0.2:
        gaps.append("cobertura")
    if not temporal_field:
        gaps.append("tempo")
    if numeric_count < 2:
        gaps.append("medidas")
    if warning_count:
        gaps.append("avisos")
    if max_missing_ratio >= 0.25:
        gaps.append("missing")
    if not gaps:
        gaps.append("denominador")
    return {
        "score": score,
        "band": _readiness_band(score),
        "label": {"pronto": "Pronto", "rever": "Rever", "fragil": "Frágil"}[_readiness_band(score)],
        "gaps": gaps[:3],
    }


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


def _field_search_text(field: dict) -> str:
    parts = []
    for key in ("name", "label", "description"):
        value = field.get(key)
        if value is not None:
            parts.append(str(value))
    return _normalize_token(" ".join(parts))


def _canonical_field_type(field: dict) -> str:
    raw_type = _normalize_token(str(field.get("type") or field.get("annotations", {}).get("type") or ""))
    name = _field_search_text(field)
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
    name = _field_search_text(field)
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
    if canonical_type in {"integer", "float"}:
        return True
    text = _field_search_text(field)
    return bool(
        re.search(r"\b(valor|total|taxa|numero|n_|quantidade|dias|encargo|custo|pvp|percent|percentagem|media|ratio|racio|indice|score|utentes)\b", text)
        and not re.search(r"\b(codigo|cod|id|grupo|tipo|categoria|sexo|idade|regiao|entidade|hospital|uls|concelho|distrito)\b", text)
    )


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
            message = f"ODS HTTP {last_error.code} on {path}"
            if last_error.code in {400, 404, 422}:
                raise UpstreamContractError(message, status_code=last_error.code, path=path) from last_error
            raise UpstreamAPIError(message, status_code=last_error.code, path=path) from last_error
        if isinstance(last_error, URLError):
            raise UpstreamAPIError("Cannot reach ODS API", path=path) from last_error
        raise UpstreamAPIError("Cannot reach ODS API", path=path)


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
        title = (entry.get("title") or entry.get("title_pt") or _get_dataset_title(metas)).strip() or dataset_id
        records_count = ((metas.get("default") or {}).get("records_count") or entry.get("records_count") or 0)

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
        facets = _classify_facets(dataset_id, title, field_names)
        semantic_profile = _dataset_semantic_profile(
            entry.get("fields", []) or [],
            dataset_id=dataset_id,
            dataset_title=title,
            mega_theme=mega_theme,
        )
        semantic_metric_count = sum(
            count
            for role, count in (semantic_profile.get("role_counts") or {}).items()
            if role not in {"desconhecido"}
        )
        metric_candidate_count = max(metric_candidate_count, semantic_metric_count)
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
                "facets": facets,
                "semantic_profile": semantic_profile,
                "finprod_role": semantic_profile["finprod_role"],
                "quality_flags": facets["quality_flags"],
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
    degree_by_id: dict[str, int] = {}
    for link in links:
        degree_by_id[link["source"]] = degree_by_id.get(link["source"], 0) + 1
        degree_by_id[link["target"]] = degree_by_id.get(link["target"], 0) + 1
    for item in items:
        item["analysis_readiness"] = _catalog_readiness(
            item.get("facets") or {},
            item.get("field_count", 0),
            item.get("metric_candidate_count", 0),
            degree_by_id.get(item["dataset_id"], 0),
        )
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
    facet_counts: dict[str, int] = {}
    local_scope_counts: dict[str, int] = {}
    institution_counts: dict[str, int] = {}
    dimension_counts: dict[str, int] = {}
    quality_flag_counts: dict[str, int] = {}
    for item in items:
        facets = item.get("facets") or {}
        for tag in facets.get("tags", []) or []:
            facet_counts[tag] = facet_counts.get(tag, 0) + 1
        for scope in facets.get("local_scopes", []) or []:
            local_scope_counts[scope] = local_scope_counts.get(scope, 0) + 1
        for institution in facets.get("institution_types", []) or []:
            institution_counts[institution] = institution_counts.get(institution, 0) + 1
        for dimension in facets.get("dimension_types", []) or []:
            dimension_counts[dimension] = dimension_counts.get(dimension, 0) + 1
        for flag in item.get("quality_flags", []) or []:
            quality_flag_counts[flag] = quality_flag_counts.get(flag, 0) + 1

    return {
        "datasets": items,
        "links": links,
        "opportunities": opportunities[:150],
        "themes": theme_rankings,
        "facet_counts": {
            "tags": dict(sorted(facet_counts.items())),
            "local_scopes": dict(sorted(local_scope_counts.items())),
            "institution_types": dict(sorted(institution_counts.items())),
            "dimension_types": dict(sorted(dimension_counts.items())),
            "quality_flags": dict(sorted(quality_flag_counts.items())),
        },
        "total": len(items),
        "generated_at": int(_now()),
    }


def _fallback_analysis_catalog(min_score: int = DEFAULT_MIN_SCORE, error: Exception | None = None) -> dict:
    if isinstance(error, UpstreamContractError):
        warning = "API SNS rejeitou a configuração do pedido; rever parâmetros do proxy antes de analisar."
    elif isinstance(error, UpstreamAPIError):
        warning = "API SNS indisponível ou sem resposta; não foram carregados datasets."
    else:
        warning = "Falha ao construir catálogo; rever logs do servidor."
    error_kind = getattr(error, "kind", type(error).__name__ if error else None)
    upstream_status = getattr(error, "status_code", None)
    upstream_path = getattr(error, "path", None)
    return {
        "datasets": [],
        "links": [],
        "opportunities": [],
        "themes": [],
        "facet_counts": {"tags": {}, "local_scopes": {}, "institution_types": {}, "dimension_types": {}, "quality_flags": {}},
        "total": 0,
        "link_count": 0,
        "fallback": True,
        "warning": warning,
        "empty_reason": "api_unavailable",
        "error_type": type(error).__name__ if error else None,
        "error_kind": error_kind,
        "upstream_status": upstream_status,
        "upstream_path": upstream_path,
        "methodology": {
            "version": ANALYTICS_METHOD_VERSION,
            "source": "empty_unavailable_fallback",
            "fallback": True,
            "error_kind": error_kind,
            "upstream_status": upstream_status,
            "note": "Sem dados ficticios; a UI deve mostrar estado indisponivel ate a API real responder.",
        },
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
    if re.search(r"\b(icpc|problema|problemas|diagnostico|diagnósticos|diagnosticos|multimorbilidade|polimedicacao|fragilidade|idra|gra|pic)\b", normalized):
        return "clinico"
    if re.search(r"\b(regiao|ars|uls|localizacao|geografica|concelho|distrito|postal|hospital|freguesia)\b", normalized):
        return "territorial"
    if re.search(r"\b(entidade|instituicao|unidade|servico|grupo|fornecedor|utente|doente|uf|usf|ucsp|aces|csp)\b", normalized):
        return "entidade"
    if re.search(r"\b(valor|total|taxa|numero|contagem|volume|quantidade|dias|encargos|custo|pvp)\b", normalized):
        return "medida"
    return "generico"


def _dimension_weight(kind: str) -> float:
    return {
        "temporal": 1.15,
        "territorial": 1.2,
        "entidade": 1.25,
        "clinico": 1.3,
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
        ("UF/CSP", r"\b(uf|unidade funcional|aces|cuidados primarios|centro de saude|usf|ucsp|csp)\b"),
        ("ARS/Regiao", r"\b(ars|regiao|regional|norte|centro|lisboa|alentejo|algarve)\b"),
        ("Concelho/Distrito", r"\b(concelho|distrito|municipio|localizacao)\b"),
        ("Farmacia/Medicamento", r"\b(farmacia|medicamento|medicamentos|farmaceutic)\b"),
        ("ICPC/Problemas", r"\b(icpc|problema|problemas|diagnostico|utente|utentes)\b"),
        ("Fragilidade", r"\b(multimorbilidade|polimedicacao|fragilidade|idra|gra|pic)\b"),
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
        ("problemas e multimorbilidade", r"\b(icpc|problema|problemas|utente|utentes|multimorbilidade|polimedicacao|fragilidade|idra|gra|pic)\b", 2.0),
        ("continuidade CSP", r"\b(uf|usf|ucsp|aces|csp|cuidados primarios|faltas|plano individual)\b", 1.6),
        ("acesso e tempos de resposta", r"\b(urgencia|espera|tempo|consulta|cirurgia|internamento|episodio)\b", 1.8),
        ("capacidade assistencial", r"\b(cama|lotacao|profissional|medico|enfermeiro|unidade|servico)\b", 1.4),
        ("medicamento e terapêutica", r"\b(medicamento|farmacia|stock|reserva|consumo|prescricao)\b", 1.3),
        ("financeiro com impacto operacional", r"\b(custo|encargo|despesa|pagamento)\b", 0.8),
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
                "local_scopes": sorted(set((source.get("facets") or {}).get("local_scopes", [])) | set((target.get("facets") or {}).get("local_scopes", []))),
                "institution_types": sorted(set((source.get("facets") or {}).get("institution_types", [])) | set((target.get("facets") or {}).get("institution_types", []))),
                "facet_tags": sorted(set((source.get("facets") or {}).get("tags", [])) | set((target.get("facets") or {}).get("tags", []))),
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
                "facets": item.get("facets", {}),
                "quality_flags": item.get("quality_flags", []),
                "analysis_readiness": item.get("analysis_readiness", {}),
            }
            for item in datasets
        ],
        "facet_counts": analysis.get("facet_counts", {}),
        "quality_flags": analysis.get("facet_counts", {}).get("quality_flags", {}),
        "themes": analysis.get("themes", []),
        "fallback": bool(analysis.get("fallback")),
        "warning": analysis.get("warning"),
        "empty_reason": analysis.get("empty_reason"),
        "error_kind": analysis.get("error_kind"),
        "upstream_status": analysis.get("upstream_status"),
        "upstream_path": analysis.get("upstream_path"),
        "methodology": {
            "version": ANALYTICS_METHOD_VERSION,
            "source": analysis.get("methodology", {}).get("source") or "catalog_link_semantics",
            "fallback": bool(analysis.get("fallback")),
            "error_kind": analysis.get("error_kind"),
            "upstream_status": analysis.get("upstream_status"),
        },
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


def _looks_like_numeric_distribution(
    field: dict,
    non_empty_values: list,
    numeric_values: list[float],
    numeric_ratio: float,
) -> bool:
    """Catch decimal/rate columns stored as text before they become nominal categories."""
    if _is_identifier_or_contact_field(field):
        return False
    if len(numeric_values) < 5 or numeric_ratio < 0.8:
        return False

    text = _field_search_text(field)
    if re.search(
        r"\b(codigo|cod|id|postal|nif|nipc|niss|telefone|telemovel|email|url|link|latitude|longitude|"
        r"grupo|tipo|categoria|sexo|regiao|entidade|hospital|uls|concelho|distrito|freguesia)\b",
        text,
    ):
        return False

    rounded_values = {round(value, 6) for value in numeric_values}
    unique_ratio = len(rounded_values) / max(1, len(numeric_values))
    has_fractional_values = any(abs(value - round(value)) > 1e-9 for value in numeric_values)
    measure_hint = bool(
        re.search(
            r"\b(valor|total|taxa|numero|n_|quantidade|dias|encargo|custo|pvp|percent|percentagem|"
            r"media|ratio|racio|indice|score|utentes|consulta|consultas)\b",
            text,
        )
    )

    if measure_hint:
        return True
    return bool(non_empty_values) and has_fractional_values and len(rounded_values) >= 8 and unique_ratio >= 0.35


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    center = len(ordered) // 2
    if len(ordered) % 2 == 0:
        return (ordered[center - 1] + ordered[center]) / 2.0
    return ordered[center]


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


def _association_strength(value: float | None) -> str:
    if value is None:
        return "insuficiente"
    magnitude = abs(value)
    if magnitude >= 0.7:
        return "forte"
    if magnitude >= 0.4:
        return "moderada"
    return "fraca"


def _association_warnings(
    pairs: list[tuple[float, float]],
    pearson: float | None,
    spearman: float | None = None,
    *,
    temporal_context: bool = False,
) -> list[str]:
    warnings = []
    if len(pairs) < 12:
        warnings.append("Amostra curta para associação estatística robusta.")
    if temporal_context:
        warnings.append("Associação calculada sobre amostra temporal; validar tendência comum e autocorrelação.")
    if pearson is not None and spearman is not None and abs(pearson - spearman) >= 0.35:
        warnings.append("Pearson e Spearman divergem; possível efeito de outliers ou relação não linear.")
    return warnings


_CORRELATION_GENERIC_TOKENS = {
    "a",
    "as",
    "ao",
    "aos",
    "da",
    "das",
    "de",
    "do",
    "dos",
    "e",
    "em",
    "na",
    "nas",
    "no",
    "nos",
    "o",
    "os",
    "para",
    "por",
    "taxa",
    "percent",
    "percentagem",
    "total",
    "numero",
    "n",
    "qt",
    "qtd",
    "quantidade",
    "valor",
    "media",
    "mediana",
    "ano",
    "anos",
    "1",
    "2",
    "3",
}

_CORRELATION_DOMAIN_TOKENS = {
    "atividade",
    "consulta",
    "consultas",
    "csp",
    "despesa",
    "embalagem",
    "embalagens",
    "internamento",
    "medicamento",
    "medicamentos",
    "mdf",
    "receita",
    "receitas",
    "utente",
    "utentes",
    "urgencia",
    "urgencias",
}


def _measure_text(field: dict) -> str:
    return _normalize_token(f"{field.get('name') or ''} {field.get('label') or ''}")


def _measure_tokens(field: dict, *, include_generic: bool = False) -> set[str]:
    tokens = {token for token in _measure_text(field).split() if token and len(token) > 1}
    if include_generic:
        return tokens
    return {token for token in tokens if token not in _CORRELATION_GENERIC_TOKENS}


def _concept_overlap(left: dict, right: dict) -> float:
    left_tokens = _measure_tokens(left)
    right_tokens = _measure_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / min(len(left_tokens), len(right_tokens))


def _shared_measure_domain(left: dict, right: dict) -> bool:
    left_domains = _measure_tokens(left, include_generic=True) & _CORRELATION_DOMAIN_TOKENS
    right_domains = _measure_tokens(right, include_generic=True) & _CORRELATION_DOMAIN_TOKENS
    return bool(left_domains & right_domains)


def _field_matches(field: dict, pattern: str) -> bool:
    return bool(re.search(pattern, _measure_text(field)))


def _constant_sum_pair(pairs: list[tuple[float, float]]) -> bool:
    if len(pairs) < 8:
        return False
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    if _stddev(xs) <= 1e-9 or _stddev(ys) <= 1e-9:
        return False
    sums = [x + y for x, y in pairs]
    scale = max(1.0, abs(_mean(sums)), max(abs(value) for value in sums))
    return _stddev(sums) <= scale * 0.005


def _correlation_exclusion_reason(left: dict, right: dict, pairs: list[tuple[float, float]]) -> str | None:
    if len(pairs) < 8:
        return None
    left_role = _measure_role(left)
    right_role = _measure_role(right)
    left_text = _measure_text(left)
    right_text = _measure_text(right)
    overlap = _concept_overlap(left, right)

    if _constant_sum_pair(pairs):
        return "medidas complementares; a soma entre campos e praticamente constante"

    if left_text == right_text or (overlap >= 0.9 and left_role == right_role):
        return "medidas duplicadas ou variantes do mesmo indicador"

    if {left_role, right_role} == {"taxa", "contagem"}:
        if overlap >= 0.45 or _shared_measure_domain(left, right):
            return "taxa e contagem do mesmo dominio; validar denominador antes de correlacionar"

    if left_role == right_role == "taxa":
        qualifier_pattern = r"\b(com|sem|todos|todas|opcao|atribuido|atribuida|atribuicao|inscritos)\b"
        if (overlap >= 0.65 or _shared_measure_domain(left, right)) and (
            re.search(qualifier_pattern, left_text) or re.search(qualifier_pattern, right_text)
        ):
            return "taxas do mesmo indicador com denominadores sobrepostos"

    if left_role == right_role == "contagem" and _shared_measure_domain(left, right):
        denominatorish = r"\b(total|inscritos|inscrito|populacao|utentes)\b"
        subgroupish = r"\b(com|sem|atribuido|atribuida|atribuicao|subgrupo)\b"
        if (
            (_field_matches(left, denominatorish) or _field_matches(right, denominatorish))
            and (_field_matches(left, subgroupish) or _field_matches(right, subgroupish))
        ):
            return "relacao parte-total; correlacao mecanica com denominador comum"

    return None


def _measure_context(
    field: dict,
    *,
    dataset_id: str = "",
    dataset_title: str = "",
    mega_theme: str = "",
) -> dict:
    raw_field = f"{field.get('name') or ''} {field.get('label') or ''}"
    field_text = _normalize_token(raw_field)
    dataset_text = _normalize_token(f"{dataset_id} {dataset_title} {mega_theme}")
    combined = f"{field_text} {dataset_text}".strip()
    reasons: list[str] = []
    role = "desconhecido"
    confidence = 0.35
    unit_family = "desconhecido"

    dimension_pattern = r"\b(codigo|cod|id|grupo|tipo|categoria|sexo|idade|regiao|entidade|hospital|uls|concelho|distrito|localizacao)\b"
    money_pattern = r"\b(custo|despesa|encargo|pagamento|preco|pvp|eur|euro|montante|orcamento|receita|faturacao|facturacao)\b"
    value_pattern = r"\b(valor|total_valor)\b"
    stock_pattern = r"\b(stock|saldo|lotacao|lotação|capacidade|reserva|existencia|existencias|inventario)\b"
    count_pattern = (
        r"\b(total|numero|n |n_|no_|num_|qtd|qt|quantidade|producao|produção|atividade|actividade|"
        r"episodio|episodios|consulta|consultas|atendimento|atendimentos|internamento|internamentos|"
        r"cirurgia|cirurgias|utentes|doentes|prescricao|prescricoes|receita|receitas|embalagem|embalagens|"
        r"dose|doses|saida|saidas|entrada|entradas|dispensa|dispensas)\b"
    )
    rate_pattern = r"\b(taxa|percent|percentagem|proporcao|proporção|ratio|racio|indice|cobertura)\b"

    if "%" in raw_field or re.search(rate_pattern, field_text):
        role = "taxa"
        confidence = 0.95
        unit_family = "percentagem"
        reasons.append("campo expresso como taxa/percentagem")
    elif re.search(stock_pattern, field_text) and not re.search(money_pattern, field_text):
        role = "stock"
        confidence = 0.85
        unit_family = "volume"
        reasons.append("campo de stock/saldo/capacidade")
    elif re.search(money_pattern, field_text):
        role = "monetario"
        confidence = 0.95
        unit_family = "moeda"
        reasons.append("termos monetários no campo")
    elif re.search(value_pattern, field_text) and re.search(money_pattern, dataset_text):
        role = "monetario"
        confidence = 0.78
        unit_family = "moeda"
        reasons.append("campo genérico de valor em dataset financeiro")
    elif re.search(value_pattern, field_text):
        role = "monetario"
        confidence = 0.68
        unit_family = "moeda"
        reasons.append("campo genérico de valor; validar unidade")
    elif re.search(count_pattern, field_text):
        role = "contagem"
        confidence = 0.88
        unit_family = "volume"
        reasons.append("termos de volume/contagem no campo")
    elif re.search(value_pattern, field_text) and re.search(stock_pattern, combined):
        role = "stock"
        confidence = 0.72
        unit_family = "volume"
        reasons.append("valor associado a stock/reserva")
    elif _canonical_field_type(field) in {"integer", "float"} and not re.search(dimension_pattern, field_text):
        role = "contagem"
        confidence = 0.55
        unit_family = "volume"
        reasons.append("campo numérico sem unidade explícita")

    if re.search(dimension_pattern, field_text) and role == "desconhecido":
        reasons.append("parece dimensão/código, não medida")
    if not reasons:
        reasons.append("sem unidade inferível a partir dos metadados")

    return {
        "role": role,
        "confidence": round(confidence, 2),
        "unit_family": unit_family,
        "reasons": reasons[:3],
    }


def _measure_role(field: dict, *, dataset_id: str = "", dataset_title: str = "", mega_theme: str = "") -> str:
    return _measure_context(
        field,
        dataset_id=dataset_id,
        dataset_title=dataset_title,
        mega_theme=mega_theme,
    )["role"]


def _dataset_semantic_profile(
    fields: list[dict],
    *,
    dataset_id: str = "",
    dataset_title: str = "",
    mega_theme: str = "",
) -> dict:
    counts: dict[str, int] = {}
    confident_counts: dict[str, int] = {}
    examples: dict[str, list[str]] = {}
    for field in fields or []:
        if not field.get("name"):
            continue
        if not _is_measure_candidate(field):
            continue
        context = _measure_context(
            field,
            dataset_id=dataset_id,
            dataset_title=dataset_title,
            mega_theme=mega_theme,
        )
        role = context["role"]
        counts[role] = counts.get(role, 0) + 1
        if context["confidence"] >= 0.7:
            confident_counts[role] = confident_counts.get(role, 0) + 1
        examples.setdefault(role, [])
        if len(examples[role]) < 3:
            examples[role].append(_safe_label(field) or field["name"])

    if confident_counts.get("monetario"):
        finprod_role = "monetario"
    elif confident_counts.get("contagem") or counts.get("contagem") or confident_counts.get("stock") or counts.get("stock"):
        finprod_role = "volume"
    elif confident_counts.get("taxa") or counts.get("taxa"):
        finprod_role = "taxa"
    else:
        finprod_role = "sem_medida"

    return {
        "role_counts": dict(sorted(counts.items())),
        "confident_role_counts": dict(sorted(confident_counts.items())),
        "examples": examples,
        "finprod_role": finprod_role,
    }


def _safe_label(field: dict) -> str:
    return field.get("label") or field.get("name") or ""


def _geo_point(value) -> tuple[float, float] | None:
    """Return (lat, lon) for ODS geopoint-like values without treating them as categories."""
    if isinstance(value, dict):
        lat = value.get("lat", value.get("latitude"))
        lon = value.get("lon", value.get("lng", value.get("longitude")))
        lat_number = _coerce_number(lat)
        lon_number = _coerce_number(lon)
        if lat_number is not None and lon_number is not None:
            return lat_number, lon_number
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        first = _coerce_number(value[0])
        second = _coerce_number(value[1])
        if first is None or second is None:
            return None
        if abs(first) <= 90 and abs(second) <= 180:
            return first, second
        if abs(second) <= 90 and abs(first) <= 180:
            return second, first
    return None


def _geo_profile(column: str, field: dict, raw_values: list) -> dict | None:
    points = [_geo_point(value) for value in raw_values if value not in (None, "")]
    points = [point for point in points if point is not None]
    if not points:
        return None
    lats = [point[0] for point in points]
    lons = [point[1] for point in points]
    unique_points = sorted({(round(lat, 5), round(lon, 5)) for lat, lon in points})
    return {
        "field": column,
        "label": _safe_label(field),
        "type": "geo",
        "semantic_role": "geolocation",
        "count": len(points),
        "missing": len(raw_values) - len(points),
        "unique": len(unique_points),
        "bounds": {
            "lat_min": round(min(lats), 6),
            "lat_max": round(max(lats), 6),
            "lon_min": round(min(lons), 6),
            "lon_max": round(max(lons), 6),
        },
        "center": {
            "lat": round(_mean(lats), 6),
            "lon": round(_mean(lons), 6),
        },
        "sample_points": [
            {"lat": lat, "lon": lon}
            for lat, lon in unique_points[:5]
        ],
        "top_values": [],
    }


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
    excluded_fields = []
    missing_cells = 0
    total_cells = 0
    for field in fields:
        values_by_idx = numeric_by_column.get(field, {})
        values = [values_by_idx.get(idx) for idx in range(sample_size)]
        non_null = [value for value in values if value is not None]
        total_cells += len(values)
        missing_cells += len(values) - len(non_null)
        if len(non_null) < 5:
            excluded_fields.append({"field": field, "reason": "menos de 5 valores numericos"})
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
        "method": "PCA aproximada sobre medidas padronizadas; valores em falta imputados como média (0 após padronização).",
        "sample_size": sample_size,
        "included_fields": usable_fields,
        "excluded_fields": excluded_fields,
        "missingness": {
            "cells": missing_cells,
            "total_cells": total_cells,
            "ratio": round(missing_cells / total_cells, 4) if total_cells else 0,
        },
        "explained_variance": {
            "pc1": round(eigen1 / total_variance, 4),
            "pc2": round(eigen2 / total_variance, 4),
        },
        "warnings": [
            warning
            for warning in [
                "PCA sensivel a imputacao de valores em falta." if missing_cells else None,
                "Interpretar apenas como triagem exploratoria, nao como modelo causal.",
            ]
            if warning
        ],
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
        if profile.get("semantic_role") == "geolocation":
            completeness = profile["count"] / sample_size
            bounds = profile.get("bounds") or {}
            lat_span = abs((bounds.get("lat_max") or 0) - (bounds.get("lat_min") or 0))
            lon_span = abs((bounds.get("lon_max") or 0) - (bounds.get("lon_min") or 0))
            spatial_span = min(1, (lat_span + lon_span) / 3)
            score = (completeness * 28) + (spatial_span * 18) + min(18, math.log(profile.get("unique", 1) + 1) * 4)
            rows.append(
                {
                    "field": profile["field"],
                    "label": profile["label"] or profile["field"],
                    "kind": "geografia",
                    "score": round(score, 2),
                    "drivers": [
                        f"coordenadas válidas {round(completeness * 100)}%",
                        f"pontos únicos {profile.get('unique', 0)}",
                        "usar para mapa, não como categoria nominal",
                    ],
                    "dominant_sample": None,
                }
            )
            continue
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
    return rows[:18]


def _deterministic_method_rng(dataset_id: str) -> random.Random:
    seed_material = f"{dataset_id}|{ANALYTICS_METHOD_VERSION}|feature_screening".encode("utf-8")
    seed = int(hashlib.sha256(seed_material).hexdigest()[:16], 16)
    return random.Random(seed)


def _build_boruta_selection(payload: dict) -> dict:
    """Boruta-inspired deterministic screening against synthetic shadow noise."""
    features = payload.get("feature_importance", [])
    if not features:
        return {
            "method": "feature_screening_deterministic_shadow",
            "confirmed": [],
            "tentative": [],
            "rejected": [],
            "thresholds": {"max_shadow": 0, "median_shadow": 0},
        }

    rng = _deterministic_method_rng(str(payload.get("dataset_id") or "dataset"))
    shadow_scores = []
    for _ in range(50):
        shadow_score = (rng.random() * 22) + (rng.random() * 22) + (rng.random() * 10)
        shadow_scores.append(shadow_score)

    max_shadow = max(shadow_scores)
    median_shadow = _median(shadow_scores) or 0

    confirmed = []
    tentative = []
    rejected = []

    for feat in features:
        row = dict(feat)
        score = float(row.get("score") or 0)
        if score > max_shadow:
            row["selection"] = "confirmada"
            confirmed.append(row)
        elif score > median_shadow:
            row["selection"] = "tentativa"
            tentative.append(row)
        else:
            row["selection"] = "rejeitada"
            rejected.append(row)

    return {
        "method": "feature_screening_deterministic_shadow",
        "confirmed": confirmed,
        "tentative": tentative,
        "rejected": rejected,
        "thresholds": {
            "max_shadow": round(max_shadow, 2),
            "median_shadow": round(median_shadow, 2),
        },
    }


def _build_territorial_map(payload: dict) -> dict:
    """Map signal intensity to SNS Regions and ULS based on categorical profiles."""
    profiles = payload.get("categorical_profiles", [])
    geo_data = {"regions": {}, "uls": {}, "entities": {}}

    for profile in profiles:
        field = _normalize_token(profile["field"])
        is_region = "regiao" in field or "ars" in field
        is_uls = "uls" in field
        is_entity = "entidade" in field or "hospital" in field

        if not (is_region or is_uls or is_entity):
            continue

        for val in profile.get("top_values", []):
            name = str(val["value"])
            count = val["count"]
            if is_region:
                geo_data["regions"][name] = geo_data["regions"].get(name, 0) + count
            elif is_uls:
                geo_data["uls"][name] = geo_data["uls"].get(name, 0) + count
            else:
                geo_data["entities"][name] = geo_data["entities"].get(name, 0) + count
    return geo_data



def _build_data_analytics(dataset_id: str, limit: int) -> dict:
    cache_key = f"data_analytics:{dataset_id}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    if dataset_id.startswith("demo-"):
        raise ValueError("Dataset demonstrativo recusado: a aplicação só deve analisar datasets reais da API SNS.")

    dataset_path = f"/catalog/datasets/{quote(dataset_id)}"
    dataset = _ods_fetch(dataset_path)
    fields = dataset.get("fields", []) or []
    metas = dataset.get("metas", {}) or {}
    title = (dataset.get("title") or dataset.get("title_pt") or _get_dataset_title(metas)).strip() or dataset_id
    mega_theme = _classify_dataset(dataset_id, title, [field.get("name") or "" for field in fields])
    total_records = ((metas.get("default") or {}).get("records_count") or dataset.get("records_count") or 0)
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

        geo_profile = _geo_profile(column, field, raw_values)
        if geo_profile and (canonical_type == "geo" or geo_profile["count"] / max(1, len(non_empty)) >= 0.65):
            categorical_profiles.append(geo_profile)
            continue

        numeric_pairs = [(idx, _coerce_number(value)) for idx, value in enumerate(raw_values)]
        numeric_values_by_idx = {idx: value for idx, value in numeric_pairs if value is not None}
        numeric_values = list(numeric_values_by_idx.values())
        numeric_ratio = len(numeric_values) / len(non_empty) if non_empty else 0
        measure_candidate = _is_measure_candidate(field)
        numeric_distribution = (
            not measure_candidate
            and _looks_like_numeric_distribution(field, non_empty, numeric_values, numeric_ratio)
        )

        if (measure_candidate or numeric_distribution) and len(numeric_values) >= 5 and numeric_ratio >= 0.65:
            avg = _mean(numeric_values)
            measure_context = _measure_context(
                field,
                dataset_id=dataset_id,
                dataset_title=title,
                mega_theme=mega_theme,
            )
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
                    "measure_role": measure_context["role"],
                    "measure_context": measure_context,
                    "semantic_role": "numeric_distribution" if numeric_distribution else "measure",
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
    correlation_exclusions = []
    numeric_columns = [profile["field"] for profile in numeric_profiles[:12]]
    for idx, left in enumerate(numeric_columns):
        for right in numeric_columns[idx + 1 :]:
            left_values = numeric_by_column.get(left, {})
            right_values = numeric_by_column.get(right, {})
            shared_indexes = sorted(set(left_values) & set(right_values))
            pairs = [(left_values[index], right_values[index]) for index in shared_indexes]
            left_field = field_by_name.get(left, {"name": left})
            right_field = field_by_name.get(right, {"name": right})
            exclusion_reason = _correlation_exclusion_reason(left_field, right_field, pairs)
            if exclusion_reason:
                correlation_exclusions.append(
                    {
                        "field_a": left,
                        "field_b": right,
                        "label_a": _safe_label(left_field),
                        "label_b": _safe_label(right_field),
                        "reason": exclusion_reason,
                        "samples": len(pairs),
                    }
                )
                continue
            corr = _pearson(pairs)
            if corr is None or abs(corr) < 0.25:
                continue
            rank_corr = _spearman(pairs)
            correlations.append(
                {
                    "field_a": left,
                    "field_b": right,
                    "label_a": _safe_label(left_field),
                    "label_b": _safe_label(right_field),
                    "method": "pearson",
                    "correlation": round(corr, 4),
                    "abs_correlation": round(abs(corr), 4),
                    "spearman": round(rank_corr, 4) if rank_corr is not None else None,
                    "strength": _association_strength(corr),
                    "samples": len(pairs),
                    "warnings": _association_warnings(pairs, corr, rank_corr, temporal_context=bool(temporal_field)),
                }
            )
    correlations.sort(key=lambda item: (-item["abs_correlation"], -item["samples"], item["field_a"], item["field_b"]))

    trends = []
    if temporal_field:
        for profile in numeric_profiles[:4]:
            buckets: dict[str, dict[str, object]] = {}
            values_by_idx = numeric_by_column.get(profile["field"], {})
            role = profile.get("measure_role", "desconhecido")
            aggregation = "soma" if role in {"monetario", "contagem"} else "media"
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
                {
                    "period": bucket["label"],
                    "avg": round(_mean(bucket["values"]), 4),
                    "sum": round(sum(bucket["values"]), 4),
                    "value": round(sum(bucket["values"]) if aggregation == "soma" else _mean(bucket["values"]), 4),
                    "count": len(bucket["values"]),
                }
                for _, bucket in sorted(buckets.items())
                if bucket["values"]
            ]
            if len(points) >= 2:
                min_count = min(point["count"] for point in points)
                trends.append(
                    {
                        "field": profile["field"],
                        "label": profile["label"],
                        "measure_role": role,
                        "measure_context": profile.get("measure_context") or {"role": role, "confidence": 0.35, "unit_family": "desconhecido", "reasons": []},
                        "aggregation": aggregation,
                        "validation": {
                            "periods": len(points),
                            "min_records_per_period": min_count,
                            "warnings": [
                                warning
                                for warning in [
                                    "Poucos periodos para leitura temporal robusta." if len(points) < 6 else None,
                                    "Periodos com menos de 3 registos." if min_count < 3 else None,
                                    "Medida com papel semantico desconhecido; validar denominador e unidade." if profile.get("measure_role") == "desconhecido" else None,
                                ]
                                if warning
                            ],
                        },
                        "points": points[-24:],
                    }
                )

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
        nominal_profiles = [profile for profile in categorical_profiles if profile.get("semantic_role") != "geolocation"]
        if nominal_profiles:
            dominant = max(nominal_profiles, key=lambda item: item["top_values"][0]["count"] if item["top_values"] else 0)
            top_value = dominant["top_values"][0] if dominant["top_values"] else {"value": "-", "count": 0}
            insights.append({"label": "Categoria dominante", "value": dominant["label"], "detail": f"{top_value['value']} · {top_value['count']} registos"})
        else:
            spatial = categorical_profiles[0]
            insights.append({"label": "Dimensão espacial", "value": spatial["label"], "detail": f"{spatial.get('unique', 0)} pontos únicos"})

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
    sample_warnings = [
        warning
        for warning in [
            "Amostra truncada pelo limite local; nao interpretar como estimativa completa do dataset."
            if total_records and len(records) < total_records
            else None,
            "Amostra ordenada por periodo recente; pode enviesar correlacoes, PCA e tendencias."
            if ordering == "temporal_desc"
            else None,
            "Sem eixo temporal detetado; tendencias e projecoes ficam limitadas."
            if not temporal_field
            else None,
        ]
        if warning
    ]
    profile_missing_ratios = []
    for profile in [*numeric_profiles, *categorical_profiles]:
        total_observed = (profile.get("count") or 0) + (profile.get("missing") or 0)
        if total_observed:
            profile_missing_ratios.append((profile.get("missing") or 0) / total_observed)
    max_missing_ratio = max(profile_missing_ratios, default=0.0)
    readiness = _analysis_readiness(
        sample_size=len(records),
        total_records=total_records,
        temporal_field=temporal_field,
        numeric_count=len(numeric_profiles),
        categorical_count=len(categorical_profiles),
        correlation_count=len(correlations),
        trend_count=len(trends),
        warning_count=len(sample_warnings),
        max_missing_ratio=max_missing_ratio,
    )
    result = {
        "dataset_id": dataset_id,
        "title": title,
        "mega_theme": mega_theme,
        "sample_size": len(records),
        "requested_limit": safe_limit,
        "total_records": total_records,
        "temporal_field": temporal_field,
        "ordering": ordering,
        "methodology": {
            "version": ANALYTICS_METHOD_VERSION,
            "scope": "triagem exploratoria sobre amostra leve",
            "sample_design": "amostra de API local, nao aleatoria; validar antes de inferencia",
        },
        "sample": {
            "sample_size": len(records),
            "requested_limit": safe_limit,
            "total_records": total_records,
            "ordering": ordering,
            "coverage_ratio": round(len(records) / total_records, 4) if total_records else None,
            "max_missing_ratio": round(max_missing_ratio, 4),
        },
        "quality_warnings": sample_warnings,
        "analysis_readiness": readiness,
        "schema_quality": _schema_quality(fields, observed_columns),
        "semantic_profile": _dataset_semantic_profile(
            fields,
            dataset_id=dataset_id,
            dataset_title=title,
            mega_theme=mega_theme,
        ),
        **meta,
        "numeric_profiles": numeric_profiles[:12],
        "categorical_profiles": categorical_profiles[:10],
        "correlations": correlations[:20],
        "correlation_exclusions": correlation_exclusions[:20],
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
    return sorted(
        trends,
        key=lambda trend: (
            -_finprod_trend_role_score(trend, mode=mode),
            -len(_finprod_trend_points(trend, mode=mode)),
            str(trend.get("label") or trend.get("field") or ""),
        ),
    )[0]


def _finprod_trend_points(trend: dict | None, *, mode: str | None = None) -> dict[str, float]:
    points: dict[str, float] = {}
    role = _normalize_token(str((trend or {}).get("measure_role") or ""))
    prefer_sum = role in {"monetario", "contagem"} or mode in {"financial", "production"}
    for point in (trend or {}).get("points", []):
        period = _finprod_period_key(point.get("period"))
        value_source = point.get("sum") if prefer_sum and point.get("sum") is not None else point.get("avg")
        value = _coerce_number(value_source)
        if period is None or value is None:
            continue
        points[period] = float(value)
    return points


def _finprod_trend_role_score(trend: dict | None, *, mode: str) -> int:
    if not trend:
        return 0
    label = _normalize_token(str(trend.get("label") or trend.get("field") or ""))
    role = _normalize_token(str(trend.get("measure_role") or ""))
    keywords = {
        "financial": r"\b(custo|despesa|encargo|pagamento|contrato|valor|orcamento|orçamento)\b",
        "production": r"\b(producao|produção|consulta|urgencia|urgência|cirurgia|episodio|episódio|atendimento|atividade|volume|numero)\b",
    }.get(mode, r"$^")
    score = 0
    if re.search(keywords, label):
        score += 4
    if mode == "financial" and role in {"monetario", "stock"}:
        score += 3
    if mode == "financial" and role == "stock":
        score -= 2
    if mode == "production" and role == "contagem":
        score += 3
    if mode == "production" and role == "monetario":
        score -= 2
    if mode == "financial" and role == "contagem":
        score -= 3
    confidence = _coerce_number((trend.get("measure_context") or {}).get("confidence"))
    if confidence is not None:
        score += int(confidence * 2)
    if role and role != "desconhecido":
        score += 1
    return score


def _select_finprod_trend_pair(financial_trends: list[dict], production_trends: list[dict]) -> dict:
    def period_range(points: dict[str, float]) -> dict[str, str | None]:
        periods = sorted(points)
        return {"start": periods[0], "end": periods[-1]} if periods else {"start": None, "end": None}

    candidates = []
    for financial_trend in financial_trends or []:
        financial_points = _finprod_trend_points(financial_trend, mode="financial")
        if not financial_points:
            continue
        for production_trend in production_trends or []:
            production_points = _finprod_trend_points(production_trend, mode="production")
            if not production_points:
                continue
            shared_periods = sorted(set(financial_points) & set(production_points))
            candidates.append(
                {
                    "financial_trend": financial_trend,
                    "production_trend": production_trend,
                    "financial_points": financial_points,
                    "production_points": production_points,
                    "shared_periods": shared_periods,
                    "score": (
                        len(shared_periods) * 10
                        + _finprod_trend_role_score(financial_trend, mode="financial")
                        + _finprod_trend_role_score(production_trend, mode="production")
                    ),
                }
            )
    if not candidates:
        return {
            "financial_trend": _pick_finprod_trend(financial_trends, mode="financial"),
            "production_trend": _pick_finprod_trend(production_trends, mode="production"),
            "financial_points": _finprod_trend_points(_pick_finprod_trend(financial_trends, mode="financial"), mode="financial"),
            "production_points": _finprod_trend_points(_pick_finprod_trend(production_trends, mode="production"), mode="production"),
            "shared_periods": [],
            "candidates": [],
            "selection_reason": "sem_tendencias_temporais_validas",
        }
    candidates.sort(key=lambda item: (-len(item["shared_periods"]), -item["score"]))
    selected = candidates[0]
    selected["candidates"] = [
        {
            "financial_label": candidate["financial_trend"].get("label") or candidate["financial_trend"].get("field"),
            "production_label": candidate["production_trend"].get("label") or candidate["production_trend"].get("field"),
            "shared_periods": len(candidate["shared_periods"]),
            "financial_periods": len(candidate["financial_points"]),
            "production_periods": len(candidate["production_points"]),
            "financial_range": period_range(candidate["financial_points"]),
            "production_range": period_range(candidate["production_points"]),
        }
        for candidate in candidates[:8]
    ]
    selected["selection_reason"] = "melhor_sobreposicao_temporal"
    return selected


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
    xs = [float(pair[0]) for pair in pairs]
    ys = [float(pair[1]) for pair in pairs]

    def _average_ranks(values: list[float]) -> list[float]:
        ordered = sorted(enumerate(values), key=lambda item: item[1])
        ranks = [0.0] * len(values)
        idx = 0
        while idx < len(ordered):
            end = idx + 1
            while end < len(ordered) and ordered[end][1] == ordered[idx][1]:
                end += 1
            average_rank = (idx + 1 + end) / 2.0
            for pos in range(idx, end):
                original_idx = ordered[pos][0]
                ranks[original_idx] = average_rank
            idx = end
        return ranks

    rank_x = _average_ranks(xs)
    rank_y = _average_ranks(ys)
    ranked = list(zip(rank_x, rank_y))
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
    median = _median(values) or 0
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
    fin_share = {}
    fin_labels = {}
    for item in fin_top.get("top_values", []):
        raw = str(item.get("value") or "")
        key = _normalize_token(raw)
        if not key:
            continue
        fin_share[key] = fin_share.get(key, 0.0) + (item.get("count", 0) / fin_count)
        fin_labels.setdefault(key, raw)

    prod_share = {}
    prod_labels = {}
    for item in prod_top.get("top_values", []):
        raw = str(item.get("value") or "")
        key = _normalize_token(raw)
        if not key:
            continue
        prod_share[key] = prod_share.get(key, 0.0) + (item.get("count", 0) / prod_count)
        prod_labels.setdefault(key, raw)

    shared = sorted(set(fin_share) & set(prod_share))
    rows = []
    for key in shared:
        display = fin_labels.get(key) or prod_labels.get(key) or key
        rows.append(
            {
                "entity": display,
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
    trend_pair = _select_finprod_trend_pair(financial.get("trends", []), production.get("trends", []))
    financial_trend = trend_pair.get("financial_trend")
    production_trend = trend_pair.get("production_trend")
    financial_points = trend_pair.get("financial_points", {})
    production_points = trend_pair.get("production_points", {})
    shared_periods = trend_pair.get("shared_periods", [])
    financial_role = (financial_trend or {}).get("measure_role") or "desconhecido"
    production_role = (production_trend or {}).get("measure_role") or "desconhecido"
    ratio_valid = financial_role == "monetario" and production_role == "contagem"
    rows = []
    pairs = []
    for period in shared_periods:
        expense_value = financial_points[period]
        output_value = production_points[period]
        unit_cost = expense_value / output_value if ratio_valid and output_value > 0 else None
        rows.append(
            {
                "period": period,
                "financial_value": round(expense_value, 4),
                "production_value": round(output_value, 4),
                "unit_cost": round(unit_cost, 4) if unit_cost is not None else None,
            }
        )
        if ratio_valid and output_value > 0:
            pairs.append((expense_value, output_value))

    expense_scale, expense_unit = _normalize_measure_scale(
        [row["financial_value"] for row in rows],
        mode="financial" if financial_role == "monetario" else "production",
    )
    production_scale, production_unit = _normalize_measure_scale([row["production_value"] for row in rows], mode="production")
    for row in rows:
        row["financial_normalized"] = round(row["financial_value"] / expense_scale, 4) if expense_scale else row["financial_value"]
        row["production_normalized"] = round(row["production_value"] / production_scale, 4) if production_scale else row["production_value"]

    unit_cost_values = [row["unit_cost"] for row in rows if isinstance(row.get("unit_cost"), (int, float))]
    unit_cost_avg = round(_mean(unit_cost_values), 4) if unit_cost_values else None
    median_value = _median(unit_cost_values)
    unit_cost_median = round(median_value, 4) if median_value is not None else None
    correlation = _pearson(pairs)
    spearman = _spearman(pairs)
    correlation_label = _association_strength(correlation)
    outliers = _detect_unit_cost_outliers(rows)
    benchmark_rows = _entity_benchmark(financial, production)
    robustness = _robustness_band(pairs, correlation, spearman)
    shared_granularity = "desconhecida"
    if shared_periods:
        if all("-" in period for period in shared_periods):
            shared_granularity = "mensal/trimestral"
        elif all(re.fullmatch(r"\d{4}", period) for period in shared_periods):
            shared_granularity = "anual"
    numerator = {
        "dataset_id": financial.get("dataset_id"),
        "field": (financial_trend or {}).get("field"),
        "label": (financial_trend or {}).get("label"),
        "role": financial_role,
        "context": (financial_trend or {}).get("measure_context") or {},
        "aggregation": "soma",
    }
    denominator = {
        "dataset_id": production.get("dataset_id"),
        "field": (production_trend or {}).get("field"),
        "label": (production_trend or {}).get("label"),
        "role": production_role,
        "context": (production_trend or {}).get("measure_context") or {},
        "aggregation": "soma",
    }
    numerator_valid = financial_role == "monetario"
    denominator_valid = production_role == "contagem"
    hard_blocked = not numerator_valid or not denominator_valid or not shared_periods
    numerator_label = "soma monetária por período comum" if numerator_valid else "soma de volume/quantidade por período comum"
    denominator_label = "soma de produção/atividade por período comum" if denominator_valid else "medida não confirmada como contagem"
    unit_warnings = [
        warning
        for warning in [
            "Numerador é volume/quantidade, não valor monetário; não interpretar como custo unitário."
            if numerator["role"] != "monetario"
            else None,
            "Denominador de produção sem papel de contagem confiável."
            if denominator["role"] != "contagem"
            else None,
            "Sem períodos comuns para estimar custo unitário."
            if not shared_periods
            else None,
            "Produção nula ou negativa em alguns períodos; esses pontos não entram na associação."
            if any((row.get("production_value") or 0) <= 0 for row in rows)
            else None,
            "Sem interseção territorial/entidade nas top categorias; validar âmbito antes de comparar."
            if not benchmark_rows
            else None,
        ]
        if warning
    ]
    comparability_checks = [
        {
            "label": "Eixo temporal nos dois datasets",
            "status": "ok" if financial.get("temporal_field") and production.get("temporal_field") else "warning",
            "detail": f"{financial.get('temporal_field') or '-'} / {production.get('temporal_field') or '-'}",
        },
        {
            "label": "Períodos em comum",
            "status": "ok" if len(rows) >= 6 else ("warning" if len(rows) >= 3 else "error"),
            "detail": f"{len(rows)} períodos · granularidade {shared_granularity}",
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
        {
            "label": "Compatibilidade metodológica",
            "status": "warning" if unit_warnings else "ok",
            "detail": "Validar unidade, âmbito geográfico, base contabilística e população antes de interpretar custo unitário.",
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
            "blocked": hard_blocked,
            "blocked_reason": "validar numerador monetário, denominador de produção e períodos comuns" if hard_blocked else None,
            "avg_unit_cost": unit_cost_avg,
            "median_unit_cost": unit_cost_median,
            "expense_output_correlation": round(correlation, 4) if correlation is not None else None,
            "spearman_correlation": round(spearman, 4) if spearman is not None else None,
            "correlation_strength": correlation_label,
            "robustness": robustness,
            "sample_pairs": len(pairs),
        },
        "comparability": {"checks": comparability_checks},
        "aggregation": {
            "period": shared_granularity,
            "numerator": numerator_label,
            "denominator": denominator_label,
        },
        "numerator": numerator,
        "denominator": denominator,
        "grouping": {
            "period_key": "periodo normalizado",
            "matched_periods": len(shared_periods),
            "entity_benchmark_available": bool(benchmark_rows),
        },
        "unit_warnings": unit_warnings,
        "methodology": {
            "version": ANALYTICS_METHOD_VERSION,
            "scope": "custo unitario exploratorio por periodo comum",
            "required_validation": [
                "mesma granularidade temporal",
                "mesmo ambito territorial/entidade",
                "unidades financeiras e produtivas conhecidas",
                "denominador de producao compatível com o numerador financeiro",
            ],
        },
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
            "trend_selection": trend_pair.get("selection_reason"),
            "trend_candidates": trend_pair.get("candidates", []),
            "warnings": [
                warning
                for warning in [
                    "Sem períodos comuns suficientes nas tendências disponíveis." if len(rows) < 2 else None,
                    "Poucos períodos comuns; usar apenas como triagem temporal." if 2 <= len(rows) < 6 else None,
                    "Correlação com baixa robustez estatística." if robustness in {"baixa", "insuficiente"} else None,
                    "Outliers detetados no custo unitário." if outliers else None,
                    "Outliers instáveis com menos de 10 períodos." if outliers and len(rows) < 10 else None,
                    "Custo unitário depende de validação de unidade, geografia e base contabilística.",
                    *unit_warnings,
                ]
                if warning
            ],
        },
        "generated_at": int(_now()),
    }
    _cache_set(cache_key, result)
    return result


def _build_finprod_recommendations(financial_dataset: str, production_dataset: str | None, limit: int, candidate_limit: int) -> dict:
    cache_key = f"finprod:recommendations:{financial_dataset}:{production_dataset or '-'}:{limit}:{candidate_limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    catalog = _get_analysis_catalog()
    datasets = catalog.get("datasets", []) or []
    title_by_id = {dataset.get("dataset_id"): dataset.get("title") for dataset in datasets}
    production_candidates = [
        dataset
        for dataset in datasets
        if dataset.get("dataset_id") != financial_dataset and dataset.get("mega_theme") == "Acesso & Produção"
    ]
    production_candidates.sort(
        key=lambda dataset: (
            dataset.get("dataset_id") != production_dataset,
            -int(dataset.get("metric_candidate_count") or 0),
            -int(dataset.get("records_count") or 0),
            dataset.get("title") or dataset.get("dataset_id") or "",
        )
    )

    recommendations = []
    errors = []
    for candidate in production_candidates[:candidate_limit]:
        candidate_id = candidate.get("dataset_id")
        if not candidate_id:
            continue
        try:
            payload = _build_finance_production(financial_dataset, candidate_id, limit)
        except Exception as exc:
            errors.append({"dataset_id": candidate_id, "error": type(exc).__name__})
            continue
        best = (payload.get("diagnostics", {}).get("trend_candidates") or [{}])[0]
        summary = payload.get("summary") or {}
        numerator = payload.get("numerator") or {}
        denominator = payload.get("denominator") or {}
        recommendations.append(
            {
                "financial_dataset_id": financial_dataset,
                "financial_title": title_by_id.get(financial_dataset) or financial_dataset,
                "production_dataset_id": candidate_id,
                "production_title": title_by_id.get(candidate_id) or candidate.get("title") or candidate_id,
                "matched_periods": summary.get("matched_periods") or 0,
                "sample_pairs": summary.get("sample_pairs") or 0,
                "robustness": summary.get("robustness") or "insuficiente",
                "correlation_strength": summary.get("correlation_strength") or "insuficiente",
                "blocked": bool(summary.get("blocked")),
                "numerator_role": numerator.get("role") or "desconhecido",
                "denominator_role": denominator.get("role") or "desconhecido",
                "financial_range": best.get("financial_range") or {},
                "production_range": best.get("production_range") or {},
                "trend_label_financial": (payload.get("financial_dataset") or {}).get("trend_label"),
                "trend_label_production": (payload.get("production_dataset") or {}).get("trend_label"),
                "is_current": candidate_id == production_dataset,
            }
        )

    recommendations.sort(
        key=lambda row: (
            -int(row.get("matched_periods") or 0),
            -int(row.get("sample_pairs") or 0),
            0 if row.get("robustness") == "alta" else 1 if row.get("robustness") == "moderada" else 2,
            0 if row.get("is_current") else 1,
            row.get("production_title") or "",
        )
    )
    useful_recommendations = [
        row
        for row in recommendations
        if int(row.get("matched_periods") or 0) > 0 and not row.get("blocked")
    ]
    result = {
        "financial_dataset_id": financial_dataset,
        "financial_title": title_by_id.get(financial_dataset) or financial_dataset,
        "active_production_dataset_id": production_dataset,
        "recommendations": useful_recommendations[:6],
        "candidates": recommendations[:candidate_limit],
        "useful_count": len(useful_recommendations),
        "candidate_count": len(production_candidates[:candidate_limit]),
        "errors": errors[:5],
        "warning": "Sem alternativas temporais com períodos comuns nos candidatos testados." if not useful_recommendations else None,
        "generated_at": int(_now()),
    }
    _cache_set(cache_key, result)
    return result


def _predictive_catalog_score(dataset: dict) -> tuple[int, list[str]]:
    text = _normalize_token(f"{dataset.get('dataset_id') or ''} {dataset.get('title') or ''}")
    score = 0
    reasons: list[str] = []
    if re.search(r"\b(mensal|mes|mês)\b", text):
        score += 45
        reasons.append("mensal")
    if re.search(r"\b(trimestr|quarter)\b", text):
        score += 36
        reasons.append("trimestral")
    if re.search(r"\b(diari|dia|tempo real)\b", text):
        score += 30
        reasons.append("diário")
    if re.search(r"\b(evolucao|evolução|historico|histórico|sazonal|serie|série)\b", text):
        score += 26
        reasons.append("evolução")
    metric_count = int(dataset.get("metric_candidate_count") or 0)
    if metric_count > 0:
        score += min(20, metric_count * 4)
        reasons.append(f"{metric_count} medida(s) provável(eis)")
    if int(dataset.get("field_count") or 0) >= 6:
        score += 8
        reasons.append("schema rico")
    return score, reasons


def _predictive_trend_points(trend: dict) -> list[dict]:
    points = []
    for index, point in enumerate(trend.get("points") or []):
        value = _coerce_number(point.get("value", point.get("avg")))
        if value is None:
            continue
        points.append({"x": index, "period": point.get("period"), "value": value})
    return points


def _predictive_trend_diagnostic(trend: dict, payload: dict) -> dict:
    points = _predictive_trend_points(trend)
    sample_size = int(payload.get("sample_size") or 0)
    records_per_period = sample_size / len(points) if points else 0
    values = [point["value"] for point in points]
    unique_values = len({round(value, 6) for value in values})
    min_value = min(values) if values else 0
    max_value = max(values) if values else 0
    mean_value = _mean(values) if values else 0
    relative_span = abs((max_value - min_value) / (abs(mean_value) or 1)) if values else 0
    gaps = []
    if len(points) < 2:
        gaps.append("sem pontos temporais comparáveis")
    if 2 <= len(points) < 4:
        gaps.append(f"faltam {4 - len(points)} período(s)")
    if len(points) > 50:
        gaps.append("série longa exige agregação/sazonalidade")
    if sample_size and records_per_period < 8:
        gaps.append("baixo volume médio por período")
    if values and unique_values < 3:
        gaps.append("valores quase constantes")
    if values and relative_span < 0.015:
        gaps.append("variação relativa baixa")

    ok = bool(points) and len(points) >= 4 and len(points) <= 50 and not (
        (sample_size and records_per_period < 8)
        or (values and unique_values < 3)
        or (values and relative_span < 0.015)
    )
    score = max(
        0,
        min(
            100,
            round(
                min(34, len(points) * 8)
                + min(22, records_per_period or sample_size / 6)
                + min(22, relative_span * 180)
                + min(12, unique_values * 3)
                + (10 if ok else 0)
                - (16 if len(points) > 50 else 0)
            ),
        ),
    )
    if ok and not gaps:
        gaps.append("apta para projeção exploratória curta")
    return {
        "label": trend.get("label") or trend.get("field") or "Série temporal",
        "field": trend.get("field"),
        "ok": ok,
        "score": score,
        "points": len(points),
        "records_per_period": round(records_per_period, 2) if records_per_period else 0,
        "relative_span": round(relative_span, 4),
        "reason": "" if ok else (gaps[0] if gaps else "sem sinal preditivo suficiente"),
        "gaps": gaps,
    }


def _predictive_payload_fit(payload: dict, catalog_score: int = 0, catalog_reasons: list[str] | None = None) -> dict:
    diagnostics = [
        _predictive_trend_diagnostic(trend, payload)
        for trend in payload.get("trends", []) or []
        if trend.get("points")
    ]
    diagnostics.sort(key=lambda row: (not row["ok"], -row["score"], -row["points"], row["label"]))
    eligible = [row for row in diagnostics if row["ok"]]
    near = [row for row in diagnostics if not row["ok"] and row["points"] >= 2]
    best = eligible[0] if eligible else (near[0] if near else (diagnostics[0] if diagnostics else None))
    if eligible:
        band = "ready" if eligible[0]["score"] >= 65 else "usable"
    elif near:
        band = "near"
    else:
        band = "blocked"
    if not payload.get("temporal_field"):
        reason = "sem eixo temporal detetado"
    elif not diagnostics:
        reason = "sem séries numéricas temporais"
    else:
        reason = best.get("reason") or "apta para projeção exploratória curta"
    return {
        "dataset_id": payload.get("dataset_id"),
        "title": payload.get("title") or payload.get("dataset_id"),
        "temporal_field": payload.get("temporal_field"),
        "sample_size": payload.get("sample_size", 0),
        "total_records": payload.get("total_records", 0),
        "catalog_score": catalog_score,
        "catalog_reasons": catalog_reasons or [],
        "band": band,
        "eligible_count": len(eligible),
        "near_miss_count": len(near),
        "trend_count": len(diagnostics),
        "best_indicator": best.get("label") if best else None,
        "best_score": best.get("score") if best else 0,
        "best_points": best.get("points") if best else 0,
        "reason": reason,
        "gaps": (best.get("gaps") if best else [reason])[:3],
        "indicators": diagnostics[:4],
    }


def _build_predictive_recommendations(dataset_id: str | None, limit: int, candidate_limit: int) -> dict:
    cache_key = f"predictive:recommendations:{dataset_id or '-'}:{limit}:{candidate_limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    catalog = _get_analysis_catalog()
    datasets = catalog.get("datasets", []) or []
    scored = []
    for dataset in datasets:
        item_id = dataset.get("dataset_id")
        if not item_id:
            continue
        score, reasons = _predictive_catalog_score(dataset)
        if item_id == dataset_id or score >= 30:
            scored.append((dataset, score, reasons))
    scored.sort(
        key=lambda item: (
            item[0].get("dataset_id") != dataset_id,
            -item[1],
            -int(item[0].get("metric_candidate_count") or 0),
            -int(item[0].get("records_count") or 0),
            item[0].get("title") or item[0].get("dataset_id") or "",
        )
    )

    selected = scored[:candidate_limit]
    if dataset_id and not any(item[0].get("dataset_id") == dataset_id for item in selected):
        active = next((item for item in scored if item[0].get("dataset_id") == dataset_id), None)
        if active:
            selected = [active, *selected[: max(0, candidate_limit - 1)]]

    rows = []
    errors = []
    for dataset, catalog_score, reasons in selected:
        candidate_id = dataset.get("dataset_id")
        try:
            payload = _build_data_analytics(candidate_id, limit)
            fit = _predictive_payload_fit(payload, catalog_score, reasons)
        except Exception as exc:
            errors.append({"dataset_id": candidate_id, "error": type(exc).__name__})
            continue
        fit.update(
            {
                "dataset_id": candidate_id,
                "title": fit.get("title") or dataset.get("title") or candidate_id,
                "mega_theme": dataset.get("mega_theme"),
                "is_current": candidate_id == dataset_id,
            }
        )
        rows.append(fit)

    band_rank = {"ready": 0, "usable": 1, "near": 2, "blocked": 3}
    rows.sort(
        key=lambda row: (
            band_rank.get(row.get("band"), 9),
            -int(row.get("eligible_count") or 0),
            -int(row.get("best_score") or 0),
            0 if row.get("is_current") else 1,
            row.get("title") or "",
        )
    )
    active = next((row for row in rows if row.get("is_current")), None)
    result = {
        "active_dataset_id": dataset_id,
        "active": active,
        "recommendations": rows,
        "ready_count": sum(1 for row in rows if row.get("band") in {"ready", "usable"}),
        "near_count": sum(1 for row in rows if row.get("band") == "near"),
        "blocked_count": sum(1 for row in rows if row.get("band") == "blocked"),
        "candidate_count": len(selected),
        "errors": errors[:5],
        "methodology": {
            "version": ANALYTICS_METHOD_VERSION,
            "scope": "triagem de viabilidade preditiva por séries temporais da amostra",
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

    def _expensive_rate_limited(self, path: str) -> bool:
        if path not in EXPENSIVE_API_PATHS:
            return False
        retry_after = _route_rate_limit_retry_after(self.client_address[0], path)
        if retry_after is None:
            return False
        self.send_response(429)
        self.send_header("Retry-After", str(retry_after))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(b'{"error":"Too many expensive analysis requests"}')
        return True

    def _acquire_expensive_slot(self, path: str) -> bool:
        if path not in EXPENSIVE_API_PATHS:
            return False
        if _expensive_request_slots.acquire(blocking=False):
            return True
        self.send_response(503)
        self.send_header("Retry-After", "2")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(b'{"error":"Analysis queue is busy"}')
        return False

    def do_GET(self):
        if self._rate_limited():
            return

        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        expensive_slot_acquired = False

        try:
            if path.startswith("/api/"):
                if self._expensive_rate_limited(path):
                    return
                if path in EXPENSIVE_API_PATHS:
                    expensive_slot_acquired = self._acquire_expensive_slot(path)
                    if not expensive_slot_acquired:
                        return
                if path == "/api/health":
                    self._json(200, {"status": "ok"}, cache_control="no-store")
                    return

            if path == "/api/status":
                try:
                    catalog = _get_analysis_catalog()
                    payload = {
                        "status": "ok" if not catalog.get("fallback") else "degraded",
                        "catalog": {
                            "fallback": bool(catalog.get("fallback")),
                            "dataset_count": len(catalog.get("datasets", []) or []),
                            "link_count": len(catalog.get("links", []) or []),
                            "warning": catalog.get("warning"),
                            "error_kind": catalog.get("error_kind"),
                            "upstream_status": catalog.get("upstream_status"),
                            "upstream_path": catalog.get("upstream_path"),
                        },
                        "methodology": {
                            "version": ANALYTICS_METHOD_VERSION,
                            "source": catalog.get("methodology", {}).get("source") or "catalog_link_semantics",
                        },
                    }
                except Exception as exc:
                    fallback = _fallback_analysis_catalog(DEFAULT_MIN_SCORE, exc)
                    payload = {
                        "status": "degraded",
                        "catalog": {
                            "fallback": True,
                            "dataset_count": len(fallback.get("datasets", []) or []),
                            "link_count": len(fallback.get("links", []) or []),
                            "warning": fallback.get("warning"),
                            "error_kind": fallback.get("error_kind"),
                            "upstream_status": fallback.get("upstream_status"),
                            "upstream_path": fallback.get("upstream_path"),
                        },
                        "methodology": {
                            "version": ANALYTICS_METHOD_VERSION,
                            "source": "status_probe",
                        },
                    }
                self._json(200, payload, cache_control="no-store")
                return

            if path == "/api/analytics":
                try:
                    min_score = _parse_int_param((query.get("min_score") or [None])[0], 4, DEFAULT_MIN_SCORE, MAX_MIN_SCORE, "min_score")
                    payload = _build_analytics(_get_analysis_catalog(), min_score)
                    self._json(200, payload, cache_control="public, max-age=45")
                except Exception as exc:
                    if isinstance(exc, ValueError):
                        self._send_exception_json(exc)
                    else:
                        fallback_min_score = _parse_int_param(
                            (query.get("min_score") or [None])[0],
                            4,
                            DEFAULT_MIN_SCORE,
                            MAX_MIN_SCORE,
                            "min_score",
                        )
                        payload = _build_analytics(_fallback_analysis_catalog(fallback_min_score, exc), fallback_min_score)
                        self._json(200, payload, cache_control="no-store")
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

            if path == "/api/finprod/recommendations":
                try:
                    financial_dataset = _parse_dataset_id((query.get("financial_dataset") or [None])[0])
                    production_raw = (query.get("production_dataset") or [None])[0]
                    production_dataset = _parse_dataset_id(production_raw) if production_raw else None
                    limit = _parse_int_param(
                        (query.get("limit") or [None])[0],
                        DEFAULT_DATA_ANALYTICS_LIMIT,
                        20,
                        MAX_DATA_ANALYTICS_LIMIT,
                        "limit",
                    )
                    candidate_limit = _parse_int_param((query.get("candidate_limit") or [None])[0], 8, 2, 12, "candidate_limit")
                    payload = _build_finprod_recommendations(financial_dataset, production_dataset, limit, candidate_limit)
                    self._json(200, payload, cache_control="no-store")
                except Exception as exc:
                    self._send_exception_json(exc)
                return

            if path == "/api/predictive/recommendations":
                try:
                    dataset_raw = (query.get("dataset_id") or [None])[0]
                    dataset_id = _parse_dataset_id(dataset_raw) if dataset_raw else None
                    limit = _parse_int_param(
                        (query.get("limit") or [None])[0],
                        DEFAULT_DATA_ANALYTICS_LIMIT,
                        20,
                        MAX_DATA_ANALYTICS_LIMIT,
                        "limit",
                    )
                    candidate_limit = _parse_int_param((query.get("candidate_limit") or [None])[0], 8, 2, 12, "candidate_limit")
                    payload = _build_predictive_recommendations(dataset_id, limit, candidate_limit)
                    self._json(200, payload, cache_control="no-store")
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

            if path == "/api/deep-research":
                try:
                    dataset_id = _parse_dataset_id((query.get("dataset_id") or [None])[0])
                    limit = _parse_int_param((query.get("limit") or [None])[0], 100, 20, 200, "limit")
                    analysis_payload = copy.deepcopy(_build_data_analytics(dataset_id, limit))
                    feature_screening = _build_boruta_selection(analysis_payload)
                    territorial_map = _build_territorial_map(analysis_payload)
                    payload = {
                        "dataset_id": analysis_payload.get("dataset_id"),
                        "title": analysis_payload.get("title"),
                        "analysis": analysis_payload,
                        "feature_screening": feature_screening,
                        "boruta": feature_screening,
                        "territorial_map": territorial_map,
                        "methodology": {
                            "version": ANALYTICS_METHOD_VERSION,
                            "source": "deep_research_sample",
                            "feature_screening": feature_screening.get("method"),
                            "copy_safe": True,
                        },
                        "quality_warnings": analysis_payload.get("quality_warnings", []),
                        "generated_at": int(_now()),
                    }
                    self._json(200, payload, cache_control="no-store")
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
                        "facet_counts": cached.get("facet_counts", {}),
                        "fallback": bool(cached.get("fallback")),
                        "warning": cached.get("warning"),
                        "empty_reason": cached.get("empty_reason"),
                        "error_kind": cached.get("error_kind"),
                        "upstream_status": cached.get("upstream_status"),
                        "upstream_path": cached.get("upstream_path"),
                        "methodology": {
                            "version": ANALYTICS_METHOD_VERSION,
                            "source": "catalog_link_semantics",
                            "fallback": bool(cached.get("fallback")),
                            "error_kind": cached.get("error_kind"),
                            "upstream_status": cached.get("upstream_status"),
                        },
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
                    if isinstance(exc, ValueError):
                        self._send_exception_json(exc)
                    else:
                        fallback_min_score = _parse_int_param(
                            (query.get("min_score") or [None])[0],
                            DEFAULT_MIN_SCORE,
                            DEFAULT_MIN_SCORE,
                            MAX_MIN_SCORE,
                            "min_score",
                        )
                        self._json(200, _fallback_analysis_catalog(fallback_min_score, exc), cache_control="no-store")
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

            if path.startswith("/api/"):
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
        finally:
            if expensive_slot_acquired:
                _expensive_request_slots.release()

    def list_directory(self, path):
        self.send_error(404, "Directory listing disabled")
        return None


def run() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8000), TransparenciaHandler)
    print("http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
