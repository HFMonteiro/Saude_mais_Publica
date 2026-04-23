#!/usr/bin/env python3
"""
Transparent local explorer for the Opendatasoft Explore API used by
transparencia.sns.gov.pt.

This server serves a static frontend and exposes API helper endpoints with a local
proxy so the browser does not hit CORS issues.
"""

from __future__ import annotations

import json
import re
import threading
import time
import unicodedata
from datetime import date, datetime
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlparse, urlencode
from urllib.request import Request, urlopen

ODS_BASE = "https://transparencia.sns.gov.pt/api/explore/v2.1"
CACHE_TTL_SECONDS = 60 * 5
MAX_CACHE_ENTRIES = 8
DEFAULT_DATASET_LIMIT = 500
MAX_DATASET_LIMIT = 100
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


_cache: dict[str, tuple[float, dict]] = {}
_cache_lock = threading.Lock()


def _now() -> float:
    return time.time()


def _cache_get(key: str):
    with _cache_lock:
        entry = _cache.get(key)
        if not entry:
            return None
        timestamp, payload = entry
        if _now() - timestamp > CACHE_TTL_SECONDS:
            del _cache[key]
            return None
        return payload


def _cache_set(key: str, payload: dict):
    with _cache_lock:
        if len(_cache) >= MAX_CACHE_ENTRIES:
            oldest_key = sorted(_cache.items(), key=lambda item: item[1][0])[0][0]
            del _cache[oldest_key]
        _cache[key] = (_now(), payload)


def _normalize_token(value: str) -> str:
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(value.split())


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


def _build_ods_url(path: str, params: dict[str, str] | None = None) -> str:
    query = ""
    if params:
        normalized = {key: str(value) for key, value in params.items()}
        query = "?" + urlencode(normalized, doseq=True)
    return f"{ODS_BASE}{path}{query}"


def _ods_fetch(path: str, params: dict[str, str] | None = None) -> dict:
    cache_key = path + "?" + urlencode(params or {}, doseq=True)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    request = Request(_build_ods_url(path, params))
    request.add_header("Accept", "application/json")
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
            _cache_set(cache_key, payload)
            return payload
    except HTTPError as exc:
        message = exc.read().decode("utf-8", errors="ignore") if exc.fp else str(exc)
        raise RuntimeError(f"ODS error on {path}: {message}") from exc
    except URLError as exc:
        raise RuntimeError(f"Cannot reach ODS API: {exc}") from exc


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
                "select": "dataset_id,fields",
            },
        )
        results = page.get("results", []) or []
        all_results.extend(results)
        if total_count is None:
            total_count = page.get("total_count")
        if not results or (total_count is not None and len(all_results) >= total_count):
            break
        offset += len(results)

    if total_count is None:
        return {"total_count": len(all_results), "results": all_results}
    return {"total_count": total_count, "results": all_results}


def _analyze_datasets(datasets_payload: dict) -> dict:
    results = datasets_payload.get("results", []) or []
    items = []
    key_to_datasets: dict[str, list[str]] = {}
    dataset_id_to_fields: dict[str, list[str]] = {}

    for entry in results:
        dataset_id = entry.get("dataset_id") or ""
        if not dataset_id:
            continue

        metas = entry.get("metas", {}) or {}
        title = _get_dataset_title(metas).strip() or dataset_id
        records_count = ((metas.get("default") or {}).get("records_count") or 0)

        field_names = []
        canonical_keys: set[str] = set()
        for field in entry.get("fields", []) or []:
            field_name = field.get("name") or ""
            field_label = field.get("label") or ""
            if not field_name:
                continue
            field_names.append(field_name)
            for token in _field_tokens({"name": field_name, "label": field_label}):
                canonical_keys.add(token)
                key_to_datasets.setdefault(token, []).append(dataset_id)

        dataset_id_to_fields[dataset_id] = sorted(canonical_keys)
        mega_theme = _classify_dataset(dataset_id, title, field_names)
        items.append(
            {
                "dataset_id": dataset_id,
                "title": title,
                "mega_theme": mega_theme,
                "records_count": records_count,
                "field_count": len(field_names),
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
                    "shared_fields": shared[:12],
                }
            )

    opportunities = []
    for key, ds_list in key_to_datasets.items():
        unique_datasets = sorted(set(ds_list))
        if len(unique_datasets) < 2:
            continue
        opportunities.append(
            {
                "key": key,
                "dataset_ids": unique_datasets,
                "dataset_count": len(unique_datasets),
            }
        )

    opportunities.sort(key=lambda item: (-item["dataset_count"], item["key"]))

    links.sort(key=lambda item: (-item["score"], item["source"], item["target"]))
    theme_counts: dict[str, int] = {}
    theme_link_scores: dict[str, int] = {}
    for item in items:
        theme_counts[item["mega_theme"]] = theme_counts.get(item["mega_theme"], 0) + 1
    for link in links:
        source_theme = next((item["mega_theme"] for item in items if item["dataset_id"] == link["source"]), DEFAULT_THEME)
        target_theme = next((item["mega_theme"] for item in items if item["dataset_id"] == link["target"]), DEFAULT_THEME)
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


def _recent_records(dataset_id: str, limit: int) -> dict:
    dataset = _ods_fetch(f"/catalog/datasets/{quote(dataset_id)}", None)
    fields = dataset.get("fields", []) or []
    temporal_field = _pick_temporal_field(fields)
    min_year = date.today().year - 2
    params = {"limit": str(min(max(limit, 1), 100))}
    if temporal_field:
        params["order_by"] = f"{temporal_field} desc"

    try:
        payload = _ods_fetch(f"/catalog/datasets/{quote(dataset_id)}/records", params)
    except Exception:
        payload = _ods_fetch(f"/catalog/datasets/{quote(dataset_id)}/records", {"limit": str(min(max(limit, 1), 100))})

    records = payload.get("results", []) or []
    recent = [record for record in records if _is_recent_record(record, temporal_field, min_year)]
    if not recent:
        recent = records[: min(len(records), limit)]

    columns = []
    for field in fields:
        name = field.get("name")
        if name and any(name in record for record in recent[:20]):
            columns.append({"name": name, "label": field.get("label") or name, "type": field.get("type")})
        if len(columns) >= 12:
            break

    return {
        "dataset_id": dataset_id,
        "temporal_field": temporal_field,
        "min_year": min_year,
        "columns": columns,
        "records": recent[:limit],
        "returned_count": len(recent[:limit]),
        "source_count": len(records),
    }


class TransparenciaHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, message: str):
        self._json(status, {"error": message})

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path.startswith("/api/"):
            if path == "/api/health":
                self._json(200, {"status": "ok"})
                return

            if path == "/api/datasets":
                try:
                    raw_limit = int((query.get("limit") or [DEFAULT_DATASET_LIMIT])[0])
                    limit = min(max(raw_limit, 1), 100)
                    payload = _ods_fetch("/catalog/datasets", {"limit": limit})
                    self._json(200, payload)
                except Exception as exc:
                    self._send_error_json(502, str(exc))
                return

            if path == "/api/analysis":
                min_score = int((query.get("min_score") or ["1"])[0])
                try:
                    cache_key = f"analysis|min={min_score}"
                    cached = _cache_get(cache_key)
                    if cached is None:
                        catalog = _get_datasets()
                        cached = _analyze_datasets(catalog)
                        _cache_set(cache_key, cached)
                    filtered = {
                        "datasets": cached["datasets"],
                        "opportunities": cached["opportunities"],
                        "themes": cached.get("themes", []),
                        "generated_at": cached["generated_at"],
                    }
                    if min_score > 1:
                        filtered["links"] = [l for l in cached["links"] if l["score"] >= min_score]
                    else:
                        filtered["links"] = cached["links"]
                    filtered["total"] = len(filtered["datasets"])
                    filtered["link_count"] = len(filtered["links"])
                    self._json(200, filtered)
                except Exception as exc:
                    self._send_error_json(502, str(exc))
                return

            if path.startswith("/api/dataset/"):
                dataset_id = unquote(path.split("/api/dataset/", 1)[1])
                if not dataset_id:
                    self._send_error_json(400, "dataset_id required")
                    return
                try:
                    dataset = _ods_fetch(f"/catalog/datasets/{quote(dataset_id)}", None)
                    self._json(200, dataset)
                except Exception as exc:
                    self._send_error_json(502, str(exc))
                return

            if path.startswith("/api/records/"):
                dataset_id = unquote(path.split("/api/records/", 1)[1])
                if not dataset_id:
                    self._send_error_json(400, "dataset_id required")
                    return
                dataset_id = quote(dataset_id)
                limit = str((query.get("limit") or ["50"])[0])
                try:
                    records = _ods_fetch(
                        f"/catalog/datasets/{dataset_id}/records",
                        {"limit": limit},
                    )
                    self._json(200, records)
                except Exception as exc:
                    self._send_error_json(502, str(exc))
                return

            if path.startswith("/api/recent/"):
                dataset_id = unquote(path.split("/api/recent/", 1)[1])
                if not dataset_id:
                    self._send_error_json(400, "dataset_id required")
                    return
                limit = int((query.get("limit") or ["60"])[0])
                try:
                    self._json(200, _recent_records(dataset_id, limit))
                except Exception as exc:
                    self._send_error_json(502, str(exc))
                return

            self._send_error_json(404, f"Unknown API route: {path}")
            return

        return super().do_GET()


def run() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8000), TransparenciaHandler)
    print("http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()
