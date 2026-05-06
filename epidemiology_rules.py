"""Formal, conservative epidemiology review rules for exploratory analytics.

The module is intentionally standalone: server.py provides samples and metadata,
this file classifies the analytical context and returns auditable review objects.
"""

from __future__ import annotations

import math
import re
import unicodedata
from statistics import mean
from typing import Any


ONTOLOGY_VERSION = "2026-05-06.internal-epi-ontology-v1"

STATUS_LABELS = {
    "pronto": "Pronto",
    "rever": "Rever",
    "fragil": "Frágil",
}

ANALYTICAL_INTENTS = {
    "triagem_exploratoria": "Triagem exploratória",
    "vigilancia_temporal": "Vigilância temporal",
    "custo_unitario": "Custo unitário exploratório",
    "comparacao_cruzada": "Comparação cruzada",
}

SURVEILLANCE_DOMAINS = {
    "financeiro": "Financeiro",
    "atividade_assistencial": "Atividade assistencial",
    "cobertura_populacional": "Cobertura populacional",
    "eventos_raros": "Eventos raros",
    "capacidade_recursos": "Capacidade / recursos",
    "territorial_agregado": "Territorial agregado",
    "administrativo_generico": "Administrativo genérico",
}

OBSERVATION_UNITS = {
    "episodio": "Episódio",
    "entidade": "Entidade",
    "territorio": "Território",
    "periodo": "Período agregado",
    "agregado": "Agregado administrativo",
    "por_validar": "Unidade por validar",
}

POPULATION_BASES = {
    "populacao_exposta": "População exposta",
    "utentes_inscritos": "Utentes inscritos",
    "atividade_observada": "Atividade observada",
    "base_contabilistica": "Base contabilística",
    "stock_capacidade": "Stock/capacidade",
    "por_validar": "Denominador por validar",
}

MEASURE_CONSTRUCTS = {
    "valor_monetario": "Valor monetário",
    "contagem_eventos": "Contagem de eventos",
    "taxa_percentagem": "Taxa/percentagem",
    "stock_capacidade": "Stock/capacidade",
    "indice_proxy": "Índice/proxy",
    "desconhecido": "Medida por validar",
}

DENOMINATOR_STATUSES = {
    "validado": "Denominador validado",
    "sensivel": "Denominador sensível",
    "fragil": "Denominador frágil",
    "ausente": "Denominador ausente",
}

TIME_AXIS_TYPES = {
    "mensal": "Mensal provável",
    "trimestral": "Trimestral provável",
    "anual": "Anual provável",
    "desconhecida": "Periodicidade por validar",
    "ausente": "Sem eixo temporal",
}

ZERO_MEANINGS = {
    "sem_zeros": "Sem zeros relevantes",
    "zero_plausivel": "Zero plausível",
    "zero_suspeito": "Zero suspeito",
    "zero_contabilistico": "Zero financeiro por validar",
    "silencio_reporte": "Possível silêncio de reporte",
    "sem_serie": "Sem série para interpretar zeros",
}

REPORTING_PROCESSES = {
    "sem_sinal": "Sem atraso visível",
    "amostra_recente_parcial": "Amostra recente parcial",
    "queda_densidade": "Queda recente de densidade",
    "sem_serie": "Sem base temporal",
}

COMPARABILITY_STATES = {
    "comparavel": "Comparável para triagem",
    "por_validar": "Comparabilidade por validar",
    "incompativel": "Comparabilidade frágil",
}

EVIDENCE_GRADES = {
    "A": "Evidência exploratória forte",
    "B": "Evidência exploratória moderada",
    "C": "Evidência exploratória limitada",
    "D": "Evidência frágil",
}

RULE_CATALOG = [
    {
        "rule_id": "SURV-DOMAIN-001",
        "name": "Domínio financeiro",
        "domain": "measure",
        "condition": "Unidade monetária, despesa, orçamento, custo, valor financeiro ou tema financeiro.",
        "effect": "Classifica como financeiro.",
        "severity": "info",
        "explanation": "Valores financeiros exigem base contabilística e período de execução.",
    },
    {
        "rule_id": "SURV-DOMAIN-002",
        "name": "Domínio de atividade assistencial",
        "domain": "measure",
        "condition": "Consultas, urgência, internamento, produção, atividade, acesso ou tempo de espera.",
        "effect": "Classifica como atividade assistencial.",
        "severity": "info",
        "explanation": "Atividade assistencial é sensível a reporte operacional e caudas recentes.",
    },
    {
        "rule_id": "SURV-DOMAIN-003",
        "name": "Domínio de cobertura populacional",
        "domain": "denominator",
        "condition": "Taxa, percentagem, cobertura, inscritos, utentes ou população.",
        "effect": "Classifica como cobertura populacional.",
        "severity": "info",
        "explanation": "Taxas e coberturas dependem de denominador explícito.",
    },
    {
        "rule_id": "SURV-DOMAIN-004",
        "name": "Domínio de eventos raros",
        "domain": "measure",
        "condition": "Óbitos, mortalidade, infeção, incidente, emergência, surto ou evento.",
        "effect": "Classifica como eventos raros.",
        "severity": "info",
        "explanation": "Zeros podem ser plausíveis, mas a definição de caso tem de ser estável.",
    },
    {
        "rule_id": "SURV-DOMAIN-005",
        "name": "Domínio de capacidade e recursos",
        "domain": "measure",
        "condition": "Camas, profissionais, stock, equipamento, vagas ou capacidade.",
        "effect": "Classifica como capacidade/recursos.",
        "severity": "info",
        "explanation": "Capacidade mistura stock, entidade e disponibilidade operacional.",
    },
    {
        "rule_id": "OBS-UNIT-001",
        "name": "Unidade de observação frágil",
        "domain": "measure",
        "condition": "Sem pista de episódio, entidade, território, período ou agregado.",
        "effect": "Baixa para Frágil.",
        "severity": "error",
        "explanation": "Sem unidade de observação não há interpretação analítica defensável.",
    },
    {
        "rule_id": "DENOM-001",
        "name": "Denominador ausente em taxa",
        "domain": "denominator",
        "condition": "Métrica é taxa/percentagem sem campo de população ou base.",
        "effect": "Baixa denominador para Frágil.",
        "severity": "error",
        "explanation": "Taxas sem população/base não devem ser lidas como cobertura validada.",
    },
    {
        "rule_id": "DENOM-002",
        "name": "Custo unitário sem denominador produtivo",
        "domain": "denominator",
        "condition": "Denominador não é produção/atividade válida.",
        "effect": "Bloqueia destaque de custo unitário.",
        "severity": "error",
        "explanation": "Custo unitário exige denominador produtivo explícito e positivo.",
    },
    {
        "rule_id": "TIME-001",
        "name": "Menos de três períodos",
        "domain": "time",
        "condition": "Menos de três períodos válidos.",
        "effect": "Baixa temporalidade para Frágil.",
        "severity": "error",
        "explanation": "Menos de três períodos não sustentam leitura temporal.",
    },
    {
        "rule_id": "TIME-002",
        "name": "Poucos períodos ou baixa densidade",
        "domain": "time",
        "condition": "Três a cinco períodos ou baixa densidade por período.",
        "effect": "Baixa temporalidade para Rever.",
        "severity": "warning",
        "explanation": "A série é útil para triagem, não para tendência forte.",
    },
    {
        "rule_id": "ZERO-001",
        "name": "Zero em cobertura populacional",
        "domain": "zero",
        "condition": "Zero em taxa/cobertura sem evidência explícita de zero plausível.",
        "effect": "Baixa zeros para Frágil.",
        "severity": "error",
        "explanation": "Em cobertura populacional, zero costuma sinalizar denominador, reporte ou transformação.",
    },
    {
        "rule_id": "ZERO-002",
        "name": "Zero em eventos raros",
        "domain": "zero",
        "condition": "Zero em eventos raros, com cauda recente de zeros a rever.",
        "effect": "Mantém Pronto ou baixa para Rever.",
        "severity": "warning",
        "explanation": "Zeros podem ser reais em eventos raros, mas cauda recente pode ser atraso.",
    },
    {
        "rule_id": "ZERO-003",
        "name": "Zero em atividade depois de histórico ativo",
        "domain": "zero",
        "condition": "Zeros recentes depois de histórico ativo em atividade assistencial.",
        "effect": "Baixa zeros para Frágil.",
        "severity": "error",
        "explanation": "Pode representar ausência de reporte, não ausência real de atividade.",
    },
    {
        "rule_id": "ZERO-004",
        "name": "Zero financeiro",
        "domain": "zero",
        "condition": "Zero em série financeira.",
        "effect": "Baixa zeros para Rever.",
        "severity": "warning",
        "explanation": "Pode ser não execução, atraso contabilístico ou transformação da fonte.",
    },
    {
        "rule_id": "REPORT-001",
        "name": "Amostra recente parcial",
        "domain": "reporting",
        "condition": "Amostra ordenada por período recente e cobertura parcial.",
        "effect": "Baixa reporte para Rever.",
        "severity": "warning",
        "explanation": "A janela recente pode não representar a série completa.",
    },
    {
        "rule_id": "REPORT-002",
        "name": "Queda abrupta de densidade recente",
        "domain": "reporting",
        "condition": "Densidade dos últimos períodos cai face ao histórico.",
        "effect": "Baixa reporte para Frágil.",
        "severity": "error",
        "explanation": "Queda de registos pode ser cauda incompleta de reporte.",
    },
    {
        "rule_id": "GEO-001",
        "name": "Geografia ou entidade ausente",
        "domain": "geography",
        "condition": "Comparação territorial sem geografia/entidade explícita.",
        "effect": "Baixa geografia para Rever.",
        "severity": "warning",
        "explanation": "Comparações territoriais precisam de âmbito e entidade consistentes.",
    },
    {
        "rule_id": "COMP-001",
        "name": "Comparação cruzada incompatível",
        "domain": "comparability",
        "condition": "Unidade, período ou denominador divergem.",
        "effect": "Baixa comparabilidade para Frágil.",
        "severity": "error",
        "explanation": "Cruzamentos e rácios não devem ser promovidos com bases incompatíveis.",
    },
]

RULE_BY_ID = {rule["rule_id"]: rule for rule in RULE_CATALOG}


def normalize_token(value: Any) -> str:
    text = str(value or "").lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = re.sub(r"[^a-z0-9%]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _field_text(fields: list[dict] | list[str] | None) -> str:
    parts: list[str] = []
    for field in fields or []:
        if isinstance(field, dict):
            parts.extend([str(field.get("name") or ""), str(field.get("label") or ""), str(field.get("type") or "")])
        else:
            parts.append(str(field))
    return " ".join(parts)


def _status_item(status: str, label: str, detail: str, **extra: Any) -> dict:
    return {"status": status, "label": label, "detail": detail, **extra}


def _rule_trace(rule_id: str, input_signal: str, decision: str, status_delta: str = "neutro") -> dict:
    rule = RULE_BY_ID.get(rule_id, {})
    return {
        "rule_id": rule_id,
        "input_signal": input_signal,
        "decision": decision,
        "status_delta": status_delta,
        "explanation": rule.get("explanation") or decision,
    }


def _blocker(rule_id: str, code: str, label: str, severity: str, domain: str, action: str) -> dict:
    return {
        "code": code,
        "label": label,
        "severity": severity,
        "domain": domain,
        "action": action,
        "rule_id": rule_id,
    }


def _coerce_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        return number if math.isfinite(number) else None
    text = str(value).strip().replace(" ", "")
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _rule_status(severity: str) -> str:
    if severity == "error":
        return "fragil"
    if severity == "warning":
        return "rever"
    return "pronto"


def _combine_status(statuses: list[str]) -> str:
    if "fragil" in statuses:
        return "fragil"
    if "rever" in statuses:
        return "rever"
    return "pronto"


def _domain_alias(domain: dict) -> dict:
    code = domain.get("code") or "administrativo_generico"
    zero_policy = {
        "financeiro": "zero pode ser não execução, atraso contabilístico ou transformação",
        "atividade_assistencial": "zero recente depois de histórico ativo é suspeito",
        "cobertura_populacional": "zero é frágil salvo evidência explícita",
        "eventos_raros": "zero pode ser plausível com definição estável",
        "capacidade_recursos": "zero pode representar ausência estrutural ou erro",
        "territorial_agregado": "zero depende da regra de agregação territorial",
    }.get(code, "zero por validar")
    lag_sensitivity = "alta" if code in {"atividade_assistencial", "cobertura_populacional"} else "media"
    return {
        "code": code,
        "label": domain.get("label") or SURVEILLANCE_DOMAINS.get(code, "Administrativo genérico"),
        "zero_policy": zero_policy,
        "lag_sensitivity": lag_sensitivity,
        "review_focus": domain.get("detail") or "unidade, denominador, período e reporte",
        "signals": domain.get("signals", [])[:5],
    }


def classify_surveillance_domain(
    *,
    dataset_id: str = "",
    dataset_title: str = "",
    mega_theme: str = "",
    fields: list[dict] | list[str] | None = None,
    numeric_profiles: list[dict] | None = None,
) -> tuple[dict, list[dict]]:
    roles = {normalize_token(profile.get("measure_role") or "") for profile in numeric_profiles or []}
    labels = " ".join(str(profile.get("label") or profile.get("field") or "") for profile in numeric_profiles or [])
    text = normalize_token(" ".join([dataset_id, dataset_title, mega_theme, _field_text(fields), " ".join(roles), labels]))

    checks = [
        (
            "SURV-DOMAIN-001",
            "financeiro",
            r"\b(despesa|orcamento|custo|valor financeiro|financeiro|pagamento|compra|conta|euro|eur)\b",
            "monetario" in roles or "financas" in normalize_token(mega_theme),
            ["valor monetário", "execução", "contabilidade"],
        ),
        (
            "SURV-DOMAIN-003",
            "cobertura_populacional",
            r"\b(taxa|percentagem|percentual|cobertura|inscritos|utente|utentes|populacao|racio|ratio)\b",
            "taxa" in roles,
            ["taxa", "denominador", "população"],
        ),
        (
            "SURV-DOMAIN-005",
            "capacidade_recursos",
            r"\b(cama|camas|profissional|profissionais|medico|enfermeiro|stock|equipamento|vaga|vagas|capacidade|recurso|recursos)\b",
            False,
            ["recursos", "capacidade", "stock"],
        ),
        (
            "SURV-DOMAIN-004",
            "eventos_raros",
            r"\b(obito|obitos|mortalidade|infeccao|infecao|incidente|incidencia|emergencia|surto|evento|eventos)\b",
            False,
            ["evento raro", "definição de caso", "janela"],
        ),
        (
            "SURV-DOMAIN-002",
            "atividade_assistencial",
            r"\b(consulta|consultas|urgencia|internamento|episodio|atividade|actividade|producao|acesso|espera|tempo de espera)\b",
            False,
            ["produção", "atividade", "acesso"],
        ),
    ]
    has_activity_signal = bool(re.search(r"\b(consulta|consultas|urgencia|internamento|episodio|atividade|actividade|producao|acesso|espera|tempo de espera)\b", text))
    has_rate_signal = bool(re.search(r"\b(taxa|percentagem|percentual|cobertura|racio|ratio)\b", text) or "taxa" in roles)
    for rule_id, code, pattern, role_match, signals in checks:
        if code == "cobertura_populacional" and has_activity_signal and not has_rate_signal:
            continue
        if role_match or re.search(pattern, text):
            domain = {
                "code": code,
                "label": SURVEILLANCE_DOMAINS[code],
                "status": "pronto",
                "detail": _domain_detail(code),
                "signals": signals,
                "rule_id": rule_id,
            }
            return domain, [_rule_trace(rule_id, signals[0], f"Classificado como {SURVEILLANCE_DOMAINS[code]}.")]

    if re.search(r"\b(regiao|ars|uls|concelho|distrito|municipio|territorio|entidade|unidade)\b", text):
        return (
            {
                "code": "territorial_agregado",
                "label": SURVEILLANCE_DOMAINS["territorial_agregado"],
                "status": "rever",
                "detail": "Leitura depende da estabilidade da geografia, entidade e agregação.",
                "signals": ["território", "entidade"],
                "rule_id": "GEO-001",
            },
            [_rule_trace("GEO-001", "pista territorial", "Classificado como territorial agregado.", "rever")],
        )

    return (
        {
            "code": "administrativo_generico",
            "label": SURVEILLANCE_DOMAINS["administrativo_generico"],
            "status": "rever",
            "detail": "Domínio por validar a partir de metadados e campos.",
            "signals": ["metadados insuficientes"],
            "rule_id": "SURV-DOMAIN-DEFAULT",
        },
        [_rule_trace("SURV-DOMAIN-DEFAULT", "sem regra específica", "Classificado como administrativo genérico.", "rever")],
    )


def _domain_detail(code: str) -> str:
    return {
        "financeiro": "Validar base contabilística, período e unidade monetária antes de interpretar.",
        "atividade_assistencial": "Validar atividade, episódio, período e reporte operacional.",
        "cobertura_populacional": "Validar população exposta, denominador e transformação da taxa.",
        "eventos_raros": "Validar definição de caso, janela de observação e sub-reporte.",
        "capacidade_recursos": "Validar stock, capacidade, entidade e disponibilidade operacional.",
    }.get(code, "Validar domínio antes de interpretar.")


def _extract_values(temporal_points: list[dict] | None) -> list[float]:
    values = []
    for point in temporal_points or []:
        value = _coerce_number(point.get("value"))
        if value is None:
            value = _coerce_number(point.get("avg"))
        if value is None:
            value = _coerce_number(point.get("sum"))
        if value is not None:
            values.append(value)
    return values


def _extract_counts(temporal_points: list[dict] | None) -> list[int]:
    counts = []
    for point in temporal_points or []:
        count = _coerce_number(point.get("count"))
        counts.append(max(1, int(count)) if count is not None else 1)
    return counts


def _infer_periodicity(temporal_points: list[dict], temporal_field: str | None) -> dict:
    labels = [str(point.get("period") or "") for point in temporal_points if point.get("period")]
    field_text = normalize_token(temporal_field or "")
    if labels and all(re.fullmatch(r"\d{4}-\d{2}", label) for label in labels):
        code = "mensal"
    elif labels and all(re.fullmatch(r"\d{4}-T[1-4]", label) for label in labels):
        code = "trimestral"
    elif labels and all(re.fullmatch(r"\d{4}", label) for label in labels):
        code = "anual"
    elif re.search(r"\b(mes|mensal)\b", field_text):
        code = "mensal"
    elif re.search(r"\b(trimestre|trimestral)\b", field_text):
        code = "trimestral"
    elif re.search(r"\b(ano|anual)\b", field_text):
        code = "anual"
    elif temporal_field:
        code = "desconhecida"
    else:
        code = "ausente"
    return {"code": code, "label": TIME_AXIS_TYPES[code]}


def evaluate_observation_unit(
    *,
    fields: list[dict] | list[str] | None = None,
    temporal_field: str | None = None,
    numeric_profiles: list[dict] | None = None,
    categorical_profiles: list[dict] | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    text = normalize_token(_field_text(fields))
    local_fields = re.search(r"\b(regiao|ars|uls|hospital|concelho|distrito|entidade|unidade|localizacao)\b", text)
    episode_fields = re.search(r"\b(episodio|consulta|urgencia|internamento|evento|id)\b", text)
    blockers: list[dict] = []
    trace: list[dict] = []
    if episode_fields:
        return _status_item("pronto", "episódio/agregado assistencial", "Há pistas de episódio, evento ou atividade observada.", code="episodio"), blockers, trace
    if local_fields:
        return _status_item("pronto", "agregado territorial/entidade", "Há dimensão territorial ou entidade explícita.", code="territorio"), blockers, trace
    if temporal_field:
        return _status_item("pronto", "agregado por período", "Há eixo temporal para agregação da amostra.", code="periodo"), blockers, trace
    if numeric_profiles or categorical_profiles:
        return _status_item("rever", "agregado administrativo", "Há campos analisáveis, mas a unidade da linha deve ser confirmada.", code="agregado"), blockers, trace

    blockers.append(_blocker("OBS-UNIT-001", "observation_unit_unclear", "unidade de observação por validar", "error", "measure", "confirmar se a linha representa episódio, entidade, território, período ou agregado"))
    trace.append(_rule_trace("OBS-UNIT-001", "sem pistas de unidade", "Unidade observada classificada como frágil.", "fragil"))
    return _status_item("fragil", "unidade por validar", "Sem pista de episódio, entidade, território, período ou agregado.", code="por_validar"), blockers, trace


def evaluate_measure_construct(numeric_profiles: list[dict] | None = None) -> dict:
    roles = [normalize_token(profile.get("measure_role") or "") for profile in numeric_profiles or []]
    labels = " ".join(str(profile.get("label") or profile.get("field") or "") for profile in numeric_profiles or [])
    text = normalize_token(labels)
    if "monetario" in roles or re.search(r"\b(despesa|custo|valor|orcamento|pagamento)\b", text):
        return _status_item("pronto", MEASURE_CONSTRUCTS["valor_monetario"], "Medida monetária provável.", code="valor_monetario")
    if "taxa" in roles or re.search(r"\b(taxa|percentagem|cobertura|%)\b", text):
        return _status_item("rever", MEASURE_CONSTRUCTS["taxa_percentagem"], "Taxa/percentagem exige denominador e população exposta.", code="taxa_percentagem")
    if "stock" in roles or re.search(r"\b(stock|camas|capacidade|vagas)\b", text):
        return _status_item("rever", MEASURE_CONSTRUCTS["stock_capacidade"], "Stock/capacidade exige entidade e data de referência.", code="stock_capacidade")
    if "contagem" in roles or numeric_profiles:
        return _status_item("pronto", MEASURE_CONSTRUCTS["contagem_eventos"], "Contagem/volume provável.", code="contagem_eventos")
    return _status_item("fragil", MEASURE_CONSTRUCTS["desconhecido"], "Sem medida quantitativa suficientemente clara.", code="desconhecido")


def evaluate_denominator(
    *,
    surveillance_domain: dict,
    measure_construct: dict,
    quality_summary: dict | None = None,
    fields: list[dict] | list[str] | None = None,
) -> tuple[dict, dict, list[dict], list[dict]]:
    quality_summary = quality_summary or {}
    denominator_quality = quality_summary.get("denominator") or {}
    field_text = normalize_token(_field_text(fields))
    domain_code = surveillance_domain.get("code")
    measure_code = measure_construct.get("code")
    has_population_basis = bool(re.search(r"\b(populacao|utente|utentes|inscritos|base|denominador|total)\b", field_text))
    blockers: list[dict] = []
    trace: list[dict] = []

    if measure_code == "taxa_percentagem" and not has_population_basis:
        blockers.append(_blocker("DENOM-001", "denominator_missing_for_rate", "taxa sem população/base explícita", "error", "denominator", "identificar denominador, população exposta ou base da percentagem"))
        trace.append(_rule_trace("DENOM-001", "taxa sem campo de população/base", "Denominador classificado como frágil.", "fragil"))
        denominator = _status_item("fragil", DENOMINATOR_STATUSES["fragil"], "Métrica de taxa/percentagem sem denominador explícito.", code="fragil")
    elif domain_code == "financeiro":
        denominator = _status_item("rever", DENOMINATOR_STATUSES["sensivel"], "Leitura financeira depende de base contabilística, unidade e período.", code="sensivel")
    elif domain_code == "cobertura_populacional":
        status = "pronto" if has_population_basis else "rever"
        denominator = _status_item(status, DENOMINATOR_STATUSES["validado" if status == "pronto" else "sensivel"], "População/base detetada nos campos." if status == "pronto" else "Confirmar população exposta e denominador.", code="validado" if status == "pronto" else "sensivel")
    else:
        status = denominator_quality.get("status") or ("pronto" if measure_code == "contagem_eventos" else "rever")
        label = denominator_quality.get("label") or ("contagem simples" if status == "pronto" else "denominador por validar")
        denominator = _status_item(status, label, "Confirmar unidade e denominador antes de comparar rácios.", code="validado" if status == "pronto" else "sensivel")

    population_basis_code = {
        "financeiro": "base_contabilistica",
        "atividade_assistencial": "atividade_observada",
        "cobertura_populacional": "utentes_inscritos" if has_population_basis else "por_validar",
        "eventos_raros": "populacao_exposta" if has_population_basis else "por_validar",
        "capacidade_recursos": "stock_capacidade",
    }.get(domain_code, "por_validar" if denominator["status"] != "pronto" else "atividade_observada")
    population_basis = _status_item(
        denominator["status"],
        POPULATION_BASES[population_basis_code],
        denominator["detail"],
        code=population_basis_code,
    )
    return denominator, population_basis, blockers, trace


def evaluate_time_axis(
    *,
    temporal_field: str | None,
    temporal_points: list[dict] | None,
    surveillance_domain: dict | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    temporal_points = temporal_points or []
    counts = _extract_counts(temporal_points)
    valid_periods = len(temporal_points)
    min_records = min(counts, default=0)
    periodicity = _infer_periodicity(temporal_points, temporal_field)
    domain_code = (surveillance_domain or {}).get("code") or "administrativo_generico"
    aggregate_density_floor = 1 if domain_code in {
        "financeiro",
        "atividade_assistencial",
        "cobertura_populacional",
        "eventos_raros",
        "capacidade_recursos",
        "territorial_agregado",
    } else 3
    blockers: list[dict] = []
    trace: list[dict] = []
    if valid_periods < 3:
        blockers.append(_blocker("TIME-001", "time_axis_too_short", "menos de 3 períodos válidos", "error", "time", "obter pelo menos 3 períodos comparáveis antes de ler evolução"))
        trace.append(_rule_trace("TIME-001", f"{valid_periods} períodos", "Temporalidade classificada como frágil.", "fragil"))
        status = "fragil"
        label = f"{valid_periods} períodos válidos" if valid_periods else "sem períodos válidos"
        detail = "Menos de três períodos não sustentam leitura temporal."
    elif valid_periods <= 5 or min_records < aggregate_density_floor:
        blockers.append(_blocker("TIME-002", "time_axis_low_density", "poucos períodos ou baixa densidade", "warning", "time", "tratar como vigilância curta e validar densidade por período"))
        trace.append(_rule_trace("TIME-002", f"{valid_periods} períodos; mínimo {min_records} registos", "Temporalidade classificada como a rever.", "rever"))
        status = "rever"
        label = f"{valid_periods} períodos · baixa densidade"
        detail = "A série é curta ou tem poucos registos por período."
    else:
        status = "pronto"
        label = f"{valid_periods} períodos válidos"
        detail = "Há períodos suficientes para vigilância temporal exploratória."
    return _status_item(status, label, detail, valid_periods=valid_periods, min_records_per_period=min_records, density_floor=aggregate_density_floor, periodicity=periodicity), blockers, trace


def evaluate_zero_meaning(
    *,
    surveillance_domain: dict,
    temporal_points: list[dict] | None,
    values: list[float] | None = None,
    ordering: str = "api_default",
) -> tuple[dict, list[dict], list[dict]]:
    temporal_points = temporal_points or []
    values = list(values if values is not None else _extract_values(temporal_points))
    zero_count = sum(1 for value in values if value == 0)
    active_count = sum(1 for value in values if value > 0)
    zero_tail = 0
    for value in reversed(values):
        if value == 0:
            zero_tail += 1
        else:
            break
    total = len(values)
    zero_ratio = zero_count / total if total else 0
    domain_code = surveillance_domain.get("code") or "administrativo_generico"
    blockers: list[dict] = []
    trace: list[dict] = []

    common = {
        "zero_periods": zero_count,
        "active_periods": active_count,
        "zero_tail": zero_tail,
        "zero_ratio": round(zero_ratio, 4),
    }
    if not temporal_points or not values:
        return _status_item("fragil", ZERO_MEANINGS["sem_serie"], "Não é possível interpretar zeros sem série válida.", code="sem_serie", reason_code="no_series", interpretation="zero por validar", **common), blockers, trace
    if zero_count == 0:
        return _status_item("pronto", ZERO_MEANINGS["sem_zeros"], "A série não sugere silêncio de reporte por zeros.", code="sem_zeros", reason_code="no_relevant_zeros", interpretation="sem zeros relevantes", **common), blockers, trace

    if domain_code == "cobertura_populacional":
        blockers.append(_blocker("ZERO-001", "zero_population_coverage", "zeros em cobertura populacional", "error", "zero", "confirmar denominador, transformação e reporte antes de interpretar cobertura"))
        trace.append(_rule_trace("ZERO-001", f"{zero_count} zeros; razão {zero_ratio:.2f}", "Zero em cobertura populacional classificado como frágil.", "fragil"))
        return _status_item("fragil", f"{zero_count} zeros suspeitos", "Em taxas/coberturas populacionais, zero tende a indicar denominador, reporte ou transformação por validar.", code="zero_suspeito", reason_code="population_rate_zero_suspect", interpretation="zero frágil salvo evidência explícita", **common), blockers, trace

    if domain_code == "eventos_raros":
        if zero_tail >= 3 and ordering == "temporal_desc":
            blockers.append(_blocker("ZERO-002", "rare_event_zero_tail", "cauda recente de zeros em eventos raros", "warning", "zero", "confirmar atualização e definição de caso nos períodos recentes"))
            trace.append(_rule_trace("ZERO-002", f"cauda de {zero_tail} zeros", "Zero raro plausível, mas cauda recente fica a rever.", "rever"))
            return _status_item("rever", f"{zero_count} zeros plausíveis", "Zeros podem ser compatíveis com eventos raros; cauda recente exige confirmação de reporte.", code="zero_plausivel", reason_code="rare_event_recent_zero_tail", interpretation="zero plausível com caveat", **common), blockers, trace
        trace.append(_rule_trace("ZERO-002", f"{zero_count} zeros", "Zero em evento raro considerado plausível por defeito.", "neutro"))
        return _status_item("pronto", f"{zero_count} zeros plausíveis", "Zeros podem ser compatíveis com eventos raros; confirmar definição de caso e sub-reporte.", code="zero_plausivel", reason_code="rare_event_zero_plausible", interpretation="zero plausível", **common), blockers, trace

    if domain_code == "atividade_assistencial" and (zero_tail >= 2 or (active_count >= 2 and zero_ratio >= 0.4)):
        blockers.append(_blocker("ZERO-003", "activity_recent_zero_tail", "zeros recentes em atividade", "error", "zero", "validar reporte operacional antes de desenhar tendência contínua"))
        trace.append(_rule_trace("ZERO-003", f"cauda {zero_tail}; ativos {active_count}", "Zero em atividade depois de histórico ativo classificado como frágil.", "fragil"))
        return _status_item("fragil", f"{zero_count} zeros na atividade", "Em atividade assistencial, zeros recentes depois de períodos ativos sugerem reporte incompleto.", code="silencio_reporte", reason_code="activity_zero_reporting_risk", interpretation="zero pode ser silêncio de reporte", **common), blockers, trace

    if domain_code == "financeiro":
        blockers.append(_blocker("ZERO-004", "financial_zero_review", "zeros financeiros por validar", "warning", "zero", "confirmar se zero é não execução, atraso contabilístico ou transformação"))
        trace.append(_rule_trace("ZERO-004", f"{zero_count} zeros financeiros", "Zero financeiro classificado como a rever.", "rever"))
        return _status_item("rever", f"{zero_count} zeros financeiros", "Zeros financeiros podem representar não execução, atraso contabilístico ou transformação da fonte.", code="zero_contabilistico", reason_code="financial_zero_accounting_risk", interpretation="zero contabilístico por validar", **common), blockers, trace

    status = "rever" if zero_count else "pronto"
    return _status_item(status, f"{zero_count} zeros por validar", "Validar se zero significa ausência real, ausência de reporte ou valor imputado.", code="zero_suspeito", reason_code="generic_zero_unknown", interpretation="zero por validar", **common), blockers, trace


def evaluate_reporting_process(
    *,
    surveillance_domain: dict,
    ordering: str,
    temporal_points: list[dict] | None,
    values: list[float] | None = None,
    coverage_ratio: float | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    temporal_points = temporal_points or []
    counts = _extract_counts(temporal_points)
    values = list(values if values is not None else _extract_values(temporal_points))
    blockers: list[dict] = []
    trace: list[dict] = []
    if not temporal_points:
        return _status_item("fragil", REPORTING_PROCESSES["sem_serie"], "Não há série para avaliar atraso de reporte.", code="sem_serie", lag_suspected_periods=[], lag_confidence="baixa", reason_code="no_series"), blockers, trace

    recent_window = min(3, max(1, len(counts) // 4))
    recent_counts = counts[-recent_window:]
    previous_counts = counts[:-recent_window] or counts
    recent_density = mean(recent_counts) if recent_counts else 0
    previous_density = mean(previous_counts) if previous_counts else 0
    recent_periods = [str(point.get("period") or "") for point in temporal_points[-recent_window:]]
    density_drop = previous_density > 0 and recent_density < previous_density * 0.35

    if density_drop:
        blockers.append(_blocker("REPORT-002", "recent_density_drop", "queda abrupta de densidade recente", "error", "reporting", "validar se os últimos períodos estão completos antes de interpretar evolução"))
        trace.append(_rule_trace("REPORT-002", f"densidade recente {recent_density:.1f} vs {previous_density:.1f}", "Reporte classificado como frágil.", "fragil"))
        return _status_item("fragil", REPORTING_PROCESSES["queda_densidade"], "A contagem de registos nos períodos recentes cai abruptamente face ao histórico observado.", code="queda_densidade", lag_suspected_periods=recent_periods, lag_confidence="alta", reason_code="recent_density_drop"), blockers, trace

    if ordering == "temporal_desc" and (coverage_ratio is None or coverage_ratio < 0.8):
        blockers.append(_blocker("REPORT-001", "recent_partial_sample", "amostra recente parcial", "warning", "reporting", "confirmar cobertura da amostra e atualização da fonte"))
        trace.append(_rule_trace("REPORT-001", f"ordenação {ordering}; cobertura {coverage_ratio}", "Reporte classificado como a rever.", "rever"))
        return _status_item("rever", REPORTING_PROCESSES["amostra_recente_parcial"], "A amostra está ordenada por período recente e pode não representar a série completa.", code="amostra_recente_parcial", lag_suspected_periods=recent_periods, lag_confidence="baixa", reason_code="recent_sample_ordering"), blockers, trace

    return _status_item("pronto", REPORTING_PROCESSES["sem_sinal"], "Não há indício forte de cauda recente truncada.", code="sem_sinal", lag_suspected_periods=[], lag_confidence="baixa", reason_code="no_lag_signal"), blockers, trace


def evaluate_geography_axis(
    *,
    fields: list[dict] | list[str] | None = None,
    categorical_profiles: list[dict] | None = None,
    surveillance_domain: dict | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    text = normalize_token(" ".join([_field_text(fields), " ".join(str(profile.get("field") or profile.get("label") or "") for profile in categorical_profiles or [])]))
    local_count = len(re.findall(r"\b(regiao|ars|uls|hospital|concelho|distrito|entidade|unidade|localizacao)\b", text))
    blockers: list[dict] = []
    trace: list[dict] = []
    if local_count:
        return _status_item("pronto", f"{local_count} pista(s) territorial/entidade", "Há geografia ou entidade explícita para contextualizar a leitura.", local_dimension_count=local_count), blockers, trace
    if (surveillance_domain or {}).get("code") == "territorial_agregado":
        blockers.append(_blocker("GEO-001", "geography_missing_for_territory", "geografia/entidade por validar", "warning", "geography", "identificar geografia ou entidade antes de comparar territórios"))
        trace.append(_rule_trace("GEO-001", "domínio territorial sem campo territorial", "Geografia classificada como a rever.", "rever"))
    return _status_item("rever", "sem geografia/entidade explícita", "Comparação territorial exige geografia e entidade estáveis.", local_dimension_count=0), blockers, trace


def evaluate_missingness(
    *,
    quality_summary: dict | None = None,
    numeric_profiles: list[dict] | None = None,
    categorical_profiles: list[dict] | None = None,
) -> dict:
    quality_summary = quality_summary or {}
    ratio = (quality_summary.get("granularity") or {}).get("max_missing_ratio")
    if ratio is None:
        ratios = []
        for profile in [*(numeric_profiles or []), *(categorical_profiles or [])]:
            total = (profile.get("count") or 0) + (profile.get("missing") or 0)
            if total:
                ratios.append((profile.get("missing") or 0) / total)
        ratio = max(ratios, default=0.0)
    if ratio >= 0.45:
        status, label = "fragil", "missing elevado"
    elif ratio >= 0.25:
        status, label = "rever", "missing a rever"
    else:
        status, label = "pronto", "missing controlado"
    return _status_item(status, label, f"Maior proporção de valores em falta: {ratio:.1%}.", max_missing_ratio=round(ratio, 4))


def _evidence_grade(overall_status: str, blockers: list[dict]) -> dict:
    if overall_status == "pronto":
        grade = "A"
    elif overall_status == "rever" and len(blockers) <= 2:
        grade = "B"
    elif overall_status == "rever":
        grade = "C"
    else:
        grade = "D"
    return _status_item(overall_status, EVIDENCE_GRADES[grade], "Grau interno de evidência exploratória; não é validade clínica nem causal.", grade=grade)


def _quality_aliases(review: dict) -> dict:
    domain = review["surveillance_domain"]
    time_axis = review["time_axis"]
    denominator = review["denominator"]
    geography = review["geography_axis"]
    reporting = review["reporting_process"]
    zero = review["zero_meaning"]
    source_profile = {
        "dataset_family": domain["code"],
        "update_pattern": (time_axis.get("periodicity") or {}).get("label") or "periodicidade por validar",
        "expected_periodicity": time_axis.get("periodicity") or {"code": "desconhecida", "label": "periodicidade por validar", "expected": "por validar"},
        "known_truncation_risk": {
            "status": reporting["status"],
            "label": reporting["label"],
            "detail": reporting["detail"],
        },
        "denominator_reliability": {
            "status": denominator["status"],
            "label": denominator["label"],
            "detail": denominator["detail"],
        },
        "territorial_stability": {
            "status": geography["status"],
            "label": geography["label"],
            "detail": geography["detail"],
        },
    }
    return {
        "dataset_family": _domain_alias(domain),
        "source_reliability_profile": source_profile,
        "population_at_risk": review["population_basis"],
        "period_comparability": time_axis,
        "zero_semantics": {
            **zero,
            "interpretation": zero.get("interpretation") or zero.get("label"),
        },
        "reporting_lag": reporting,
        "coverage_assessment": review["coverage_assessment"],
        "granularity_assessment": geography,
    }


def build_epidemiology_review(
    *,
    dataset_id: str = "",
    dataset_title: str = "",
    mega_theme: str = "",
    fields: list[dict] | list[str] | None = None,
    temporal_field: str | None = None,
    ordering: str = "api_default",
    quality_summary: dict | None = None,
    quality_warnings: list[str] | None = None,
    numeric_profiles: list[dict] | None = None,
    categorical_profiles: list[dict] | None = None,
    trends: list[dict] | None = None,
) -> dict:
    quality_summary = quality_summary or {}
    quality_warnings = quality_warnings or []
    numeric_profiles = numeric_profiles or []
    categorical_profiles = categorical_profiles or []
    trends = trends or []
    temporal_points = (trends[0].get("points") if trends else []) or []
    values = _extract_values(temporal_points)
    coverage_ratio = (quality_summary.get("coverage") or {}).get("ratio")

    surveillance_domain, trace = classify_surveillance_domain(
        dataset_id=dataset_id,
        dataset_title=dataset_title,
        mega_theme=mega_theme,
        fields=fields,
        numeric_profiles=numeric_profiles,
    )
    observation_unit, observation_blockers, observation_trace = evaluate_observation_unit(
        fields=fields,
        temporal_field=temporal_field,
        numeric_profiles=numeric_profiles,
        categorical_profiles=categorical_profiles,
    )
    measure_construct = evaluate_measure_construct(numeric_profiles)
    denominator, population_basis, denominator_blockers, denominator_trace = evaluate_denominator(
        surveillance_domain=surveillance_domain,
        measure_construct=measure_construct,
        quality_summary=quality_summary,
        fields=fields,
    )
    time_axis, time_blockers, time_trace = evaluate_time_axis(
        temporal_field=temporal_field,
        temporal_points=temporal_points,
        surveillance_domain=surveillance_domain,
    )
    zero_meaning, zero_blockers, zero_trace = evaluate_zero_meaning(
        surveillance_domain=surveillance_domain,
        temporal_points=temporal_points,
        values=values,
        ordering=ordering,
    )
    reporting_process, reporting_blockers, reporting_trace = evaluate_reporting_process(
        surveillance_domain=surveillance_domain,
        ordering=ordering,
        temporal_points=temporal_points,
        values=values,
        coverage_ratio=coverage_ratio,
    )
    geography_axis, geo_blockers, geo_trace = evaluate_geography_axis(
        fields=fields,
        categorical_profiles=categorical_profiles,
        surveillance_domain=surveillance_domain,
    )
    missingness = evaluate_missingness(
        quality_summary=quality_summary,
        numeric_profiles=numeric_profiles,
        categorical_profiles=categorical_profiles,
    )
    coverage_quality = quality_summary.get("coverage") or {}
    coverage_assessment = _status_item(
        coverage_quality.get("status") or "rever",
        coverage_quality.get("label") or "cobertura por validar",
        "A amostra cobre boa parte do total conhecido." if coverage_quality.get("status") == "pronto" else "A leitura pode estar limitada por amostra parcial.",
        ratio=coverage_ratio,
    )

    blocking_factors = [
        *observation_blockers,
        *denominator_blockers,
        *time_blockers,
        *zero_blockers,
        *reporting_blockers,
        *geo_blockers,
    ]
    if coverage_assessment["status"] == "fragil":
        blocking_factors.append(_blocker("REPORT-001", "coverage_low", "cobertura insuficiente", "warning", "reporting", "evitar leitura substantiva de amostra parcial"))
    if missingness["status"] == "fragil":
        blocking_factors.append(_blocker("COMP-001", "missingness_high", "missing elevado", "warning", "comparability", "validar campos antes de comparar dimensões"))

    comparability_status = _combine_status([
        observation_unit["status"],
        denominator["status"],
        time_axis["status"],
        geography_axis["status"],
        missingness["status"],
    ])
    if any(blocker["severity"] == "error" for blocker in blocking_factors):
        comparability_status = "fragil"
    comparability = _status_item(
        comparability_status,
        COMPARABILITY_STATES["comparavel" if comparability_status == "pronto" else ("incompativel" if comparability_status == "fragil" else "por_validar")],
        "Unidade, período, denominador e âmbito suportam triagem." if comparability_status == "pronto" else "Comparabilidade exige validação explícita antes de resultados fortes.",
    )

    overall_status = "fragil" if any(item["severity"] == "error" for item in blocking_factors) else ("rever" if blocking_factors else "pronto")
    evidence_grade = _evidence_grade(overall_status, blocking_factors)
    analytical_intent = _status_item(
        overall_status,
        ANALYTICAL_INTENTS["triagem_exploratoria"],
        "Primeiro validar interpretabilidade; depois ler sinais úteis.",
        code="triagem_exploratoria",
    )
    case_definition_proxy = _status_item(
        "rever" if surveillance_domain["code"] in {"eventos_raros", "atividade_assistencial"} else "pronto",
        "proxy por metadados" if surveillance_domain["code"] in {"eventos_raros", "atividade_assistencial"} else "não aplicável / agregado",
        "A definição de caso é inferida por título/campos e deve ser validada na fonte.",
    )
    review = {
        "ontology_version": ONTOLOGY_VERSION,
        "analytical_intent": analytical_intent,
        "surveillance_domain": surveillance_domain,
        "observation_unit": observation_unit,
        "case_definition_proxy": case_definition_proxy,
        "population_basis": population_basis,
        "measure_construct": measure_construct,
        "denominator": denominator,
        "time_axis": time_axis,
        "geography_axis": geography_axis,
        "reporting_process": reporting_process,
        "zero_meaning": zero_meaning,
        "missingness": missingness,
        "coverage_assessment": coverage_assessment,
        "comparability": comparability,
        "evidence_grade": evidence_grade,
        "blocking_factors": blocking_factors,
        "rule_trace": [*trace, *observation_trace, *denominator_trace, *time_trace, *zero_trace, *reporting_trace, *geo_trace],
        "overall": {
            "status": overall_status,
            "label": STATUS_LABELS[overall_status],
            "summary": "A série pode ser interpretada." if overall_status == "pronto" else ("Há leitura exploratória com cautela." if overall_status == "rever" else "A interpretação deve ficar em modo de revisão."),
        },
        "warnings": quality_warnings[:5],
    }
    review.update(_quality_aliases(review))
    return review


def build_finprod_epidemiology_review(
    *,
    numerator_valid: bool,
    denominator_valid: bool,
    shared_periods: list[str],
    pairs: list[tuple[float, float]],
    benchmark_rows: list[dict],
    blockers: list[dict] | None = None,
    rows: list[dict] | None = None,
    shared_granularity: str = "",
    numerator_label: str = "",
    denominator_label: str = "",
    unit_warnings: list[str] | None = None,
) -> dict:
    unit_warnings = unit_warnings or []
    rows = rows or []
    blockers = list(blockers or [])
    trace: list[dict] = []
    if not denominator_valid:
        blockers.append(_blocker("DENOM-002", "unit_cost_denominator_invalid", "denominador produtivo por validar", "error", "denominator", "confirmar produção/atividade como denominador válido"))
        trace.append(_rule_trace("DENOM-002", denominator_label or "denominador não produtivo", "Custo unitário bloqueado.", "fragil"))
    if not numerator_valid:
        blockers.append(_blocker("COMP-001", "unit_cost_numerator_invalid", "numerador não monetário", "error", "measure", "validar variável financeira antes de estimar custo unitário"))
        trace.append(_rule_trace("COMP-001", numerator_label or "numerador não monetário", "Comparação classificada como frágil.", "fragil"))
    if len(shared_periods) < 3:
        blockers.append(_blocker("TIME-001", "time_axis_too_short", "menos de 3 períodos comuns", "error", "time", "alinhar pelo menos 3 períodos comuns"))
        trace.append(_rule_trace("TIME-001", f"{len(shared_periods)} períodos comuns", "Temporalidade comum classificada como frágil.", "fragil"))
    elif len(shared_periods) <= 5:
        blockers.append(_blocker("TIME-002", "time_axis_low_density", "poucos períodos comuns", "warning", "time", "obter mais períodos comuns antes de comunicar custo unitário"))
        trace.append(_rule_trace("TIME-002", f"{len(shared_periods)} períodos comuns", "Temporalidade comum classificada como a rever.", "rever"))
    if shared_periods and not pairs:
        blockers.append(_blocker("COMP-001", "unit_cost_no_valid_pairs", "sem pares válidos", "error", "comparability", "validar valores positivos de produção"))
    if not benchmark_rows:
        blockers.append(_blocker("GEO-001", "scope_unvalidated", "âmbito territorial/entidade por validar", "warning", "geography", "confirmar geografia e entidade comuns"))

    denominator_status = "pronto" if denominator_valid else "fragil"
    time_status = "pronto" if len(shared_periods) >= 6 else ("rever" if len(shared_periods) >= 3 else "fragil")
    coverage_status = "pronto" if len(pairs) >= 6 else ("rever" if pairs else "fragil")
    geography_status = "pronto" if benchmark_rows else "rever"
    overall_status = "fragil" if any(item["severity"] == "error" for item in blockers) else ("rever" if blockers else "pronto")
    zero_count = sum(1 for row in rows if (row.get("production_value") or 0) <= 0)
    active_count = sum(1 for row in rows if (row.get("production_value") or 0) > 0)
    review = {
        "ontology_version": ONTOLOGY_VERSION,
        "analytical_intent": _status_item(overall_status, ANALYTICAL_INTENTS["custo_unitario"], "Só promover custo unitário quando numerador, denominador, período e âmbito passam.", code="custo_unitario"),
        "surveillance_domain": _status_item("pronto", SURVEILLANCE_DOMAINS["financeiro"], "Custo unitário cruza valor financeiro com produção.", code="financeiro", signals=["financeiro", "produção", "período comum"], rule_id="SURV-DOMAIN-001"),
        "observation_unit": _status_item("pronto", "agregado por período comum", "A comparação usa séries agregadas por período comum.", code="periodo"),
        "case_definition_proxy": _status_item("rever", "não aplicável / proxy operacional", "A produção usada como denominador deve ser validada na fonte."),
        "population_basis": _status_item(denominator_status, POPULATION_BASES["atividade_observada"], "O denominador deve representar produção/atividade válida.", code="atividade_observada"),
        "measure_construct": _status_item("pronto" if numerator_valid else "fragil", MEASURE_CONSTRUCTS["valor_monetario"], "Numerador deve ser valor monetário por período.", code="valor_monetario"),
        "denominator": _status_item(denominator_status, denominator_label or ("denominador validado" if denominator_valid else "denominador produtivo por validar"), "Custo unitário exige denominador produtivo explícito.", code="validado" if denominator_valid else "fragil"),
        "time_axis": _status_item(time_status, f"{len(shared_periods)} períodos em comum", "A comparação depende da interseção temporal real.", valid_periods=len(shared_periods), min_records_per_period=len(pairs), periodicity={"code": shared_granularity or "desconhecida", "label": shared_granularity or "granularidade por validar"}),
        "geography_axis": _status_item(geography_status, "âmbito comparável" if benchmark_rows else "âmbito por validar", "Comparar só quando geografia/entidade forem compatíveis.", local_dimension_count=len(benchmark_rows)),
        "reporting_process": _status_item("rever" if len(rows) < len(shared_periods) else "pronto", "pares válidos abaixo dos períodos comuns" if len(rows) < len(shared_periods) else "sem atraso visível", "A leitura depende da interseção temporal e de produção > 0.", code="paired_period_loss" if len(rows) < len(shared_periods) else "sem_sinal", lag_suspected_periods=[], lag_confidence="baixa", reason_code="paired_period_loss" if len(rows) < len(shared_periods) else "no_lag_signal"),
        "zero_meaning": _status_item("rever" if zero_count else "pronto", "produção com zeros" if zero_count else "zeros não dominantes", "Zeros no denominador bloqueiam ou fragilizam o custo unitário.", code="denominator_zero", reason_code="unit_cost_denominator_zero", zero_periods=zero_count, active_periods=active_count, zero_tail=0, zero_ratio=round(zero_count / max(1, len(rows)), 4), interpretation="zero no denominador não é custo zero"),
        "missingness": _status_item("rever", "missing por validar nos dois datasets", "A qualidade depende das duas amostras e da interseção usada."),
        "coverage_assessment": _status_item(coverage_status, f"{len(pairs)} pares válidos", "A cobertura útil depende de pares com produção > 0.", ratio=round((len(pairs) / max(1, len(shared_periods))), 4) if shared_periods else 0),
        "comparability": _status_item(overall_status, COMPARABILITY_STATES["comparavel" if overall_status == "pronto" else ("incompativel" if overall_status == "fragil" else "por_validar")], "Numerador, denominador, período e âmbito têm de passar antes de promover custo unitário."),
        "evidence_grade": _evidence_grade(overall_status, blockers),
        "blocking_factors": blockers,
        "rule_trace": trace,
        "overall": {
            "status": overall_status,
            "label": STATUS_LABELS[overall_status],
            "summary": "Par comparável para leitura exploratória." if overall_status == "pronto" else "Só leitura exploratória enquanto houver travões.",
        },
        "warnings": unit_warnings[:5],
    }
    aliases = _quality_aliases(review)
    aliases["dataset_family"] = {
        "code": "custo_unitario",
        "label": "Custo unitário",
        "zero_policy": "zero no denominador bloqueia leitura de rácio",
        "lag_sensitivity": "alta",
        "review_focus": "numerador, denominador, âmbito e período comum",
        "signals": ["financeiro", "produção", "período comum"],
    }
    review.update(aliases)
    return review
