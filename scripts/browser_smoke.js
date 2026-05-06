const fs = require("fs");
const http = require("http");
const path = require("path");

const root = path.resolve(__dirname, "..");

const datasets = [
  {
    dataset_id: "financeiro",
    title: "Despesa sintética",
    mega_theme: "Finanças & Compras",
    metric_candidate_count: 3,
    records_count: 120,
    field_count: 8,
    fields: ["periodo", "regiao", "valor", "entidade"],
    facets: {
      tags: ["financeiro", "medida", "regiao", "temporal"],
      labels: ["Financeiro", "Medida", "Região/ARS", "Temporal"],
      local_scopes: ["regiao"],
      institution_types: [],
      dimension_types: ["medida", "territorial", "temporal"],
    },
    quality_flags: [],
    analysis_readiness: {score: 88, band: "pronto", label: "Pronto", gaps: []},
  },
  {
    dataset_id: "producao",
    title: "Produção sintética",
    mega_theme: "Acesso & Produção",
    metric_candidate_count: 3,
    records_count: 120,
    field_count: 8,
    fields: ["periodo", "regiao", "valor", "entidade"],
    facets: {
      tags: ["producao", "medida", "regiao", "temporal"],
      labels: ["Produção", "Medida", "Região/ARS", "Temporal"],
      local_scopes: ["regiao"],
      institution_types: [],
      dimension_types: ["medida", "territorial", "temporal"],
    },
    quality_flags: [],
    analysis_readiness: {score: 88, band: "pronto", label: "Pronto", gaps: []},
  },
];

function json(response, payload) {
  response.writeHead(200, {"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store"});
  response.end(JSON.stringify(payload));
}

function dataAnalytics(datasetId) {
  const review = {
    ontology_version: "browser-smoke.ontology",
    overall: {status: "pronto", label: "Pronto", summary: "A série pode ser interpretada."},
    analytical_intent: {status: "pronto", label: "Triagem exploratória", detail: "Validar interpretabilidade antes de ler sinais úteis.", code: "triagem_exploratoria"},
    surveillance_domain: {status: "pronto", code: "financeiro", label: "Financeiro", detail: "Validar base contabilística, período e unidade monetária antes de interpretar.", signals: ["valor monetário"], rule_id: "SURV-DOMAIN-001"},
    population_basis: {status: "rever", code: "base_contabilistica", label: "Base contabilística", detail: "Confirmar denominador e unidade."},
    measure_construct: {status: "pronto", code: "valor_monetario", label: "Valor monetário", detail: "Medida monetária provável."},
    denominator: {status: "rever", code: "sensivel", label: "denominador sensível", detail: "Validar unidade."},
    time_axis: {status: "pronto", label: "6 períodos válidos", detail: "Períodos comparáveis para leitura temporal.", valid_periods: 6, min_records_per_period: 3, periodicity: {code: "mensal", label: "mensal provável"}},
    geography_axis: {status: "pronto", label: "1 dimensão territorial", detail: "Região disponível.", local_dimension_count: 1},
    reporting_process: {status: "pronto", label: "sem atraso visível", detail: "Não há indício forte de cauda recente truncada.", lag_suspected_periods: [], lag_confidence: "baixa", reason_code: "no_lag_signal"},
    zero_meaning: {status: "pronto", label: "sem zeros relevantes", detail: "A série não sugere silêncio de reporte.", zero_periods: 0, active_periods: 6, zero_tail: 0, zero_ratio: 0, interpretation: "zero por validar", reason_code: "no_relevant_zeros"},
    missingness: {status: "pronto", label: "missing controlado", detail: "Maior proporção de valores em falta: 0%."},
    comparability: {status: "pronto", label: "Comparável para triagem", detail: "Unidade, período, denominador e âmbito suportam triagem."},
    evidence_grade: {status: "pronto", label: "Evidência exploratória forte", detail: "Grau interno; não é validade clínica nem causal.", grade: "A"},
    rule_trace: [{rule_id: "SURV-DOMAIN-001", input_signal: "valor monetário", decision: "Classificado como Financeiro.", status_delta: "neutro", explanation: "Valores financeiros exigem base contabilística e período de execução."}],
    dataset_family: {code: "financeiro", label: "Financeiro", zero_policy: "zero pode ser não execução ou atraso contabilístico", lag_sensitivity: "media", review_focus: "base contabilística, período e unidade", signals: ["valor monetário"]},
    source_reliability_profile: {
      dataset_family: "financeiro",
      update_pattern: "mensal provável",
      expected_periodicity: {code: "mensal", label: "mensal provável", expected: "mensal, trimestral ou anual"},
      known_truncation_risk: {status: "pronto", label: "sem truncamento forte", detail: "Fixture com série completa."},
      denominator_reliability: {status: "rever", label: "denominador sensível", detail: "Validar unidade."},
      territorial_stability: {status: "pronto", label: "1 dimensão territorial", detail: "Região disponível."},
    },
    observation_unit: {status: "pronto", label: "agregado por período", detail: "A unidade observada parece agregada e legível."},
    population_at_risk: {status: "rever", label: "validar unidade", detail: "Confirmar denominador e unidade."},
    period_comparability: {status: "pronto", label: "6 períodos válidos", detail: "Períodos comparáveis para leitura temporal.", valid_periods: 6, min_records_per_period: 3},
    zero_semantics: {status: "pronto", label: "sem zeros relevantes", detail: "A série não sugere silêncio de reporte.", zero_periods: 0, active_periods: 6, zero_tail: 0, zero_ratio: 0, interpretation: "zero por validar", reason_code: "no_relevant_zeros"},
    reporting_lag: {status: "pronto", label: "sem atraso visível", detail: "Não há indício forte de cauda recente truncada.", lag_suspected_periods: [], lag_confidence: "baixa", reason_code: "no_lag_signal"},
    coverage_assessment: {status: "rever", label: "cobertura 66.7%", detail: "Fixture parcial.", ratio: 0.6667},
    granularity_assessment: {status: "pronto", label: "granularidade legível", detail: "1 dimensão territorial.", local_dimension_count: 1},
    blocking_factors: [],
    warnings: ["Amostra sintética para smoke test."],
  };
  return {
    dataset_id: datasetId,
    title: datasetId === "financeiro" ? "Despesa sintética" : "Produção sintética",
    sample_size: 80,
    total_records: 120,
    requested_limit: 80,
    temporal_field: "periodo",
    ordering: "temporal_desc",
    methodology: {version: "browser-smoke", scope: "fixture"},
    sample: {sample_size: 80, requested_limit: 80, total_records: 120, ordering: "temporal_desc", coverage_ratio: 0.6667},
    quality_warnings: ["Amostra sintética para smoke test."],
    epidemiology_review: review,
    analysis_readiness: {score: 82, band: "pronto", label: "Pronto", gaps: ["confirmar fonte"]},
    numeric_profiles: [
      {field: "valor", label: "Valor", count: 80, missing: 0, min: 1, max: 80, avg: 40, stddev: 10, measure_role: "contagem"},
      {field: "taxa", label: "Taxa", count: 80, missing: 0, min: 1, max: 8, avg: 4, stddev: 1, measure_role: "taxa"},
    ],
    categorical_profiles: [
      {field: "regiao", label: "Região", count: 80, missing: 0, unique: 2, top_values: [{value: "Norte", count: 40}, {value: "Sul", count: 40}]},
    ],
    correlations: [
      {field_a: "valor", field_b: "taxa", label_a: "Valor", label_b: "Taxa", method: "pearson", correlation: 0.82, abs_correlation: 0.82, spearman: 0.8, strength: "forte", samples: 80, warnings: []},
    ],
    trends: [
      {field: "valor", label: "Valor", measure_role: "contagem", aggregation: "soma", validation: {periods: 6, min_records_per_period: 3, warnings: []}, points: [
        {period: "2025-01", avg: 10, sum: 30, value: 30, count: 3},
        {period: "2025-02", avg: 14, sum: 42, value: 42, count: 3},
        {period: "2025-03", avg: 18, sum: 54, value: 54, count: 3},
        {period: "2025-04", avg: 21, sum: 63, value: 63, count: 3},
        {period: "2025-05", avg: 24, sum: 72, value: 72, count: 3},
        {period: "2025-06", avg: 27, sum: 81, value: 81, count: 3},
      ]},
    ],
    feature_importance: [
      {field: "valor", label: "Valor", kind: "medida", score: 82, selection: "forte", drivers: ["completude 100%", "variação 80%"]},
    ],
    pca_summary: {
      available: true,
      explained_variance: {pc1: 0.7, pc2: 0.2},
      warnings: ["PCA fixture."],
      loadings: [
        {field: "valor", label: "Valor", pc1: 0.8, pc2: 0.2, magnitude: 0.82},
        {field: "taxa", label: "Taxa", pc1: -0.2, pc2: 0.7, magnitude: 0.73},
      ],
    },
    insights: [{label: "Correlação mais forte", value: "0.82", detail: "Valor / Taxa · 80 pares"}],
    generated_at: 1,
  };
}

function analyticsPayload() {
  return {
    generated_at: 1,
    methodology: {version: "browser-smoke"},
    datasets,
    summary: {dataset_count: 2, link_count: 1, dimension_count: 2, high_confidence_count: 1},
    dimensions: [{field: "periodo", label: "Período", kind: "temporal", coverage: 2, score: 8}],
    correlations: [{
      source: "financeiro",
      target: "producao",
      source_title: "Despesa sintética",
      target_title: "Produção sintética",
      source_theme: "Finanças & Compras",
      target_theme: "Acesso & Produção",
      semantic_score: 8,
      score: 8,
      confidence: "alta",
      dimension_kinds: ["temporal", "entidade"],
      local_scopes: ["regiao"],
      institution_types: [],
      facet_tags: ["regiao", "temporal", "financeiro", "producao"],
      shared_fields: ["periodo", "regiao"],
      risk_flags: [],
      join_recipe: {suggested_keys: ["periodo", "regiao"]},
      public_health_model: {likelihood: {level: "alta"}, impact: {level: "medio"}, matched_areas: ["Acesso"]},
    }],
    theme_matrix: [],
    dimension_matrix: [],
    facet_counts: {local_scopes: {regiao: 2}, dimension_types: {temporal: 2, territorial: 2, medida: 2}},
    quality_flags: {},
  };
}

function analysisPayload() {
  return {
    generated_at: 1,
    methodology: {version: "browser-smoke", source: "catalog_link_semantics", fallback: false},
    datasets,
    links: [{source: "financeiro", target: "producao", score: 8, shared_fields: ["periodo", "regiao"]}],
    opportunities: [{key: "periodo", dataset_ids: ["financeiro", "producao"], dataset_count: 2, truncated: false}],
    themes: [
      {theme: "Finanças & Compras", dataset_count: 1, link_score: 8, description: "Despesa sintética."},
      {theme: "Acesso & Produção", dataset_count: 1, link_score: 8, description: "Produção sintética."},
    ],
    facet_counts: {local_scopes: {regiao: 2}, dimension_types: {temporal: 2, territorial: 2, medida: 2}},
    total: 2,
    link_count: 1,
    fallback: false,
    warning: null,
  };
}

function finprodPayload() {
  return {
    financial_dataset: {dataset_id: "financeiro", title: "Despesa sintética", temporal_field: "periodo", trend_label: "Valor"},
    production_dataset: {dataset_id: "producao", title: "Produção sintética", temporal_field: "periodo", trend_label: "Valor"},
    summary: {matched_periods: 6, avg_unit_cost: 5, median_unit_cost: 5, expense_output_correlation: 0.9, spearman_correlation: 0.9, correlation_strength: "forte", robustness: "moderada", sample_pairs: 6},
    comparability: {checks: [{label: "Períodos em comum", status: "ok", detail: "6 períodos"}]},
    aggregation: {period: "mensal", numerator: "soma monetária por período comum", denominator: "soma de produção/atividade por período comum"},
    numerator: {dataset_id: "financeiro", field: "valor", label: "Valor", role: "monetario", aggregation: "soma"},
    denominator: {dataset_id: "producao", field: "valor", label: "Valor", role: "contagem", aggregation: "soma"},
    grouping: {period_key: "periodo normalizado", matched_periods: 6, entity_benchmark_available: false},
    unit_warnings: [],
    methodology: {version: "browser-smoke"},
    normalization: {financial: {scale: 1, unit_label: "€"}, production: {scale: 1, unit_label: "unidade"}},
    rows: [
      {period: "2025-01", financial_normalized: 10, production_normalized: 2, unit_cost: 5},
      {period: "2025-02", financial_normalized: 20, production_normalized: 4, unit_cost: 5},
    ],
    outliers: [],
    entity_benchmark: [],
    diagnostics: {warnings: []},
    epidemiology_review: {
      overall: {status: "pronto", label: "Pronto", summary: "Par comparável para leitura exploratória."},
      dataset_family: {code: "custo_unitario", label: "Custo unitário", zero_policy: "zero no denominador bloqueia leitura de rácio", lag_sensitivity: "alta", review_focus: "numerador, denominador, âmbito e período comum", signals: ["financeiro", "produção"]},
      source_reliability_profile: {
        dataset_family: "custo_unitario",
        update_pattern: "mensal/trimestral",
        expected_periodicity: {code: "mensal/trimestral", label: "mensal/trimestral", expected: "mesma granularidade"},
        known_truncation_risk: {status: "pronto", label: "sem truncamento forte", detail: "Pares válidos suficientes."},
        denominator_reliability: {status: "pronto", label: "soma de produção/atividade por período comum", detail: "Denominador produtivo."},
        territorial_stability: {status: "rever", label: "âmbito por validar", detail: "Fixture sem benchmark territorial."},
      },
      observation_unit: {status: "pronto", label: "agregado por período comum", detail: "Séries agregadas por período."},
      population_at_risk: {status: "pronto", label: "soma de produção/atividade por período comum", detail: "Denominador produtivo."},
      period_comparability: {status: "pronto", label: "6 períodos em comum", detail: "Interseção temporal real.", valid_periods: 6, min_records_per_period: 6},
      zero_semantics: {status: "pronto", label: "zeros não dominantes", detail: "Sem zeros no denominador.", zero_periods: 0, active_periods: 6, zero_tail: 0, interpretation: "zero no denominador não é custo zero", reason_code: "unit_cost_denominator_zero"},
      reporting_lag: {status: "pronto", label: "sem atraso visível", detail: "Todos os períodos geram pares válidos.", lag_suspected_periods: [], lag_confidence: "baixa", reason_code: "no_lag_signal"},
      coverage_assessment: {status: "pronto", label: "6 pares válidos", detail: "Cobertura útil suficiente.", ratio: 1},
      granularity_assessment: {status: "rever", label: "âmbito por validar", detail: "Geografia e entidade devem ser comparáveis.", local_dimension_count: 0},
      blocking_factors: [],
      warnings: [],
    },
    generated_at: 1,
  };
}

function deepResearchPayload(datasetId) {
  const analysis = dataAnalytics(datasetId);
  return {
    dataset_id: datasetId,
    title: analysis.title,
    analysis,
    feature_screening: {
      method: "feature_screening_deterministic_shadow",
      thresholds: {max_shadow: 45, median_shadow: 24},
      confirmed: [{field: "valor", label: "Valor", kind: "medida", score: 82, drivers: ["completude 100%"], selection: "confirmada"}],
      tentative: [],
      rejected: [],
    },
    boruta: {
      method: "feature_screening_deterministic_shadow",
      thresholds: {max_shadow: 45, median_shadow: 24},
      confirmed: [{field: "valor", label: "Valor", kind: "medida", score: 82, drivers: ["completude 100%"], selection: "confirmada"}],
      tentative: [],
      rejected: [],
    },
    territorial_map: {regions: {Norte: 40, Centro: 40}, uls: {}, entities: {}},
    methodology: {version: "browser-smoke", copy_safe: true},
    quality_warnings: analysis.quality_warnings,
    epidemiology_review: analysis.epidemiology_review,
    generated_at: 1,
  };
}

function datasetPayload(datasetId) {
  const found = datasets.find((dataset) => dataset.dataset_id === datasetId) || datasets[0];
  return {
    dataset_id: found.dataset_id,
    title: found.title,
    metas: {default: {title: found.title, records_count: found.records_count}},
    records_count: found.records_count,
    fields: (found.fields || []).map((name) => ({name, label: name, type: name === "periodo" ? "date" : "text"})),
    schema_quality: {field_count: found.field_count, typed_fields: 3, temporal_fields: 1, territorial_fields: 1, ignored_identifier_fields: 0},
  };
}

function recentPayload(datasetId) {
  return {
    dataset_id: datasetId,
    temporal_field: "periodo",
    min_year: 2025,
    columns: ["periodo", "regiao", "valor", "entidade"].map((name) => ({name, label: name, type: name === "valor" ? "integer" : "string"})),
    records: [
      {periodo: "2025-01", regiao: "Norte", valor: 30, entidade: "SNS Norte"},
      {periodo: "2025-02", regiao: "Centro", valor: 42, entidade: "SNS Centro"},
    ],
    total_count: 2,
    cache_status: "fixture",
  };
}

function finprodRecommendationsPayload() {
  return {
    financial_dataset_id: "financeiro",
    financial_title: "Despesa sintética",
    active_production_dataset_id: "producao",
    candidate_count: 1,
    recommendations: [
      {
        financial_dataset_id: "financeiro",
        financial_title: "Despesa sintética",
        production_dataset_id: "producao",
        production_title: "Produção sintética",
        matched_periods: 6,
        sample_pairs: 6,
        robustness: "moderada",
        correlation_strength: "forte",
        financial_range: {start: "2025-01", end: "2025-06"},
        production_range: {start: "2025-01", end: "2025-06"},
        is_current: true,
      },
    ],
    errors: [],
    generated_at: 1,
  };
}

function predictiveRecommendationsPayload() {
  return {
    active_dataset_id: "financeiro",
    recommendations: [
      {
        dataset_id: "financeiro",
        title: "Despesa sintética",
        mega_theme: "Finanças & Compras",
        band: "pronto",
        score: 88,
        reasons: ["período válido", "amostra suficiente"],
        warnings: [],
      },
      {
        dataset_id: "producao",
        title: "Produção sintética",
        mega_theme: "Acesso & Produção",
        band: "rever",
        score: 72,
        reasons: ["período comum"],
        warnings: ["confirmar granularidade"],
      },
    ],
    generated_at: 1,
  };
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) return "image/jpeg";
  return "application/octet-stream";
}

const server = http.createServer((request, response) => {
  const url = new URL(request.url, "http://127.0.0.1");
  if (url.pathname === "/api/analysis") return json(response, analysisPayload());
  if (url.pathname === "/api/analytics") return json(response, analyticsPayload());
  if (url.pathname === "/api/data-analytics") return json(response, dataAnalytics(url.searchParams.get("dataset_id") || "financeiro"));
  if (url.pathname === "/api/deep-research") return json(response, deepResearchPayload(url.searchParams.get("dataset_id") || "financeiro"));
  if (url.pathname === "/api/finprod/recommendations") return json(response, finprodRecommendationsPayload());
  if (url.pathname === "/api/predictive/recommendations") return json(response, predictiveRecommendationsPayload());
  if (url.pathname === "/api/finprod") return json(response, finprodPayload());
  if (url.pathname.startsWith("/api/dataset/")) return json(response, datasetPayload(decodeURIComponent(url.pathname.split("/api/dataset/")[1])));
  if (url.pathname.startsWith("/api/recent/")) return json(response, recentPayload(decodeURIComponent(url.pathname.split("/api/recent/")[1])));
  const pathname = url.pathname === "/" ? "/index.html" : url.pathname;
  const filePath = path.join(root, pathname.replace(/^\/+/, ""));
  if (!filePath.startsWith(root) || !fs.existsSync(filePath)) {
    response.writeHead(404);
    response.end("not found");
    return;
  }
  response.writeHead(200, {"Content-Type": contentType(filePath)});
  fs.createReadStream(filePath).pipe(response);
});

async function startServer() {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return server.address().port;
}

if (process.argv.includes("--server-only")) {
  startServer().then((port) => {
    console.log(`http://127.0.0.1:${port}/analytics.html`);
  });
} else {
  (async () => {
    const {chromium} = require("playwright");
    const port = await startServer();
    const browser = await chromium.launch({headless: true});
    const page = await browser.newPage({viewport: {width: 1280, height: 900}});
    const errors = [];
    const failedResponses = [];
    page.on("console", (message) => {
      if (message.type() === "error") errors.push(message.text());
    });
    page.on("pageerror", (error) => errors.push(error.message));
    page.on("response", (response) => {
      if (response.status() >= 400) {
        failedResponses.push(`${response.status()} ${response.url()}`);
      }
    });

    async function checkNoHorizontalOverflow(label) {
      const metrics = await page.evaluate(() => ({
        clientWidth: document.documentElement.clientWidth,
        scrollWidth: Math.max(document.documentElement.scrollWidth, document.body.scrollWidth),
      }));
      if (metrics.scrollWidth > metrics.clientWidth + 2) {
        throw new Error(`${label} has horizontal overflow ${metrics.scrollWidth}/${metrics.clientWidth}`);
      }
    }

    const viewports = [
      {width: 1510, height: 768, label: "desktop"},
      {width: 1024, height: 768, label: "tablet"},
      {width: 390, height: 844, label: "mobile"},
    ];
    const pages = [
      {path: "/index.html", selector: "#datasetList .dataset-item"},
      {path: "/index.html?dataset_id=financeiro", selector: ".selected-guided-actions"},
      {path: "/crosswalk.html?dataset_id=financeiro", selector: ".cross-details-drawer"},
      {path: "/analytics.html?dataset_id=financeiro", selector: "#analyticsEpiReview .epi-review-card"},
      {path: "/analytics.html?dataset_id=financeiro&mode=tempo", selector: "#predictiveKpis div"},
      {path: "/analytics.html?dataset_id=financeiro&mode=economia", selector: "#finProdKpis div"},
      {path: "/analytics.html?dataset_id=financeiro&mode=anomalias", selector: "#anomalySummary div"},
      {path: "/analytics.html?tab=health", selector: "#publicHealthKpis div"},
      {path: "/analytics.html?tab=care", selector: "#careOpsTable tbody tr"},
      {path: "/analytics.html?tab=local", selector: "#localPlanKpis div"},
      {path: "/metodologia.html", selector: ".methodology-hero"},
      {path: "/research.html", selector: "#dataStatRows"},
    ];

    for (const viewport of viewports) {
      await page.setViewportSize({width: viewport.width, height: viewport.height});
      for (const spec of pages) {
        await page.goto(`http://127.0.0.1:${port}${spec.path}`, {waitUntil: "networkidle"});
        await page.waitForSelector(spec.selector, {timeout: 10000});
        await checkNoHorizontalOverflow(`${viewport.label} ${spec.path}`);
      }
    }

    await page.setViewportSize({width: 1280, height: 900});
    await page.goto(`http://127.0.0.1:${port}/analytics.html`, {waitUntil: "networkidle"});
    await page.waitForSelector("#dataStatRows");
    await page.click('[data-analytics-tab="finprod"]');
    await page.waitForSelector("#finProdKpis div");
    await page.click("details.analytics-secondary-nav summary");
    await page.click('[data-secondary-tab="care"]');
    await page.waitForSelector("#careOpsTable tbody tr");
    await page.click('[data-secondary-tab="tables"]');
    await page.waitForSelector("#analyticsCorrelationTable tbody tr");
    await page.selectOption("#analyticsLocalScope", "regiao");
    await page.waitForSelector("#analyticsCorrelationTable tbody tr");
    await page.click('[data-analytics-tab="finprod"]');
    await page.keyboard.press("Home");
    const selected = await page.locator('[role="tab"][aria-selected="true"]').innerText();
    if (!selected.includes("Resumo rápido")) {
      throw new Error(`Expected Home key to select Resumo rápido, got ${selected}`);
    }
    if (errors.length) {
      throw new Error(`Console/page errors: ${errors.join(" | ")}`);
    }
    if (failedResponses.length) {
      throw new Error(`Failed responses: ${failedResponses.join(" | ")}`);
    }
    await browser.close();
    server.close();
  })().catch((error) => {
    server.close();
    console.error(error);
    process.exit(1);
  });
}
