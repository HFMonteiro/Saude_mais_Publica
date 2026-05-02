"use strict";

const state = {
  selectedDataset: new URLSearchParams(window.location.search).get("dataset_id") || "",
  payload: null,
};
const DEEP_RESEARCH_LIMIT = 150;
const RESULT_LIMITS = {
  findings: 6,
  numericProfiles: 6,
  categoryProfiles: 3,
  categoryTopValues: 5,
  associations: 4,
  confirmedDrivers: 6,
  territories: 10,
};

const datasetSelect = document.getElementById("researchDatasetSelect");
const statusLabel = document.getElementById("researchStatus");
const thresholdsLabel = document.getElementById("borutaThresholds");
const overview = document.getElementById("researchOverview");
const resultNarrative = document.getElementById("resultNarrative");
const keyFindings = document.getElementById("keyFindings");
const numericResults = document.getElementById("numericResults");
const trendExplorer = document.getElementById("trendExplorer");
const categoryResults = document.getElementById("categoryResults");
const associationResults = document.getElementById("associationResults");
const featureList = document.getElementById("borutaList") || document.getElementById("confirmedDrivers");
const mapMeta = document.getElementById("mapMeta");
const territorySummary = document.getElementById("territorySummary");
const territoryBars = document.getElementById("territoryBars");
const territoryEntities = document.getElementById("territoryEntities");
const territoryReadout = document.getElementById("territoryReadout");
const kpiGrid = document.getElementById("territorialKpis");
const warningList = document.getElementById("qualityWarnings");
const confirmedDrivers = document.getElementById("confirmedDrivers");

const STATUS_LABELS = {
  confirmada: "confirmada",
  tentativa: "a rever",
  rejeitada: "fraca",
};

function ptText(value) {
  return String(value || "")
    .replace(/\bnao\b/g, "não")
    .replace(/\bNao\b/g, "Não")
    .replace(/\bperiodo\b/g, "período")
    .replace(/\bperiodos\b/g, "períodos")
    .replace(/\bcorrelacao\b/g, "correlação")
    .replace(/\bcorrelacoes\b/g, "correlações")
    .replace(/\btendencia\b/g, "tendência")
    .replace(/\btendencias\b/g, "tendências")
    .replace(/\bsemantico\b/g, "semântico")
    .replace(/\bsemantica\b/g, "semântica");
}

function clearAll() {
  [
    overview,
    resultNarrative,
    keyFindings,
    numericResults,
    trendExplorer,
    categoryResults,
    associationResults,
    featureList,
    territorySummary,
    territoryBars,
    territoryEntities,
    territoryReadout,
    kpiGrid,
    warningList,
    confirmedDrivers,
  ]
    .filter(Boolean)
    .forEach((node) => node.replaceChildren());
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = ptText(text);
  return node;
}

function formatNumber(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("pt-PT").format(number);
}

function formatDecimal(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  return new Intl.NumberFormat("pt-PT", {maximumFractionDigits: digits}).format(number);
}

function percent(value) {
  const ratio = Number(value);
  if (!Number.isFinite(ratio)) return "-";
  const bounded = Math.max(0, Math.min(1, ratio));
  return `${Math.round(bounded * 100)}%`;
}

function compactText(value, max = 78) {
  const text = String(value || "");
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

function formatMeasure(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "-";
  const abs = Math.abs(number);
  const maxDigits = abs > 999 ? 0 : abs < 1 ? 3 : digits;
  return new Intl.NumberFormat("pt-PT", {maximumFractionDigits: maxDigits}).format(number);
}

function fieldLabel(profile) {
  return profile?.label || profile?.field || "Indicador";
}

function measureRoleLabel(value) {
  return {
    monetario: "valor monetário",
    contagem: "contagem",
    taxa: "taxa",
    stock: "stock",
    desconhecido: "medida",
  }[value] || "medida";
}

function friendlyReadinessLabel(value) {
  const label = String(value || "").toLowerCase();
  if (label.includes("pronto")) return "Pronto para explorar";
  if (label.includes("rever")) return "Usar com cautela";
  if (label.includes("frágil") || label.includes("fragil")) return "Frágil";
  return value || "Rever";
}

function screeningRows(screening) {
  return [
    ...(screening.confirmed || []).map((item) => ({...item, status: "confirmada"})),
    ...(screening.tentative || []).map((item) => ({...item, status: "tentativa"})),
    ...(screening.rejected || []).map((item) => ({...item, status: "rejeitada"})),
  ];
}

function territoryEntries(group) {
  return Object.entries(group || {})
    .map(([name, count]) => ({name, count: Number(count || 0)}))
    .filter((item) => item.name && item.count > 0)
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, "pt"));
}

function screeningMethodLabel(method) {
  const normalized = String(method || "").toLowerCase();
  if (normalized.includes("feature_screening")) return "Triagem de variáveis";
  if (normalized.includes("boruta")) return "Triagem de variáveis";
  return "Triagem exploratória";
}

function screeningMethodDetail(screening, analysis) {
  const method = String(screening.method || "");
  const version = analysis.methodology?.version || screening.seed || "";
  const parts = [];
  if (method.includes("deterministic")) parts.push("determinística");
  if (method.includes("shadow")) parts.push("com referência de ruído");
  if (version) parts.push(version);
  return parts.join(" · ") || "método documentado";
}

async function init() {
  statusLabel.textContent = "A carregar catálogo...";
  const catalog = await fetchJson("/api/analytics");
  const datasets = catalog.datasets || [];
  datasetSelect.replaceChildren();

  datasets.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = dataset.title || dataset.dataset_id;
    datasetSelect.appendChild(option);
  });

  if (!state.selectedDataset && datasets.length) {
    state.selectedDataset = datasets[0].dataset_id;
  } else if (state.selectedDataset && !datasets.some((dataset) => dataset.dataset_id === state.selectedDataset)) {
    state.selectedDataset = datasets[0]?.dataset_id || "";
    if (state.selectedDataset) {
      window.history.replaceState(null, "", `?dataset_id=${encodeURIComponent(state.selectedDataset)}`);
    }
  }
  datasetSelect.value = state.selectedDataset;
  datasetSelect.disabled = !datasets.length;

  datasetSelect.addEventListener("change", (event) => {
    state.selectedDataset = event.target.value;
    window.history.replaceState(null, "", `?dataset_id=${encodeURIComponent(state.selectedDataset)}`);
    loadData();
  });

  loadData();
}

async function loadData() {
  if (!state.selectedDataset) {
    clearAll();
    statusLabel.textContent = "Sem datasets reais";
    overview.appendChild(emptyState("Catálogo indisponível", "A API não devolveu datasets. Não são usados dados fictícios."));
    return;
  }

  clearAll();
  statusLabel.textContent = "A ler dataset...";
  thresholdsLabel.textContent = "A calcular referência...";

  try {
    const data = await fetchJson(`/api/deep-research?dataset_id=${encodeURIComponent(state.selectedDataset)}&limit=${DEEP_RESEARCH_LIMIT}`);
    state.payload = data;
    renderAll();
    statusLabel.textContent = "Análise concluída";
  } catch (error) {
    statusLabel.textContent = "Erro na análise";
    renderError(error);
    console.error(error);
  }
}

function renderAll() {
  const analysis = state.payload?.analysis || state.payload || {};
  const screening = state.payload?.feature_screening || state.payload?.boruta || {};
  const territory = state.payload?.territorial_map || {};
  const rows = screeningRows(screening);
  const confirmed = rows.filter((row) => row.status === "confirmada");
  const warnings = [...(state.payload?.quality_warnings || []), ...(analysis.quality_warnings || [])];
  const thresholds = screening.thresholds || {};

  thresholdsLabel.textContent = `Referência interna: máx. ruído ${formatDecimal(thresholds.max_shadow, 1)} · mediana ${formatDecimal(thresholds.median_shadow, 1)}`;
  mapMeta.textContent = `${analysis.title || state.payload?.title || "Dataset"} · ${formatNumber(analysis.sample_size)} registos lidos · ${analysis.fallback ? "dados indisponíveis" : "amostra real"}.`;

  renderOverview(analysis, rows, warnings);
  renderResultNarrative(analysis, warnings);
  renderKeyFindings(analysis);
  renderNumericResults(analysis);
  renderTrendExplorer(analysis);
  renderCategoryResults(analysis);
  renderAssociations(analysis);
  renderTerritory(territory, analysis);
  renderQuality(analysis, screening, rows, warnings);
  renderConfirmedDrivers(confirmed);
}

function renderOverview(analysis, rows, warnings) {
  const readiness = analysis.analysis_readiness || {};
  const sample = analysis.sample || {};
  const mode = analysis.fallback ? "Indisponível" : "Real";
  const readinessScore = readiness.score == null ? "-" : `${readiness.score}/100`;
  const readinessDetail = [friendlyReadinessLabel(readiness.label), ...(readiness.gaps || ["validar denominador"])].join(" · ");
  const coverageDetail = sample.coverage_ratio == null
    ? "cobertura desconhecida; não extrapolar"
    : `${percent(sample.coverage_ratio)} de cobertura; ${sample.coverage_ratio < 0.75 ? "não extrapolar" : "cobertura razoável"}`;
  const cards = [
    {label: "Dataset", value: compactText(analysis.title || analysis.dataset_id, 64), detail: analysis.dataset_id || "-"},
    {label: "Amostra lida", value: `${formatNumber(analysis.sample_size)} registos`, detail: coverageDetail},
    {label: "Viabilidade", value: readinessScore, detail: readinessDetail},
    {label: "Campos com sinal", value: formatNumber(rows.length), detail: `${rows.filter((row) => row.status === "confirmada").length} fortes · ${warnings.length} aviso(s)`},
    {label: "Dados", value: mode, detail: analysis.warning || "amostra real, leitura exploratória"},
  ];

  cards.forEach((card, index) => {
    const node = el("div", `research-overview-card ${index === 0 ? "is-wide" : ""}`.trim());
    const label = el("span", "", card.label);
    const value = el("strong", "", card.value);
    value.title = card.value;
    const detail = el("small", "", card.detail);
    node.append(label, value, detail);
    overview.appendChild(node);
  });

  if (analysis.fallback || warnings.length) {
    const alert = el("div", "research-alert");
    const strong = el("strong", "", analysis.fallback ? "Dados reais indisponíveis" : "Avisos de qualidade");
    const small = el("small", "", warnings[0] || analysis.warning || "Validar fonte, denominadores e período antes de interpretar.");
    alert.append(strong, small);
    overview.appendChild(alert);
  }
}

function renderResultNarrative(analysis, warnings) {
  const sample = analysis.sample || {};
  const numeric = analysis.numeric_profiles || [];
  const categories = analysis.categorical_profiles || [];
  const trends = analysis.trends || [];
  const correlations = analysis.correlations || [];
  const paragraphs = [];
  const coverage = sample.coverage_ratio == null ? null : percent(sample.coverage_ratio);
  paragraphs.push(
    `A amostra lida contém ${formatNumber(analysis.sample_size)} registos${analysis.total_records ? ` de ${formatNumber(analysis.total_records)} publicados` : ""}${coverage ? ` (${coverage} de cobertura)` : ""}.`
  );
  if (numeric.length) {
    const main = numeric[0];
    paragraphs.push(
      `O principal indicador medido é "${fieldLabel(main)}", com média ${formatMeasure(main.avg)}, mínimo ${formatMeasure(main.min)} e máximo ${formatMeasure(main.max)} na amostra.`
    );
  } else {
    paragraphs.push("Não foram detetadas medidas numéricas suficientemente completas; a leitura fica limitada a categorias e cobertura.");
  }
  if (trends.length) {
    const trend = trends[0];
    paragraphs.push(`Existe eixo temporal para "${fieldLabel(trend)}", agregado por ${trend.aggregation === "soma" ? "soma" : "média"} em ${formatNumber((trend.points || []).length)} períodos.`);
  }
  if (correlations.length) {
    const top = correlations[0];
    paragraphs.push(`A associação mais forte observada liga "${top.label_a}" e "${top.label_b}" (${formatDecimal(top.correlation, 2)}), mas não deve ser lida como causalidade.`);
  }
  if (warnings.length) {
    paragraphs.push(`Travão principal: ${warnings[0]}`);
  }

  paragraphs.forEach((text) => resultNarrative.appendChild(el("p", "", text)));
}

function renderKeyFindings(analysis) {
  const insights = analysis.insights || [];
  if (!insights.length) {
    keyFindings.appendChild(emptyState("Sem síntese automática", "A amostra não gerou achados suficientemente estáveis para resumo."));
    return;
  }
  const visibleInsights = insights.slice(0, RESULT_LIMITS.findings);
  visibleInsights.forEach((insight) => {
    const card = el("article", "research-finding-card");
    card.append(el("span", "", insight.label), el("strong", "", compactText(insight.value, 52)), el("small", "", insight.detail || "validar contexto antes de usar"));
    keyFindings.appendChild(card);
  });
  const omitted = insights.length - visibleInsights.length;
  if (omitted > 0) {
    keyFindings.appendChild(el("small", "research-omitted", `Mostrados ${visibleInsights.length} de ${formatNumber(insights.length)} achados; restantes disponíveis para aprofundamento técnico.`));
  }
}

function renderNumericResults(analysis) {
  const profiles = analysis.numeric_profiles || [];
  if (!profiles.length) {
    numericResults.appendChild(emptyState("Sem indicadores numéricos", "Não há medidas suficientemente completas para comparar valores."));
    return;
  }
  const maxStddev = Math.max(...profiles.map((profile) => Number(profile.stddev || 0)), 1);
  const visibleProfiles = profiles.slice(0, RESULT_LIMITS.numericProfiles);
  visibleProfiles.forEach((profile) => {
    const row = el("article", "research-result-row");
    const head = el("div", "research-result-head");
    head.append(el("strong", "", fieldLabel(profile)), el("span", "", measureRoleLabel(profile.measure_role)));
    const stats = el("div", "research-stat-grid");
    [
      ["Média", formatMeasure(profile.avg)],
      ["Mín.", formatMeasure(profile.min)],
      ["Máx.", formatMeasure(profile.max)],
      ["N", formatNumber(profile.count)],
    ].forEach(([label, value]) => {
      const stat = el("div");
      stat.append(el("span", "", label), el("strong", "", value));
      stats.appendChild(stat);
    });
    const meter = el("div", "research-variance-meter");
    const fill = el("span");
    fill.style.width = `${Math.max(6, Math.min(100, (Number(profile.stddev || 0) / maxStddev) * 100))}%`;
    meter.appendChild(fill);
    const note = el("small", "meta", `Variação relativa na amostra · ${formatNumber(profile.missing)} valores em falta`);
    row.append(head, stats, meter, note);
    numericResults.appendChild(row);
  });
  const omitted = profiles.length - visibleProfiles.length;
  if (omitted > 0) {
    numericResults.appendChild(el("small", "research-omitted", `Mostrados ${visibleProfiles.length} de ${formatNumber(profiles.length)} indicadores; ${formatNumber(omitted)} omitido(s) para leitura compacta.`));
  }
}

function renderTrendExplorer(analysis) {
  const trends = analysis.trends || [];
  if (!trends.length) {
    trendExplorer.appendChild(emptyState("Sem evolução temporal", "O dataset não expõe eixo temporal utilizável nesta amostra."));
    return;
  }
  const trend = trends[0];
  const points = trend.points || [];
  const title = el("div", "research-trend-title");
  title.append(el("strong", "", fieldLabel(trend)), el("small", "", `${points.length} períodos · ${trend.aggregation === "soma" ? "soma" : "média"}`));
  trendExplorer.appendChild(title);
  const values = points.map((point) => Number(point.value)).filter(Number.isFinite);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const chart = el("div", "research-trend-chart");
  points.slice(-12).forEach((point) => {
    const value = Number(point.value);
    const bar = el("div", "research-trend-bar");
    bar.style.height = `${Math.max(10, ((value - min) / range) * 88 + 8)}%`;
    bar.title = `${point.period}: ${formatMeasure(value)}`;
    chart.appendChild(bar);
  });
  trendExplorer.appendChild(chart);
  const last = points[points.length - 1];
  const first = points[0];
  if (first && last) {
    const delta = Number(last.value) - Number(first.value);
    trendExplorer.appendChild(el("p", "research-trend-note", `Do primeiro ao último período lido: ${formatMeasure(first.value)} → ${formatMeasure(last.value)} (${delta >= 0 ? "+" : ""}${formatMeasure(delta)}).`));
  }
  (trend.validation?.warnings || []).slice(0, 2).forEach((warning) => trendExplorer.appendChild(el("div", "research-warning compact", warning)));
}

function renderCategoryResults(analysis) {
  const profiles = (analysis.categorical_profiles || []).filter((profile) => profile.top_values?.length);
  if (!profiles.length) {
    categoryResults.appendChild(emptyState("Sem categorias relevantes", "A amostra não tem campos categóricos com distribuição legível."));
    return;
  }
  const visibleProfiles = profiles.slice(0, RESULT_LIMITS.categoryProfiles);
  visibleProfiles.forEach((profile) => {
    const block = el("article", "research-category-block");
    block.append(el("strong", "", fieldLabel(profile)));
    const max = Math.max(...profile.top_values.map((item) => Number(item.count || 0)), 1);
    profile.top_values.slice(0, RESULT_LIMITS.categoryTopValues).forEach((item) => {
      const row = el("div", "research-category-row");
      row.append(el("span", "", compactText(item.value, 42)));
      const meter = el("i");
      const fill = el("b");
      fill.style.width = `${Math.max(8, (Number(item.count || 0) / max) * 100)}%`;
      meter.appendChild(fill);
      row.append(meter, el("strong", "", formatNumber(item.count)));
      block.appendChild(row);
    });
    categoryResults.appendChild(block);
  });
  const omittedProfiles = profiles.length - visibleProfiles.length;
  if (omittedProfiles > 0) {
    categoryResults.appendChild(el("small", "research-omitted", `Mostrados ${visibleProfiles.length} de ${formatNumber(profiles.length)} dimensões categóricas para leitura rápida.`));
  }
}

function renderAssociations(analysis) {
  const correlations = analysis.correlations || [];
  if (!correlations.length) {
    associationResults.appendChild(emptyState("Sem associações robustas", "Não há pares numéricos suficientes acima do limiar exploratório."));
    return;
  }
  const visibleCorrelations = correlations.slice(0, RESULT_LIMITS.associations);
  visibleCorrelations.forEach((item) => {
    const row = el("div", "research-association");
    row.append(
      el("strong", "", `${compactText(item.label_a, 34)} ↔ ${compactText(item.label_b, 34)}`),
      el("span", "", `${formatDecimal(item.correlation, 2)} · ${item.strength || "associação"} · ${formatNumber(item.samples)} pares`),
      el("small", "", (item.warnings || ["não interpretar como causalidade"])[0])
    );
    associationResults.appendChild(row);
  });
  const omitted = correlations.length - visibleCorrelations.length;
  if (omitted > 0) {
    associationResults.appendChild(el("small", "research-omitted", `Mostradas ${visibleCorrelations.length} de ${formatNumber(correlations.length)} associações com maior evidência.`));
  }
}

function renderFeatures(rows) {
  if (!rows.length) {
    featureList.appendChild(emptyState("Sem campos fortes", "A amostra não tem campos com sinal suficiente para triagem."));
    return;
  }

  rows.forEach((feature) => {
    const item = el("button", `boruta-item research-feature-card is-${feature.status}`);
    item.type = "button";
    const top = el("div", "boruta-label");
    const name = el("span", "", feature.label || feature.field || "Campo");
    name.title = feature.label || feature.field || "Campo";
    const badge = el("span", `status-tag ${feature.status}`, STATUS_LABELS[feature.status] || feature.status);
    top.append(name, badge);
    const score = el("div", "boruta-meta", `${feature.kind || "campo"} · força ${formatDecimal(feature.score || 0, 0)}`);
    const drivers = el("div", "boruta-meta", (feature.drivers || []).join(" · ") || "sem drivers explícitos");
    item.append(top, score, drivers);
    featureList.appendChild(item);
  });
}

function renderTerritory(territory, analysis) {
  const regions = territoryEntries(territory.regions);
  const uls = territoryEntries(territory.uls);
  const entities = territoryEntries(territory.entities);
  const total = regions.reduce((sum, item) => sum + item.count, 0);
  const leader = regions[0];

  const summary = [
    {label: "Regiões", value: regions.length || "-", detail: leader ? `maior sinal: ${leader.name}` : "sem match regional"},
    {label: "Observações", value: total ? formatNumber(total) : "-", detail: "contagem na amostra analisada"},
    {label: "ULS", value: uls.length || "-", detail: uls.length ? "detetadas na metadata" : "sem campo ULS reconhecido"},
  ];
  summary.forEach((item) => {
    const card = el("div", "research-territory-card");
    card.append(el("span", "", item.label), el("strong", "", item.value), el("small", "", item.detail));
    territorySummary.appendChild(card);
  });

  const visibleRegions = regions.slice(0, RESULT_LIMITS.territories);
  if (!visibleRegions.length) {
    territoryBars.appendChild(emptyState("Sem sinal regional", "A amostra não expõe região/ARS reconhecível. Usar entidade ou ULS se existirem."));
  } else {
    const max = Math.max(...visibleRegions.map((item) => item.count), 1);
    visibleRegions.forEach((item, index) => {
      const row = el("div", "territory-row");
      const label = el("div", "territory-label");
      label.append(el("strong", "", item.name), el("small", "", `${formatNumber(item.count)} ocorrências`));
      const meter = el("div", "territory-meter");
      const fill = el("span");
      fill.style.width = `${Math.max(8, (item.count / max) * 100)}%`;
      fill.style.opacity = String(Math.max(0.45, 1 - index * 0.12));
      meter.appendChild(fill);
      row.append(label, meter);
      territoryBars.appendChild(row);
    });
    const omittedRegions = regions.length - visibleRegions.length;
    if (omittedRegions > 0) {
      territoryBars.appendChild(el("small", "research-omitted", `${formatNumber(omittedRegions)} regiões com menor sinal ocultadas para evitar sobrecarga visual.`));
    }
  }

  [...entities.slice(0, 6), ...uls.slice(0, 4)].forEach((item) => {
    const chip = el("span", "research-chip", `${item.name} · ${formatNumber(item.count)}`);
    territoryEntities.appendChild(chip);
  });
  if (!territoryEntities.children.length) {
    territoryEntities.appendChild(el("span", "research-chip is-muted", "Sem entidade reconhecida"));
  }

  const readout = analysis.fallback
    ? "Sem dados reais para esta leitura. A página não substitui por dados fictícios."
    : "Usar como hipótese territorial. Confirmar cobertura, nomes oficiais e granularidade antes de comparar regiões.";
  territoryReadout.appendChild(el("p", "", readout));
}

function renderQuality(analysis, screening, rows, warnings) {
  const confirmed = rows.filter((row) => row.status === "confirmada").length;
  const tentative = rows.filter((row) => row.status === "tentativa").length;
  const method = screening.method || "feature_screening";
  const cards = [
    ["Sinais fortes", confirmed, "campos acima da referência de ruído"],
    ["A confirmar", tentative, "sinal útil, mas ainda frágil"],
    ["Avisos de qualidade", warnings.length, warnings.length ? "limitam a interpretação" : "sem bloqueio crítico"],
    ["Método", screeningMethodLabel(method), screeningMethodDetail(screening, analysis), method],
  ];
  cards.forEach(([label, value, detail, title]) => {
    const card = el("div", "research-kpi-card");
    const strong = el("strong", "", String(value));
    if (title) strong.title = title;
    card.append(el("span", "", label), strong, el("small", "", detail));
    kpiGrid.appendChild(card);
  });

  const visibleWarnings = warnings.length ? [...new Set(warnings)].slice(0, 5) : ["Sem avisos críticos na amostra atual; ainda assim validar denominador, período e fonte."];
  visibleWarnings.forEach((warning) => {
    const item = el("div", "research-warning", warning);
    warningList.appendChild(item);
  });
}

function renderConfirmedDrivers(rows) {
  if (!rows.length) {
    confirmedDrivers.appendChild(emptyState("Sem drivers confirmados", "Usar os sinais a rever apenas como triagem."));
    return;
  }

  rows.slice(0, 6).forEach((feature) => {
    const item = el("div", "research-driver");
    const title = el("strong", "boruta-driver-title", feature.label || feature.field || "Campo");
    const detail = el("small", "meta", (feature.drivers || [])[0] || "sinal acima da referência de ruído");
    item.append(title, detail);
    confirmedDrivers.appendChild(item);
  });
}

function emptyState(title, detail) {
  const wrap = el("div", "empty-state compact");
  wrap.append(el("strong", "", title), el("small", "", detail));
  return wrap;
}

function renderError(error) {
  const alert = el("div", "research-alert is-error");
  alert.append(el("strong", "", "Investigação indisponível"), el("small", "", error.message || "Falha ao carregar dados."));
  overview.appendChild(alert);
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

init().catch((error) => {
  statusLabel.textContent = "Erro a iniciar";
  renderError(error);
  console.error(error);
});
