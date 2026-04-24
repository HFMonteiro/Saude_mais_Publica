const state = {
  payload: null,
  filteredCorrelations: [],
  minScore: 4,
  search: "",
  kind: "",
  confidence: "",
  lowRiskOnly: false,
  selectedCorrelationKey: "",
  selectedDataDataset: "",
  dataLimit: 80,
  dataPayload: null,
};

const analyticsSearch = document.getElementById("analyticsSearch");
const analyticsKind = document.getElementById("analyticsKind");
const analyticsConfidence = document.getElementById("analyticsConfidence");
const analyticsMinScore = document.getElementById("analyticsMinScore");
const analyticsMinScoreValue = document.getElementById("analyticsMinScoreValue");
const analyticsRefresh = document.getElementById("analyticsRefresh");
const analyticsExport = document.getElementById("analyticsExport");
const analyticsStatus = document.getElementById("analyticsStatus");
const dataDatasetSelect = document.getElementById("dataDatasetSelect");
const dataSampleLimit = document.getElementById("dataSampleLimit");
const dataSampleLimitValue = document.getElementById("dataSampleLimitValue");
const dataRefreshButton = document.getElementById("dataRefreshButton");
const dataAnalyticsStatus = document.getElementById("dataAnalyticsStatus");
const dataAnalyticsMeta = document.getElementById("dataAnalyticsMeta");
const dataStatRows = document.getElementById("dataStatRows");
const dataStatNumeric = document.getElementById("dataStatNumeric");
const dataStatCategorical = document.getElementById("dataStatCategorical");
const dataStatCorrelation = document.getElementById("dataStatCorrelation");
const dataInsightCards = document.getElementById("dataInsightCards");
const dataCorrelationList = document.getElementById("dataCorrelationList");
const dataTrendChart = document.getElementById("dataTrendChart");
const dataNumericProfiles = document.getElementById("dataNumericProfiles");
const dataCategoricalProfiles = document.getElementById("dataCategoricalProfiles");
const dataFeatureImportance = document.getElementById("dataFeatureImportance");
const dataPcaMeta = document.getElementById("dataPcaMeta");
const dataPcaChart = document.getElementById("dataPcaChart");
const statDatasets = document.getElementById("analyticsStatDatasets");
const statLinks = document.getElementById("analyticsStatLinks");
const statDimensions = document.getElementById("analyticsStatDimensions");
const statConfidence = document.getElementById("analyticsStatConfidence");
const chartMeta = document.getElementById("analyticsChartMeta");
const insightCards = document.getElementById("analyticsInsightCards");
const distributionMeta = document.getElementById("analyticsDistributionMeta");
const distribution = document.getElementById("analyticsDistribution");
const modelMeta = document.getElementById("analyticsModelMeta");
const semanticModel = document.getElementById("analyticsSemanticModel");
const publicHealthMeta = document.getElementById("publicHealthMeta");
const publicHealthMatrix = document.getElementById("publicHealthMatrix");
const publicHealthStrata = document.getElementById("publicHealthStrata");
const bubbleChart = document.getElementById("analyticsBubbleChart");
const dimensionList = document.getElementById("analyticsDimensionList");
const themeMatrix = document.getElementById("analyticsThemeMatrix");
const dimensionMatrix = document.getElementById("analyticsDimensionMatrix");
const correlationMeta = document.getElementById("analyticsCorrelationMeta");
const correlationTable = document.getElementById("analyticsCorrelationTable");
const SVG_NS = "http://www.w3.org/2000/svg";

let searchTimer = null;
let loadTimer = null;
let dataLoadTimer = null;
let activeRequest = 0;
let activeDataRequest = 0;

function clearElement(element) {
  element.replaceChildren();
}

function safeText(value) {
  return value == null ? "" : String(value);
}

function formatNumber(value) {
  return new Intl.NumberFormat("pt-PT").format(value || 0);
}

function compactTitle(value, max = 58) {
  const text = safeText(value);
  return text.length > max ? `${text.slice(0, max - 1)}...` : text;
}

function kindLabel(kind) {
  return {
    temporal: "Temporal",
    territorial: "Território",
    entidade: "Entidade",
    medida: "Medida",
    generico: "Genérico",
  }[kind] || "Genérico";
}

function confidenceLabel(value) {
  return {
    alta: "Alta",
    media: "Média",
    exploratoria: "Exploratória",
  }[value] || value;
}

function likelihoodLabel(value) {
  return {
    alta: "Alta",
    media: "Média",
    baixa: "Baixa",
  }[value] || value;
}

function impactLabel(value) {
  return {
    alto: "Alto",
    medio: "Médio",
    baixo: "Baixo",
  }[value] || value;
}

function formatDecimal(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return new Intl.NumberFormat("pt-PT", {maximumFractionDigits: digits}).format(Number(value));
}

function relationKey(item) {
  return `${item.source}|${item.target}`;
}

function dominantEntry(entries) {
  return entries.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0] || ["-", 0];
}

function svgNode(tag, attributes = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  return node;
}

async function loadAnalytics() {
  const requestId = ++activeRequest;
  analyticsStatus.textContent = "A carregar...";
  const response = await fetch(`/api/analytics?min_score=${state.minScore}`);
  const payload = await response.json();
  if (requestId !== activeRequest) {
    return;
  }
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao carregar analytics");
  }
  state.payload = payload;
  populateDataDatasetOptions();
  analyticsStatus.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  renderAll();
  if (state.selectedDataDataset && !state.dataPayload) {
    loadDataAnalytics().catch(showDataError);
  }
}

function debounceLoadAnalytics() {
  clearTimeout(loadTimer);
  loadTimer = setTimeout(() => {
    loadAnalytics().catch(showError);
  }, 160);
}

function populateDataDatasetOptions() {
  const datasets = (state.payload?.datasets || []).slice().sort((a, b) => {
    const metricDelta = (b.metric_candidate_count || 0) - (a.metric_candidate_count || 0);
    if (metricDelta !== 0) return metricDelta;
    const fieldDelta = (b.field_count || 0) - (a.field_count || 0);
    if (fieldDelta !== 0) return fieldDelta;
    const recordDelta = (b.records_count || 0) - (a.records_count || 0);
    if (recordDelta !== 0) return recordDelta;
    return safeText(a.title).localeCompare(safeText(b.title));
  });
  const current = state.selectedDataDataset;
  clearElement(dataDatasetSelect);
  datasets.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = `${compactTitle(dataset.title || dataset.dataset_id, 72)} · ${dataset.mega_theme} · ${formatNumber(dataset.metric_candidate_count || 0)} medidas`;
    dataDatasetSelect.appendChild(option);
  });
  if (current && datasets.some((dataset) => dataset.dataset_id === current)) {
    dataDatasetSelect.value = current;
  } else {
    const firstWithRecords = datasets.find((dataset) => (dataset.records_count || 0) > 0) || datasets[0];
    state.selectedDataDataset = firstWithRecords?.dataset_id || "";
    dataDatasetSelect.value = state.selectedDataDataset;
  }
}

async function loadDataAnalytics() {
  if (!state.selectedDataDataset) return;
  const requestId = ++activeDataRequest;
  dataAnalyticsStatus.textContent = "A analisar amostra...";
  const response = await fetch(`/api/data-analytics?dataset_id=${encodeURIComponent(state.selectedDataDataset)}&limit=${state.dataLimit}`);
  const payload = await response.json();
  if (requestId !== activeDataRequest) return;
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao analisar dados");
  }
  state.dataPayload = payload;
  dataAnalyticsStatus.textContent = `Amostra atualizada · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  renderDataAnalytics();
}

function debounceLoadDataAnalytics() {
  clearTimeout(dataLoadTimer);
  dataLoadTimer = setTimeout(() => {
    loadDataAnalytics().catch(showDataError);
  }, 180);
}

function filteredCorrelations() {
  const payload = state.payload;
  if (!payload) return [];
  const search = state.search.trim().toLowerCase();
  return (payload.correlations || []).filter((item) => {
    if (state.confidence && item.confidence !== state.confidence) return false;
    if (state.kind && !(item.dimension_kinds || []).includes(state.kind)) return false;
    if (state.lowRiskOnly && (item.risk_flags || []).length) return false;
    if (!search) return true;
    const text = [
      item.source_title,
      item.target_title,
      item.source_theme,
      item.target_theme,
      ...(item.shared_fields || []),
      ...(item.dimension_kinds || []),
    ].join(" ").toLowerCase();
    return text.includes(search);
  });
}

function filteredDimensions() {
  const payload = state.payload;
  if (!payload) return [];
  const search = state.search.trim().toLowerCase();
  return (payload.dimensions || []).filter((item) => {
    if (state.kind && item.kind !== state.kind) return false;
    if (!search) return true;
    return `${item.field} ${item.kind}`.toLowerCase().includes(search);
  });
}

function renderSummary() {
  const summary = state.payload?.summary || {};
  const correlations = filteredCorrelations();
  const dimensions = filteredDimensions();
  statDatasets.textContent = formatNumber(summary.dataset_count);
  statLinks.textContent = `${formatNumber(correlations.length)}/${formatNumber(summary.link_count)}`;
  statDimensions.textContent = `${formatNumber(dimensions.length)}/${formatNumber(summary.dimension_count)}`;
  statConfidence.textContent = formatNumber(correlations.filter((item) => item.confidence === "alta").length);
}

function renderDataAnalytics() {
  const payload = state.dataPayload;
  if (!payload) {
    dataAnalyticsMeta.textContent = "Escolhe um dataset para calcular estatísticas reais dos registos publicados.";
    dataStatRows.textContent = "-";
    dataStatNumeric.textContent = "-";
    dataStatCategorical.textContent = "-";
    dataStatCorrelation.textContent = "-";
    clearElement(dataInsightCards);
    clearElement(dataCorrelationList);
    clearElement(dataNumericProfiles);
    clearElement(dataCategoricalProfiles);
    clearElement(dataTrendChart);
    clearElement(dataFeatureImportance);
    clearElement(dataPcaChart);
    dataPcaMeta.textContent = "Projeção das medidas numéricas para encontrar campos que explicam maior variação.";
    return;
  }

  dataAnalyticsMeta.textContent = `${payload.title} · ${formatNumber(payload.sample_size)} registos amostrados${payload.temporal_field ? ` · eixo temporal: ${payload.temporal_field}` : ""}.`;
  dataStatRows.textContent = formatNumber(payload.sample_size);
  dataStatNumeric.textContent = formatNumber((payload.numeric_profiles || []).length);
  dataStatCategorical.textContent = formatNumber((payload.categorical_profiles || []).length);
  dataStatCorrelation.textContent = payload.correlations?.[0] ? formatDecimal(payload.correlations[0].correlation, 2) : "-";
  renderDataInsights(payload);
  renderDataCorrelations(payload);
  renderDataTrend(payload);
  renderNumericProfiles(payload);
  renderCategoricalProfiles(payload);
  renderFeatureImportance(payload);
  renderPcaChart(payload);
}

function renderDataInsights(payload) {
  clearElement(dataInsightCards);
  const insights = payload.insights || [];
  if (!insights.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "A amostra ainda não tem sinais estatísticos fortes.";
    dataInsightCards.appendChild(empty);
    return;
  }
  insights.forEach((insight) => {
    const card = document.createElement("div");
    card.className = "data-insight-card";
    const label = document.createElement("span");
    label.textContent = insight.label;
    const value = document.createElement("strong");
    value.textContent = insight.value;
    const detail = document.createElement("small");
    detail.textContent = insight.detail;
    card.append(label, value, detail);
    dataInsightCards.appendChild(card);
  });
}

function renderDataCorrelations(payload) {
  clearElement(dataCorrelationList);
  const rows = payload.correlations || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem correlações numéricas relevantes nesta amostra.";
    dataCorrelationList.appendChild(empty);
    return;
  }
  rows.slice(0, 10).forEach((row) => {
    const item = document.createElement("div");
    item.className = row.correlation >= 0 ? "data-correlation-row is-positive" : "data-correlation-row is-negative";
    const label = document.createElement("span");
    label.textContent = `${compactTitle(row.label_a, 28)} / ${compactTitle(row.label_b, 28)}`;
    const bar = document.createElement("i");
    bar.style.width = `${Math.max(4, row.abs_correlation * 100)}%`;
    const value = document.createElement("strong");
    value.textContent = `${formatDecimal(row.correlation, 2)} · ${formatNumber(row.samples)}`;
    item.append(label, bar, value);
    dataCorrelationList.appendChild(item);
  });
}

function renderFeatureImportance(payload) {
  clearElement(dataFeatureImportance);
  const rows = payload.feature_importance || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem dados-chave suficientes nesta amostra.";
    dataFeatureImportance.appendChild(empty);
    return;
  }
  const maxScore = Math.max(...rows.map((row) => row.score), 1);
  rows.slice(0, 9).forEach((row, index) => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `feature-row is-${row.kind} is-${row.selection}`;
    const rank = document.createElement("span");
    rank.className = "feature-rank";
    rank.textContent = String(index + 1).padStart(2, "0");
    const body = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = row.label || row.field;
    const meta = document.createElement("small");
    meta.textContent = `${row.kind} · ${row.selection} · ${row.drivers?.slice(0, 2).join(" · ") || "sinal exploratório"}`;
    const bar = document.createElement("i");
    bar.style.width = `${Math.max(6, (row.score / maxScore) * 100)}%`;
    body.append(title, meta, bar);
    const score = document.createElement("em");
    score.textContent = formatDecimal(row.score, 1);
    item.append(rank, body, score);
    item.addEventListener("click", () => {
      analyticsSearch.value = row.field;
      state.search = row.field;
      renderAll();
    });
    dataFeatureImportance.appendChild(item);
  });
}

function renderPcaChart(payload) {
  clearElement(dataPcaChart);
  const pca = payload.pca_summary || {};
  const width = Math.max(420, dataPcaChart.closest(".pca-wrap")?.clientWidth || 420);
  const height = 280;
  dataPcaChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  if (!pca.available || !pca.loadings?.length) {
    dataPcaMeta.textContent = pca.reason || "PCA indisponível para esta amostra.";
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "PCA requer pelo menos duas medidas numéricas.";
    dataPcaChart.appendChild(empty);
    return;
  }
  const pc1 = Math.round((pca.explained_variance?.pc1 || 0) * 100);
  const pc2 = Math.round((pca.explained_variance?.pc2 || 0) * 100);
  dataPcaMeta.textContent = `PC1 explica ${pc1}% · PC2 explica ${pc2}% da variação padronizada.`;

  const left = 46;
  const right = width - 28;
  const top = 24;
  const bottom = height - 38;
  const cx = (left + right) / 2;
  const cy = (top + bottom) / 2;
  const scale = Math.min(right - left, bottom - top) * 0.42;

  const axis = svgNode("g", {class: "pca-axis"});
  axis.appendChild(svgNode("line", {x1: left, y1: cy, x2: right, y2: cy}));
  axis.appendChild(svgNode("line", {x1: cx, y1: top, x2: cx, y2: bottom}));
  const xLabel = svgNode("text", {x: right, y: cy - 8, "text-anchor": "end"});
  xLabel.textContent = `PC1 ${pc1}%`;
  const yLabel = svgNode("text", {x: cx + 8, y: top + 10});
  yLabel.textContent = `PC2 ${pc2}%`;
  axis.append(xLabel, yLabel);
  dataPcaChart.appendChild(axis);

  pca.loadings.slice(0, 8).forEach((item) => {
    const x = cx + item.pc1 * scale;
    const y = cy - item.pc2 * scale;
    const group = svgNode("g", {class: "pca-point", transform: `translate(${x}, ${y})`});
    group.appendChild(svgNode("circle", {r: Math.max(5, Math.min(14, item.magnitude * 12))}));
    const label = svgNode("text", {x: 10, y: 4});
    label.textContent = compactTitle(item.label, 20);
    group.appendChild(label);
    dataPcaChart.appendChild(group);
  });
}

function renderNumericProfiles(payload) {
  clearElement(dataNumericProfiles);
  const rows = payload.numeric_profiles || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem medidas numéricas consistentes nesta amostra.";
    dataNumericProfiles.appendChild(empty);
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "data-profile-row";
    const title = document.createElement("strong");
    title.textContent = row.label || row.field;
    const meta = document.createElement("small");
    meta.textContent = `${formatNumber(row.count)} valores · ${formatNumber(row.missing)} em falta`;
    const stats = document.createElement("div");
    stats.className = "data-profile-stats";
    [
      ["média", formatDecimal(row.avg, 2)],
      ["min", formatDecimal(row.min, 2)],
      ["max", formatDecimal(row.max, 2)],
      ["desvio", formatDecimal(row.stddev, 2)],
    ].forEach(([label, value]) => {
      const chip = document.createElement("span");
      chip.textContent = `${label}: ${value}`;
      stats.appendChild(chip);
    });
    item.append(title, meta, stats);
    dataNumericProfiles.appendChild(item);
  });
}

function renderCategoricalProfiles(payload) {
  clearElement(dataCategoricalProfiles);
  const rows = payload.categorical_profiles || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem dimensões categóricas úteis nesta amostra.";
    dataCategoricalProfiles.appendChild(empty);
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "data-profile-row";
    const title = document.createElement("strong");
    title.textContent = row.label || row.field;
    const meta = document.createElement("small");
    meta.textContent = `${formatNumber(row.unique)} categorias · ${formatNumber(row.missing)} em falta`;
    const values = document.createElement("div");
    values.className = "data-category-values";
    const max = Math.max(...(row.top_values || []).map((value) => value.count), 1);
    (row.top_values || []).slice(0, 5).forEach((value) => {
      const valueRow = document.createElement("span");
      const name = document.createElement("b");
      name.textContent = compactTitle(value.value, 36);
      const bar = document.createElement("i");
      bar.style.width = `${Math.max(4, (value.count / max) * 100)}%`;
      const count = document.createElement("em");
      count.textContent = formatNumber(value.count);
      valueRow.append(name, bar, count);
      values.appendChild(valueRow);
    });
    item.append(title, meta, values);
    dataCategoricalProfiles.appendChild(item);
  });
}

function renderDataTrend(payload) {
  clearElement(dataTrendChart);
  const trend = payload.trends?.[0];
  const width = Math.max(560, dataTrendChart.closest(".data-trend-wrap")?.clientWidth || 560);
  const height = 260;
  dataTrendChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  if (!trend || !trend.points?.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem eixo temporal suficiente para tendência.";
    dataTrendChart.appendChild(empty);
    return;
  }
  const points = trend.points;
  const values = points.map((point) => point.avg);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const left = 44;
  const right = width - 22;
  const top = 24;
  const bottom = height - 42;
  const xStep = points.length > 1 ? (right - left) / (points.length - 1) : 0;
  const coords = points.map((point, index) => ({
    x: left + index * xStep,
    y: bottom - ((point.avg - min) / span) * (bottom - top),
    point,
  }));
  const axis = svgNode("g", {class: "data-trend-axis"});
  axis.appendChild(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom}));
  axis.appendChild(svgNode("line", {x1: left, y1: top, x2: left, y2: bottom}));
  const title = svgNode("text", {x: left, y: 16});
  title.textContent = compactTitle(trend.label, 48);
  axis.appendChild(title);
  dataTrendChart.appendChild(axis);
  const path = svgNode("path", {
    class: "data-trend-line",
    d: coords.map((coord, index) => `${index ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  });
  dataTrendChart.appendChild(path);
  coords.forEach((coord) => {
    const group = svgNode("g", {class: "data-trend-point", transform: `translate(${coord.x}, ${coord.y})`});
    group.appendChild(svgNode("circle", {r: 4}));
    const label = svgNode("text", {"text-anchor": "middle", y: 19});
    label.textContent = coord.point.period;
    group.appendChild(label);
    dataTrendChart.appendChild(group);
  });
}

function addInsightCard({label, value, meta, tone = "", active = false, onClick}) {
  const card = document.createElement("button");
  card.type = "button";
  card.className = `analytics-insight-card ${tone} ${active ? "is-active" : ""}`.trim();
  const top = document.createElement("span");
  top.textContent = label;
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = meta;
  card.append(top, strong, small);
  if (onClick) {
    card.addEventListener("click", onClick);
  }
  return card;
}

function renderInsightCards() {
  clearElement(insightCards);
  const correlations = filteredCorrelations();
  const dimensions = filteredDimensions();
  const topDimension = dimensions[0];
  const topCorrelation = correlations[0];
  const lowRiskCount = correlations.filter((item) => !(item.risk_flags || []).length).length;
  const kindCounts = new Map();
  correlations.forEach((item) => {
    (item.dimension_kinds || ["generico"]).forEach((kind) => {
      kindCounts.set(kind, (kindCounts.get(kind) || 0) + 1);
    });
  });
  const [dominantKind, dominantKindCount] = dominantEntry([...kindCounts.entries()]);

  insightCards.appendChild(
    addInsightCard({
      label: "Dimensão líder",
      value: topDimension ? compactTitle(topDimension.field, 32) : "-",
      meta: topDimension ? `${formatNumber(topDimension.dataset_count)} datasets · ${kindLabel(topDimension.kind)}` : "Sem dimensão filtrada",
      tone: topDimension ? `analytics-kind-${topDimension.kind}` : "",
      onClick: topDimension
        ? () => {
            analyticsSearch.value = topDimension.field;
            state.search = topDimension.field;
            renderAll();
          }
        : null,
    }),
  );

  insightCards.appendChild(
    addInsightCard({
      label: "Par mais forte",
      value: topCorrelation ? compactTitle(topCorrelation.source_title, 26) : "-",
      meta: topCorrelation ? `${compactTitle(topCorrelation.target_title, 26)} · score ${topCorrelation.semantic_score}` : "Sem relação filtrada",
      tone: topCorrelation ? `confidence-${topCorrelation.confidence}` : "",
      onClick: topCorrelation
        ? () => {
            state.selectedCorrelationKey = relationKey(topCorrelation);
            analyticsSearch.value = "";
            state.search = "";
            renderAll();
          }
        : null,
    }),
  );

  insightCards.appendChild(
    addInsightCard({
      label: "Juntas limpas",
      value: formatNumber(lowRiskCount),
      meta: state.lowRiskOnly ? "Filtro ativo · clicar para limpar" : `${formatNumber(correlations.length)} relações visíveis`,
      tone: lowRiskCount ? "confidence-alta" : "confidence-exploratoria",
      active: state.lowRiskOnly,
      onClick: () => {
        state.lowRiskOnly = !state.lowRiskOnly;
        renderAll();
      },
    }),
  );

  insightCards.appendChild(
    addInsightCard({
      label: "Mix dominante",
      value: kindLabel(dominantKind),
      meta: `${formatNumber(dominantKindCount)} ocorrências nas relações`,
      tone: `analytics-kind-${dominantKind}`,
      onClick: dominantKind !== "-"
        ? () => {
            state.kind = dominantKind;
            analyticsKind.value = dominantKind;
            renderAll();
          }
        : null,
    }),
  );
}

function renderDistribution() {
  clearElement(distribution);
  const correlations = filteredCorrelations();
  distributionMeta.textContent = `${formatNumber(correlations.length)} relações alimentam esta distribuição.`;
  const confidenceRows = [
    ["alta", "Alta"],
    ["media", "Média"],
    ["exploratoria", "Exploratória"],
  ].map(([key, label]) => ({
    key,
    label,
    count: correlations.filter((item) => item.confidence === key).length,
    className: `confidence-${key}`,
  }));
  const kindCounts = new Map();
  correlations.forEach((item) => {
    (item.dimension_kinds || ["generico"]).forEach((kind) => kindCounts.set(kind, (kindCounts.get(kind) || 0) + 1));
  });
  const kindRows = ["temporal", "territorial", "entidade", "medida", "generico"].map((kind) => ({
    key: kind,
    label: kindLabel(kind),
    count: kindCounts.get(kind) || 0,
    className: `analytics-kind-${kind}`,
  }));

  [
    ["Confiança", confidenceRows],
    ["Dimensões", kindRows],
  ].forEach(([titleText, rows]) => {
    const group = document.createElement("div");
    group.className = "analytics-distribution-group";
    const title = document.createElement("h3");
    title.textContent = titleText;
    group.appendChild(title);
    const max = Math.max(...rows.map((row) => row.count), 1);
    rows.forEach((row) => {
      const item = document.createElement("button");
      item.type = "button";
      item.className = `analytics-distribution-row ${row.className}`;
      const label = document.createElement("span");
      label.textContent = row.label;
      const bar = document.createElement("i");
      bar.style.width = `${Math.max(3, (row.count / max) * 100)}%`;
      const value = document.createElement("strong");
      value.textContent = formatNumber(row.count);
      item.append(label, bar, value);
      item.addEventListener("click", () => {
        if (titleText === "Confiança") {
          state.confidence = state.confidence === row.key ? "" : row.key;
          analyticsConfidence.value = state.confidence;
        } else {
          state.kind = state.kind === row.key ? "" : row.key;
          analyticsKind.value = state.kind;
        }
        renderAll();
      });
      group.appendChild(item);
    });
    distribution.appendChild(group);
  });
}

function renderSemanticModel() {
  clearElement(semanticModel);
  const correlations = filteredCorrelations().slice(0, 80);
  const width = Math.max(760, semanticModel.closest(".analytics-model-wrap")?.clientWidth || 760);
  const height = 330;
  semanticModel.setAttribute("viewBox", `0 0 ${width} ${height}`);
  modelMeta.textContent = `${formatNumber(correlations.length)} relações filtradas · temas no arco exterior, dimensões no núcleo.`;

  if (!correlations.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem relações para desenhar o modelo.";
    semanticModel.appendChild(empty);
    return;
  }

  const themeCounts = new Map();
  const kindCounts = new Map();
  const edges = [];
  correlations.forEach((item) => {
    [item.source_theme, item.target_theme].forEach((theme) => themeCounts.set(theme, (themeCounts.get(theme) || 0) + 1));
    (item.dimension_kinds || ["generico"]).forEach((kind) => {
      kindCounts.set(kind, (kindCounts.get(kind) || 0) + 1);
      edges.push({theme: item.source_theme, kind, confidence: item.confidence});
      edges.push({theme: item.target_theme, kind, confidence: item.confidence});
    });
  });

  const themes = [...themeCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 7);
  const kinds = [...kindCounts.entries()].sort((a, b) => b[1] - a[1]);
  const cx = width / 2;
  const cy = height / 2 + 8;
  const rx = Math.min(width * 0.38, 360);
  const ry = 118;
  const themeNodes = themes.map(([theme, count], index) => {
    const angle = (-Math.PI * 0.88) + (themes.length === 1 ? 0 : (index / (themes.length - 1)) * Math.PI * 1.76);
    return {id: theme, label: theme, count, x: cx + Math.cos(angle) * rx, y: cy + Math.sin(angle) * ry, type: "theme"};
  });
  const kindNodes = kinds.map(([kind, count], index) => {
    const offset = (index - (kinds.length - 1) / 2) * 104;
    return {id: kind, label: kindLabel(kind), count, x: cx + offset, y: cy + 44, type: "kind"};
  });
  const themeById = new Map(themeNodes.map((node) => [node.id, node]));
  const kindById = new Map(kindNodes.map((node) => [node.id, node]));
  const edgeMap = new Map();
  edges.forEach((edge) => {
    if (!themeById.has(edge.theme) || !kindById.has(edge.kind)) return;
    const key = `${edge.theme}|${edge.kind}`;
    const current = edgeMap.get(key) || {theme: edge.theme, kind: edge.kind, count: 0, high: 0};
    current.count += 1;
    if (edge.confidence === "alta") current.high += 1;
    edgeMap.set(key, current);
  });

  const edgeLayer = svgNode("g", {class: "analytics-model-edges"});
  [...edgeMap.values()].forEach((edge) => {
    const theme = themeById.get(edge.theme);
    const kind = kindById.get(edge.kind);
    const path = svgNode("path", {
      d: `M ${theme.x} ${theme.y} C ${theme.x} ${cy - 16}, ${kind.x} ${cy - 16}, ${kind.x} ${kind.y}`,
      "stroke-width": Math.max(1.2, Math.min(7, Math.sqrt(edge.count))),
      class: edge.high ? "is-strong" : "",
    });
    edgeLayer.appendChild(path);
  });
  semanticModel.appendChild(edgeLayer);

  const nodeLayer = svgNode("g", {class: "analytics-model-nodes"});
  themeNodes.forEach((node) => {
    const group = svgNode("g", {class: "analytics-model-node theme-node", transform: `translate(${node.x}, ${node.y})`});
    group.appendChild(svgNode("circle", {r: Math.max(17, Math.min(32, Math.sqrt(node.count) * 4.2))}));
    const text = svgNode("text", {"text-anchor": "middle", y: 4});
    text.textContent = compactTitle(node.label, 15);
    group.appendChild(text);
    group.addEventListener("click", () => {
      analyticsSearch.value = node.label;
      state.search = node.label;
      renderAll();
    });
    nodeLayer.appendChild(group);
  });
  kindNodes.forEach((node) => {
    const group = svgNode("g", {class: `analytics-model-node kind-node analytics-kind-${node.id}`, transform: `translate(${node.x}, ${node.y})`});
    group.appendChild(svgNode("rect", {x: -43, y: -17, width: 86, height: 34, rx: 8}));
    const text = svgNode("text", {"text-anchor": "middle", y: 4});
    text.textContent = node.label;
    group.appendChild(text);
    group.addEventListener("click", () => {
      state.kind = node.id;
      analyticsKind.value = node.id;
      renderAll();
    });
    nodeLayer.appendChild(group);
  });
  semanticModel.appendChild(nodeLayer);
}

function summarizePublicHealthCells(correlations) {
  const cells = new Map();
  correlations.forEach((item) => {
    const model = item.public_health_model || {};
    const likelihood = model.likelihood?.level || "baixa";
    const impact = model.impact?.level || "baixo";
    const key = `${likelihood}|${impact}`;
    const cell = cells.get(key) || {
      likelihood,
      impact,
      count: 0,
      scoreSum: 0,
      areas: new Map(),
      drivers: new Map(),
      examples: [],
    };
    cell.count += 1;
    cell.scoreSum += Number(item.semantic_score || 0);
    (model.matched_areas || ["Area nao especificada"]).forEach((area) => {
      cell.areas.set(area, (cell.areas.get(area) || 0) + 1);
    });
    (model.impact?.drivers || []).forEach((driver) => {
      cell.drivers.set(driver, (cell.drivers.get(driver) || 0) + 1);
    });
    if (cell.examples.length < 3) {
      cell.examples.push(`${compactTitle(item.source_title, 26)} / ${compactTitle(item.target_title, 26)}`);
    }
    cells.set(key, cell);
  });
  return cells;
}

function renderPublicHealthMatrix() {
  clearElement(publicHealthMatrix);
  clearElement(publicHealthStrata);
  const correlations = filteredCorrelations();
  const cells = summarizePublicHealthCells(correlations);
  publicHealthMeta.textContent = `${formatNumber(correlations.length)} relações filtradas posicionadas por likelihood técnica, impact em saúde pública e área matched.`;

  const likelihoodLevels = ["alta", "media", "baixa"];
  const impactLevels = ["baixo", "medio", "alto"];
  const maxCount = Math.max(...[...cells.values()].map((cell) => cell.count), 1);

  const corner = document.createElement("div");
  corner.className = "public-health-axis public-health-corner";
  corner.textContent = "Likelihood \\ Impact";
  publicHealthMatrix.appendChild(corner);
  impactLevels.forEach((impact) => {
    const axis = document.createElement("div");
    axis.className = "public-health-axis";
    axis.textContent = impactLabel(impact);
    publicHealthMatrix.appendChild(axis);
  });

  likelihoodLevels.forEach((likelihood) => {
    const axis = document.createElement("div");
    axis.className = "public-health-axis";
    axis.textContent = likelihoodLabel(likelihood);
    publicHealthMatrix.appendChild(axis);
    impactLevels.forEach((impact) => {
      const cell = cells.get(`${likelihood}|${impact}`);
      const item = document.createElement("button");
      item.type = "button";
      item.className = `public-health-cell likelihood-${likelihood} impact-${impact}`;
      const count = document.createElement("strong");
      count.textContent = formatNumber(cell?.count || 0);
      const label = document.createElement("span");
      label.textContent = "relações";
      const dominantArea = dominantEntry([...(cell?.areas || new Map()).entries()]);
      const area = document.createElement("small");
      area.textContent = dominantArea[1] ? `${dominantArea[0]} · ${formatNumber(dominantArea[1])}` : "sem relações";
      const fill = document.createElement("i");
      fill.className = `fill-${Math.max(0, Math.min(5, Math.ceil(((cell?.count || 0) / maxCount) * 5)))}`;
      item.append(count, label, area, fill);
      item.addEventListener("click", () => renderPublicHealthStrata(cell, likelihood, impact));
      publicHealthMatrix.appendChild(item);
    });
  });

  const priorityCell = [...cells.values()].sort((a, b) => {
    const rank = {alta: 3, media: 2, baixa: 1};
    const impactRank = {alto: 3, medio: 2, baixo: 1};
    return (impactRank[b.impact] * rank[b.likelihood] * b.count) - (impactRank[a.impact] * rank[a.likelihood] * a.count);
  })[0];
  renderPublicHealthStrata(priorityCell, priorityCell?.likelihood || "alta", priorityCell?.impact || "alto");
}

function renderPublicHealthStrata(cell, likelihood, impact) {
  clearElement(publicHealthStrata);
  const title = document.createElement("div");
  title.className = "public-health-strata-title";
  const titleLabel = document.createElement("strong");
  titleLabel.textContent = `${likelihoodLabel(likelihood)} likelihood × ${impactLabel(impact)} impact`;
  const titleCount = document.createElement("span");
  titleCount.textContent = `${formatNumber(cell?.count || 0)} relações`;
  title.append(titleLabel, titleCount);
  publicHealthStrata.appendChild(title);

  if (!cell || !cell.count) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem relações nesta célula para os filtros atuais.";
    publicHealthStrata.appendChild(empty);
    return;
  }

  const maxArea = Math.max(...[...cell.areas.values()], 1);
  [...cell.areas.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 8).forEach(([area, count]) => {
    const row = document.createElement("div");
    row.className = "public-health-stratum";
    const label = document.createElement("span");
    label.textContent = area;
    const bar = document.createElement("i");
    bar.style.width = `${Math.max(8, (count / maxArea) * 100)}%`;
    const value = document.createElement("strong");
    value.textContent = formatNumber(count);
    row.append(label, bar, value);
    publicHealthStrata.appendChild(row);
  });

  const drivers = document.createElement("p");
  drivers.className = "meta";
  const topDrivers = [...cell.drivers.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3).map(([driver]) => driver);
  drivers.textContent = topDrivers.length ? `Drivers: ${topDrivers.join(", ")}.` : "Drivers: sinal exploratório.";
  publicHealthStrata.appendChild(drivers);

  cell.examples.forEach((example) => {
    const exampleRow = document.createElement("small");
    exampleRow.className = "public-health-example";
    exampleRow.textContent = example;
    publicHealthStrata.appendChild(exampleRow);
  });
}

function renderBubbleChart() {
  clearElement(bubbleChart);
  const dimensions = filteredDimensions().slice(0, 42);
  const width = Math.max(760, bubbleChart.closest(".analytics-chart-wrap")?.clientWidth || 760);
  const height = 380;
  bubbleChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  chartMeta.textContent = `${formatNumber(dimensions.length)} dimensões visíveis · raio por cobertura, eixo X por datasets, eixo Y por relações.`;

  const axis = svgNode("g", {class: "analytics-axis"});
  const xLine = svgNode("line", {x1: 58, y1: height - 42, x2: width - 28, y2: height - 42});
  const yLine = svgNode("line", {x1: 58, y1: 24, x2: 58, y2: height - 42});
  axis.append(xLine, yLine);
  const xLabel = svgNode("text", {x: width - 30, y: height - 16, "text-anchor": "end"});
  xLabel.textContent = "cobertura dataset";
  const yLabel = svgNode("text", {x: 20, y: 34});
  yLabel.textContent = "ligações";
  axis.append(xLabel, yLabel);
  bubbleChart.appendChild(axis);

  if (!dimensions.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem dimensões para estes filtros.";
    bubbleChart.appendChild(empty);
    return;
  }

  const maxDatasets = Math.max(...dimensions.map((item) => item.dataset_count), 1);
  const maxLinks = Math.max(...dimensions.map((item) => item.link_count), 1);
  const plotW = width - 100;
  const plotH = height - 82;
  const layer = svgNode("g", {class: "analytics-bubble-layer"});
  dimensions.forEach((item, index) => {
    const x = 58 + (item.dataset_count / maxDatasets) * plotW;
    const y = (height - 42) - (item.link_count / maxLinks) * plotH;
    const r = Math.max(7, Math.min(24, Math.sqrt(item.coverage_score || 1) * 2.3));
    const group = svgNode("g", {class: `analytics-bubble analytics-kind-${item.kind}`, transform: `translate(${x}, ${y})`});
    if (state.search && item.field.toLowerCase() === state.search.trim().toLowerCase()) {
      group.classList.add("is-selected");
    }
    group.appendChild(svgNode("circle", {r}));
    const label = svgNode("text", {x: r + 5, y: index % 2 ? -3 : 12});
    label.textContent = compactTitle(item.field, 18);
    group.appendChild(label);
    group.addEventListener("click", () => {
      analyticsSearch.value = item.field;
      state.search = item.field;
      renderAll();
    });
    layer.appendChild(group);
  });
  bubbleChart.appendChild(layer);
}

function renderDimensionList() {
  clearElement(dimensionList);
  const dimensions = filteredDimensions().slice(0, 28);
  if (!dimensions.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem dimensões para estes filtros.";
    dimensionList.appendChild(empty);
    return;
  }
  dimensions.forEach((item) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `analytics-dimension-row analytics-kind-${item.kind}`;
    const head = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = item.field;
    const chip = document.createElement("span");
    chip.textContent = kindLabel(item.kind);
    head.append(title, chip);
    const meta = document.createElement("small");
    meta.textContent = `${formatNumber(item.dataset_count)} datasets · ${formatNumber(item.link_count)} ligações · ${formatNumber(item.theme_count)} áreas`;
    const bar = document.createElement("div");
    bar.className = "analytics-progress";
    const fill = document.createElement("span");
    fill.style.width = `${Math.min(100, item.coverage_score)}%`;
    bar.appendChild(fill);
    row.append(head, meta, bar);
    row.addEventListener("click", () => {
      analyticsSearch.value = item.field;
      state.search = item.field;
      renderAll();
    });
    dimensionList.appendChild(row);
  });
}

function renderMatrix(container, rows, formatter) {
  clearElement(container);
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem dados para estes filtros.";
    container.appendChild(empty);
    return;
  }
  const maxScore = Math.max(...rows.map((row) => row.score_sum || row.count || row.link_count || 1), 1);
  rows.slice(0, 24).forEach((row) => {
    const item = document.createElement("div");
    item.className = "analytics-matrix-row";
    const strength = Math.max(row.score_sum || row.count || row.link_count || 1, 1);
    item.style.setProperty("--matrix-alpha", String(Math.max(0.12, Math.min(0.92, strength / maxScore))));
    const left = document.createElement("span");
    const right = document.createElement("strong");
    const detail = formatter(row);
    left.textContent = detail.label;
    right.textContent = detail.value;
    item.append(left, right);
    container.appendChild(item);
  });
}

function renderMatrices() {
  const payload = state.payload || {};
  renderMatrix(themeMatrix, payload.theme_matrix || [], (row) => ({
    label: row.source_theme === row.target_theme ? row.source_theme : `${row.source_theme} / ${row.target_theme}`,
    value: `${formatNumber(row.link_count)} · score ${formatNumber(row.score_sum)}`,
  }));
  renderMatrix(dimensionMatrix, payload.dimension_matrix || [], (row) => ({
    label: row.source === row.target ? kindLabel(row.source) : `${kindLabel(row.source)} / ${kindLabel(row.target)}`,
    value: `${formatNumber(row.count)} relações`,
  }));
}

function renderCorrelationTable() {
  const allRows = filteredCorrelations();
  const rows = allRows.slice(0, 80);
  state.filteredCorrelations = allRows;
  correlationMeta.textContent = `${formatNumber(rows.length)}/${formatNumber(allRows.length)} relações na tabela · exporta o filtro completo.`;
  const thead = correlationTable.querySelector("thead");
  const tbody = correlationTable.querySelector("tbody");
  clearElement(thead);
  clearElement(tbody);

  const header = document.createElement("tr");
  ["Relação", "Confiança", "Saúde pública", "Dimensões", "Score", "Chaves sugeridas", "Risco"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    header.appendChild(th);
  });
  thead.appendChild(header);

  rows.forEach((item) => {
    const row = document.createElement("tr");
    if (state.selectedCorrelationKey === relationKey(item)) {
      row.className = "is-selected";
    }
    const relation = document.createElement("td");
    const title = document.createElement("strong");
    title.textContent = `${compactTitle(item.source_title, 42)} / ${compactTitle(item.target_title, 42)}`;
    const sub = document.createElement("small");
    sub.textContent = `${item.source_theme} ↔ ${item.target_theme}`;
    relation.append(title, sub);

    const confidence = document.createElement("td");
    const confidenceChip = document.createElement("span");
    confidenceChip.className = `analytics-chip confidence-${item.confidence}`;
    confidenceChip.textContent = confidenceLabel(item.confidence);
    confidence.appendChild(confidenceChip);

    const publicHealth = document.createElement("td");
    const publicHealthModel = item.public_health_model || {};
    const publicHealthChip = document.createElement("span");
    publicHealthChip.className = `analytics-chip public-health-chip likelihood-${publicHealthModel.likelihood?.level || "baixa"} impact-${publicHealthModel.impact?.level || "baixo"}`;
    publicHealthChip.textContent = `${likelihoodLabel(publicHealthModel.likelihood?.level || "baixa")} × ${impactLabel(publicHealthModel.impact?.level || "baixo")}`;
    const publicHealthSub = document.createElement("small");
    publicHealthSub.textContent = (publicHealthModel.matched_areas || ["Area nao especificada"]).slice(0, 2).join(", ");
    publicHealth.append(publicHealthChip, publicHealthSub);

    const kinds = document.createElement("td");
    const kindWrap = document.createElement("div");
    kindWrap.className = "analytics-chip-row";
    (item.dimension_kinds || []).forEach((kind) => {
      const chip = document.createElement("span");
      chip.className = `analytics-chip analytics-kind-${kind}`;
      chip.textContent = kindLabel(kind);
      kindWrap.appendChild(chip);
    });
    kinds.appendChild(kindWrap);

    const score = document.createElement("td");
    score.textContent = `${item.semantic_score} / ${item.score}`;

    const keys = document.createElement("td");
    keys.textContent = (item.join_recipe?.suggested_keys || []).slice(0, 5).join(", ") || "-";

    const risks = document.createElement("td");
    risks.textContent = (item.risk_flags || []).join(", ") || "baixo";

    row.append(relation, confidence, publicHealth, kinds, score, keys, risks);
    row.addEventListener("click", () => {
      state.selectedCorrelationKey = relationKey(item);
      renderCorrelationTable();
    });
    tbody.appendChild(row);
  });
}

function renderAll() {
  if (!state.payload) return;
  renderSummary();
  renderInsightCards();
  renderDistribution();
  renderSemanticModel();
  renderPublicHealthMatrix();
  renderBubbleChart();
  renderDimensionList();
  renderMatrices();
  renderCorrelationTable();
  renderDataAnalytics();
}

function setupEvents() {
  analyticsMinScore.value = state.minScore;
  analyticsMinScoreValue.textContent = state.minScore;
  dataSampleLimit.value = state.dataLimit;
  dataSampleLimitValue.textContent = state.dataLimit;

  analyticsSearch.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = analyticsSearch.value;
      renderAll();
    }, 120);
  });

  analyticsKind.addEventListener("change", () => {
    state.kind = analyticsKind.value;
    renderAll();
  });

  analyticsConfidence.addEventListener("change", () => {
    state.confidence = analyticsConfidence.value;
    renderAll();
  });

  analyticsMinScore.addEventListener("input", () => {
    state.minScore = Number(analyticsMinScore.value);
    analyticsMinScoreValue.textContent = state.minScore;
    debounceLoadAnalytics();
  });

  analyticsRefresh.addEventListener("click", () => {
    loadAnalytics().catch(showError);
  });

  dataDatasetSelect.addEventListener("change", () => {
    state.selectedDataDataset = dataDatasetSelect.value;
    state.dataPayload = null;
    renderDataAnalytics();
    loadDataAnalytics().catch(showDataError);
  });

  dataSampleLimit.addEventListener("input", () => {
    state.dataLimit = Number(dataSampleLimit.value);
    dataSampleLimitValue.textContent = state.dataLimit;
    debounceLoadDataAnalytics();
  });

  dataRefreshButton.addEventListener("click", () => {
    loadDataAnalytics().catch(showDataError);
  });

  analyticsExport.addEventListener("click", () => {
    const payload = {
      generated_at: state.payload?.generated_at,
      filters: {
        min_score: state.minScore,
        search: state.search,
        dimension: state.kind,
        confidence: state.confidence,
        low_risk_only: state.lowRiskOnly,
      },
      correlations: state.filteredCorrelations,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], {type: "application/json"});
    const anchor = document.createElement("a");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = `analytics-correlacoes-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
    anchor.click();
    URL.revokeObjectURL(anchor.href);
  });
}

function showError(error) {
  analyticsStatus.textContent = error.message || "Erro";
}

function showDataError(error) {
  dataAnalyticsStatus.textContent = error.message || "Erro ao analisar dados";
}

setupEvents();
loadAnalytics().catch(showError);
