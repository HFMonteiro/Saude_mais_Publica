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
  selectedFeatureKey: "",
  selectedPublicHealthKey: "",
  selectedPublicHealthMatrixCell: "",
  selectedLocalPriorityCell: "",
  selectedPredictiveIndicator: "",
  activePublicHealthLayer: "impact",
  publicHealthRenderLimit: 40,
  publicHealthSort: "priority",
  activeTab: "data",
  datasetMode: "rich",
  dataLimit: 80,
  dataPayload: null,
  selectedFinancialDataset: "",
  selectedProductionDataset: "",
  finProdPayload: null,
  _predictiveLoadDataset: "",
  _filterCache: {
    key: "",
    correlations: [],
    dimensions: [],
    publicHealthKey: "",
    publicHealthRows: null,
  },
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
const dataRichMode = document.getElementById("dataRichMode");
const dataManualMode = document.getElementById("dataManualMode");
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
const featureDetailPanel = document.getElementById("featureDetailPanel");
const dataPcaMeta = document.getElementById("dataPcaMeta");
const dataPcaChart = document.getElementById("dataPcaChart");
const finProdMeta = document.getElementById("finProdMeta");
const finProdStatus = document.getElementById("finProdStatus");
const finProdFinancialDataset = document.getElementById("finProdFinancialDataset");
const finProdProductionDataset = document.getElementById("finProdProductionDataset");
const finProdRefreshButton = document.getElementById("finProdRefreshButton");
const finProdKpis = document.getElementById("finProdKpis");
const finProdTable = document.getElementById("finProdTable");
const finProdChecks = document.getElementById("finProdChecks");
const finProdTrendChart = document.getElementById("finProdTrendChart");
const finProdOutliers = document.getElementById("finProdOutliers");
const finProdBenchmark = document.getElementById("finProdBenchmark");
const finProdDiagnostics = document.getElementById("finProdDiagnostics");
const predictiveMeta = document.getElementById("predictiveMeta");
const predictiveDataButton = document.getElementById("predictiveDataButton");
const predictiveKpis = document.getElementById("predictiveKpis");
const predictiveIndicatorFilter = document.getElementById("predictiveIndicatorFilter");
const predictiveForecastChart = document.getElementById("predictiveForecastChart");
const predictiveDrivers = document.getElementById("predictiveDrivers");
const predictiveScenarios = document.getElementById("predictiveScenarios");
const predictiveRisk = document.getElementById("predictiveRisk");
const predictiveDatasetCandidates = document.getElementById("predictiveDatasetCandidates");
const analyticsTabs = Array.from(document.querySelectorAll("[data-analytics-tab]"));
const analyticsPanels = Array.from(document.querySelectorAll("[data-tab-panel]"));
const topOpportunities = document.getElementById("analyticsTopOpportunities");
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
const publicHealthKpis = document.getElementById("publicHealthKpis");
const publicHealthPriorityList = document.getElementById("publicHealthPriorityList");
const publicHealthLayerControls = document.getElementById("publicHealthLayerControls");
const publicHealthNationalSummary = document.getElementById("publicHealthNationalSummary");
const publicHealthMap = document.getElementById("publicHealthMap");
const publicHealthDecisionDetail = document.getElementById("publicHealthDecisionDetail");
const publicHealthQuestions = document.getElementById("publicHealthQuestions");
const publicHealthTableMeta = document.getElementById("publicHealthTableMeta");
const publicHealthHypothesisMatrix = document.getElementById("publicHealthHypothesisMatrix");
const publicHealthHypothesisDetail = document.getElementById("publicHealthHypothesisDetail");
const publicHealthMatrix = document.getElementById("publicHealthMatrix");
const publicHealthStrata = document.getElementById("publicHealthStrata");
const localPlanMeta = document.getElementById("localPlanMeta");
const localPlanKpis = document.getElementById("localPlanKpis");
const localDiagnosis = document.getElementById("localDiagnosis");
const localPriorityMatrix = document.getElementById("localPriorityMatrix");
const localPriorityDetail = document.getElementById("localPriorityDetail");
const localSurveillance = document.getElementById("localSurveillance");
const localRiskMatrix = document.getElementById("localRiskMatrix");
const localActionPlan = document.getElementById("localActionPlan");
const bubbleChart = document.getElementById("analyticsBubbleChart");
const dimensionList = document.getElementById("analyticsDimensionList");
const themeMatrix = document.getElementById("analyticsThemeMatrix");
const dimensionMatrix = document.getElementById("analyticsDimensionMatrix");
const correlationMeta = document.getElementById("analyticsCorrelationMeta");
const correlationTable = document.getElementById("analyticsCorrelationTable");
const SVG_NS = "http://www.w3.org/2000/svg";
const PUBLIC_HEALTH_LAYERS = [
  ["impact", "Impacto"],
  ["likelihood", "Likelihood"],
  ["risk", "Risco"],
  ["entities", "Entidades"],
  ["opportunities", "Oportunidades"],
];
const PUBLIC_HEALTH_GEO = [
  {
    id: "norte",
    label: "Norte",
    x: 216,
    y: 64,
    w: 136,
    h: 82,
    labelX: 284,
    labelY: 108,
    match: /(norte|porto|braga|viana|vila real|braganca|bragança|sao joao|são joão|santo antonio|santo antónio|tamega|tâmega|sousa|gaia|espinho|matosinhos|alto minho|medio ave|médio ave|tras-os-montes|trás-os-montes|nordeste)/,
  },
  {
    id: "centro",
    label: "Centro",
    x: 216,
    y: 156,
    w: 136,
    h: 82,
    labelX: 284,
    labelY: 200,
    match: /(centro|coimbra|leiria|viseu|guarda|castelo branco|aveiro|cova da beira|baixo mondego|dao lafoes|dão lafões|beira interior|baixo vouga)/,
  },
  {
    id: "lisboa",
    label: "Lisboa e V. Tejo",
    x: 216,
    y: 248,
    w: 136,
    h: 82,
    labelX: 284,
    labelY: 292,
    match: /(lisboa|tejo|setubal|setúbal|santarem|santarém|oeste|aml|amadora|sintra|loures|odivelas|almada|seixal|arrabida|arrábida|arco ribeirinho|leziria|lezíria|medio tejo|médio tejo|estuário)/,
  },
  {
    id: "alentejo",
    label: "Alentejo",
    x: 216,
    y: 340,
    w: 136,
    h: 82,
    labelX: 284,
    labelY: 384,
    match: /(alentejo|evora|évora|beja|portalegre|ribatejo)/,
  },
  {
    id: "algarve",
    label: "Algarve",
    x: 216,
    y: 432,
    w: 136,
    h: 64,
    labelX: 284,
    labelY: 468,
    match: /(algarve|faro|portimao|portimão)/,
  },
  {
    id: "madeira",
    label: "Madeira",
    x: 62,
    y: 398,
    w: 108,
    h: 56,
    labelX: 116,
    labelY: 430,
    match: /(madeira|funchal)/,
  },
  {
    id: "acores",
    label: "Açores",
    x: 62,
    y: 86,
    w: 108,
    h: 56,
    labelX: 116,
    labelY: 118,
    match: /(acores|açores|ponta delgada|angra|horta)/,
  },
];
const PUBLIC_HEALTH_NATIONAL_GEO = {
  id: "nacional",
  label: "Vista nacional",
  x: 346,
  y: 76,
  w: 132,
  h: 84,
  labelX: 412,
  labelY: 111,
  match: /nacional/,
};
const PUBLIC_HEALTH_AGGREGATION_GEO = {
  id: "agregar",
  label: "Entidade por validar",
  x: 346,
  y: 174,
  w: 132,
  h: 84,
  labelX: 412,
  labelY: 209,
  match: /$/,
};
const ULS_REGION_RULES = [
  ["norte", "Alto Minho", /alto minho|viana do castelo/],
  ["norte", "Braga / Médio Ave", /braga|medio ave|médio ave|barcelos|famalicao|famalicão/],
  ["norte", "São João", /sao joao|são joão|maia|valongo/],
  ["norte", "Santo António", /santo antonio|santo antónio|porto/],
  ["norte", "Gaia/Espinho", /gaia|espinho/],
  ["norte", "Matosinhos", /matosinhos/],
  ["norte", "Póvoa/Vila do Conde", /povoa|póvoa|vila do conde/],
  ["norte", "Tâmega e Sousa", /tamega|tâmega|sousa|penafiel|paredes/],
  ["norte", "Trás-os-Montes e Alto Douro", /tras os montes|trás os montes|alto douro|vila real|chaves|braganca|bragança|nordeste/],
  ["centro", "Guarda", /guarda/],
  ["centro", "Castelo Branco", /castelo branco/],
  ["centro", "Cova da Beira", /cova da beira|covilha|covilhã/],
  ["centro", "Coimbra / Baixo Mondego", /coimbra|baixo mondego|figueira da foz/],
  ["centro", "Dão Lafões", /dao lafoes|dão lafões|viseu|tondela/],
  ["centro", "Leiria", /leiria|pombal|alcobaca|alcobaça/],
  ["lisboa", "Amadora-Sintra", /amadora|sintra|fernando fonseca/],
  ["lisboa", "Lisboa Norte", /lisboa norte|santa maria|pulido valente/],
  ["lisboa", "São José", /sao jose|são josé|lisboa central|sacavem|sacavém|gama pinto/],
  ["lisboa", "Lisboa Ocidental", /lisboa ocidental|oeiras|cascais|egas moniz|sao francisco xavier|são francisco xavier/],
  ["lisboa", "Loures-Odivelas", /loures|odivelas|beatriz angelo|beatriz ângelo/],
  ["lisboa", "Oeste", /oeste|caldas da rainha|peniche|torres vedras/],
  ["lisboa", "Médio Tejo", /medio tejo|médio tejo|abrantes|tomar|torres novas/],
  ["lisboa", "Lezíria", /leziria|lezíria|santarem|santarém/],
  ["lisboa", "Estuário do Tejo", /estuario|estuário|vila franca de xira/],
  ["lisboa", "Arrábida", /arrabida|arrábida|setubal|setúbal/],
  ["lisboa", "Almada-Seixal", /almada|seixal/],
  ["lisboa", "Arco Ribeirinho", /arco ribeirinho|barreiro|montijo/],
  ["alentejo", "Alentejo Central", /alentejo central|evora|évora/],
  ["alentejo", "Alto Alentejo", /alto alentejo|portalegre|norte alentejano/],
  ["alentejo", "Baixo Alentejo", /baixo alentejo|beja/],
  ["alentejo", "Litoral Alentejano", /litoral alentejano|santiago do cacem|santiago do cacém/],
  ["algarve", "Algarve", /algarve|faro|portimao|portimão|lagos|tavira/],
];
const PNS_TERMS = [
  "desigualdade",
  "vulneravel",
  "vulnerabilidade",
  "mortalidade",
  "morbilidade",
  "doenca",
  "diabetes",
  "avc",
  "oncologico",
  "rastreio",
  "emergencia",
  "urgencia",
  "prevencao",
  "vacina",
  "acesso",
  "espera",
  "recursos",
  "territorio",
  "saude publica",
];
const LOCAL_PNS_AXES = [
  ["Equidade", /(desigualdade|equidade|vulneravel|vulnerabilidade|territorio|territorial|acesso)/],
  ["Acesso e qualidade", /(acesso|espera|consulta|urgencia|referenciacao|qualidade|satisfacao|seguranca)/],
  ["Carga da doença", /(mortalidade|morbilidade|doenca|diabetes|avc|oncologico|saude mental|hipertensao)/],
  ["Emergência e vigilância", /(emergencia|urgencia|vigilancia|epidem|risco|sazonal|resposta|alerta)/],
  ["Recursos e integração", /(recursos|profissionais|contrato|despesa|financeiro|integracao|hospital|uls|csp)/],
];

let searchTimer = null;
let loadTimer = null;
let dataLoadTimer = null;
let activeRequest = 0;
let activeDataRequest = 0;

function clearElement(element) {
  element.replaceChildren();
}

function repairEncoding(value) {
  return String(value)
    .replace(/Ã¡/g, "á").replace(/Ã /g, "à").replace(/Ã¢/g, "â").replace(/Ã£/g, "ã")
    .replace(/Ã©/g, "é").replace(/Ãª/g, "ê").replace(/Ã­/g, "í")
    .replace(/Ã³/g, "ó").replace(/Ã´/g, "ô").replace(/Ãµ/g, "õ")
    .replace(/Ãº/g, "ú").replace(/Ã§/g, "ç")
    .replace(/Ã/g, "Á").replace(/Ã‰/g, "É").replace(/Ã/g, "Í")
    .replace(/Ã“/g, "Ó").replace(/Ãš/g, "Ú").replace(/Ã‡/g, "Ç")
    .replace(/Ã—/g, "×").replace(/Â·/g, "·").replace(/Â /g, " ")
    .replace(/â€¦/g, "...").replace(/â€”/g, "-").replace(/â€“/g, "-")
    .replace(/â†’/g, "→").replace(/â†/g, "←");
}

function safeText(value) {
  return value == null ? "" : repairEncoding(value);
}

async function fetchJson(url, {timeoutMs = 22000} = {}) {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {signal: controller.signal});
    let payload = {};
    try {
      payload = await response.json();
    } catch (error) {
      payload = {};
    }
    if (!response.ok) {
      throw new Error(payload.error || `Pedido falhou (${response.status})`);
    }
    return payload;
  } catch (error) {
    if (error.name === "AbortError") {
      throw new Error("A API demorou demasiado tempo. Tenta atualizar novamente.");
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

function formatNumber(value) {
  return new Intl.NumberFormat("pt-PT").format(value || 0);
}

function formatDisplayValue(value, digits = 2) {
  const text = safeText(value).trim();
  if (!text) return "-";
  const normalized = text.replace(/\s+/g, "").replace(",", ".");
  if (/^-?\d+(?:\.\d+)?$/.test(normalized)) {
    return formatDecimal(Number(normalized), digits);
  }
  return text;
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

function invalidateFilterCache() {
  state._filterCache = {key: "", correlations: [], dimensions: [], publicHealthKey: "", publicHealthRows: null};
}

function activeFilterKey() {
  return [
    state.payload?.generated_at || "",
    state.search.trim().toLowerCase(),
    state.kind,
    state.confidence,
    state.lowRiskOnly ? "low" : "all",
  ].join("|");
}

function dominantEntry(entries) {
  return entries.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0] || ["-", 0];
}

function svgNode(tag, attributes = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  return node;
}

function renderDatasetMode() {
  dataRichMode?.classList.toggle("is-active", state.datasetMode === "rich");
  dataManualMode?.classList.toggle("is-active", state.datasetMode === "manual");
}

function setActiveTab(tab, {syncUrl = true, force = false} = {}) {
  const nextTab = tab || "data";
  if (!force && state.activeTab === nextTab) {
    return;
  }
  state.activeTab = nextTab;
  analyticsTabs.forEach((button) => {
    const isActive = button.dataset.analyticsTab === state.activeTab;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-selected", isActive ? "true" : "false");
    button.setAttribute("role", "tab");
    button.tabIndex = isActive ? 0 : -1;
  });
  analyticsPanels.forEach((panel) => {
    const panels = (panel.dataset.tabPanel || "").split(/\s+/);
    panel.hidden = !panels.includes(state.activeTab);
    panel.setAttribute("role", "tabpanel");
  });
  if (syncUrl && window.history?.replaceState) {
    const params = new URLSearchParams(window.location.search);
    if (state.activeTab === "data") {
      params.delete("tab");
    } else {
      params.set("tab", state.activeTab);
    }
    const query = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
  }
  renderAll();
}

async function loadAnalytics() {
  const requestId = ++activeRequest;
  analyticsStatus.textContent = "A carregar...";
  const payload = await fetchJson(`/api/analytics?min_score=${state.minScore}`, {timeoutMs: 26000});
  if (requestId !== activeRequest) {
    return;
  }
  state.payload = payload;
  invalidateFilterCache();
  populateDataDatasetOptions();
  populateFinProdOptions();
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
  if (state.datasetMode === "rich") {
    const richest = datasets.find((dataset) => (dataset.records_count || 0) > 0 && (dataset.metric_candidate_count || 0) >= 2)
      || datasets.find((dataset) => (dataset.records_count || 0) > 0)
      || datasets[0];
    state.selectedDataDataset = richest?.dataset_id || "";
    dataDatasetSelect.value = state.selectedDataDataset;
  } else if (current && datasets.some((dataset) => dataset.dataset_id === current)) {
    dataDatasetSelect.value = current;
  } else {
    const firstWithRecords = datasets.find((dataset) => (dataset.records_count || 0) > 0) || datasets[0];
    state.selectedDataDataset = firstWithRecords?.dataset_id || "";
    dataDatasetSelect.value = state.selectedDataDataset;
  }
  renderDatasetMode();
}

function populateFinProdOptions() {
  const datasets = state.payload?.datasets || [];
  const financial = datasets
    .filter((dataset) => dataset.mega_theme === "Finanças & Compras")
    .sort((a, b) => (b.metric_candidate_count || 0) - (a.metric_candidate_count || 0));
  const production = datasets
    .filter((dataset) => dataset.mega_theme === "Acesso & Produção")
    .sort((a, b) => (b.metric_candidate_count || 0) - (a.metric_candidate_count || 0));
  clearElement(finProdFinancialDataset);
  clearElement(finProdProductionDataset);
  financial.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = compactTitle(dataset.title || dataset.dataset_id, 72);
    finProdFinancialDataset.appendChild(option);
  });
  production.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = compactTitle(dataset.title || dataset.dataset_id, 72);
    finProdProductionDataset.appendChild(option);
  });
  if (!financial.some((dataset) => dataset.dataset_id === state.selectedFinancialDataset)) {
    state.selectedFinancialDataset = financial[0]?.dataset_id || "";
  }
  if (!production.some((dataset) => dataset.dataset_id === state.selectedProductionDataset)) {
    state.selectedProductionDataset = production[0]?.dataset_id || "";
  }
  finProdFinancialDataset.value = state.selectedFinancialDataset;
  finProdProductionDataset.value = state.selectedProductionDataset;
}

async function loadDataAnalytics() {
  if (!state.selectedDataDataset) return;
  const requestId = ++activeDataRequest;
  dataAnalyticsStatus.textContent = "A analisar amostra...";
  const payload = await fetchJson(`/api/data-analytics?dataset_id=${encodeURIComponent(state.selectedDataDataset)}&limit=${state.dataLimit}`, {
    timeoutMs: 26000,
  });
  if (requestId !== activeDataRequest) return;
  state.dataPayload = payload;
  dataAnalyticsStatus.textContent = `Amostra atualizada · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  if (state.activeTab === "predictive") {
    renderPredictiveAnalytics();
  } else {
    renderDataAnalytics();
  }
}

function debounceLoadDataAnalytics() {
  clearTimeout(dataLoadTimer);
  dataLoadTimer = setTimeout(() => {
    loadDataAnalytics().catch(showDataError);
  }, 180);
}

async function loadFinProdAnalytics() {
  if (!state.selectedFinancialDataset || !state.selectedProductionDataset) {
    throw new Error("Seleciona dataset financeiro e de produção.");
  }
  finProdStatus.textContent = "A cruzar datasets...";
  const payload = await fetchJson(
    `/api/finprod?financial_dataset=${encodeURIComponent(state.selectedFinancialDataset)}&production_dataset=${encodeURIComponent(state.selectedProductionDataset)}&limit=${state.dataLimit}`,
    {timeoutMs: 32000},
  );
  state.finProdPayload = payload;
  finProdStatus.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  renderFinProdAnalytics();
}

function addFinProdKpi(label, value, detail) {
  const card = document.createElement("div");
  const span = document.createElement("span");
  span.textContent = label;
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = detail;
  card.append(span, strong, small);
  finProdKpis.appendChild(card);
}

function renderFinProdAnalytics() {
  clearElement(finProdKpis);
  clearElement(finProdChecks);
  clearElement(finProdOutliers);
  clearElement(finProdBenchmark);
  clearElement(finProdTrendChart);
  const thead = finProdTable?.querySelector("thead");
  const tbody = finProdTable?.querySelector("tbody");
  if (thead) clearElement(thead);
  if (tbody) clearElement(tbody);

  const payload = state.finProdPayload;
  if (!payload) {
    finProdMeta.textContent = "Seleciona os dois datasets e clica em Cruzar datasets.";
    finProdDiagnostics.textContent = "Sem cruzamento ativo.";
    return;
  }
  renderFinProdChecks(payload.comparability?.checks || []);
  finProdMeta.textContent = `${payload.financial_dataset?.title || payload.financial_dataset?.dataset_id} × ${payload.production_dataset?.title || payload.production_dataset?.dataset_id}`;
  const summary = payload.summary || {};
  addFinProdKpi("Períodos em comum", formatNumber(summary.matched_periods || 0), "base temporal comparável");
  addFinProdKpi("Custo unitário médio", formatDecimal(summary.avg_unit_cost, 2), "despesa / produção");
  addFinProdKpi("Mediana custo unitário", formatDecimal(summary.median_unit_cost, 2), "robusto a outliers");
  addFinProdKpi(
    "Correlação despesa-produção",
    summary.expense_output_correlation == null ? "-" : formatDecimal(summary.expense_output_correlation, 2),
    summary.correlation_strength || "insuficiente",
  );
  addFinProdKpi("Spearman", summary.spearman_correlation == null ? "-" : formatDecimal(summary.spearman_correlation, 2), `robustez ${summary.robustness || "insuficiente"}`);

  const header = document.createElement("tr");
  ["Período", "Financeiro (norm.)", "Produção (norm.)", "Custo unitário"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    header.appendChild(th);
  });
  thead?.appendChild(header);
  (payload.rows || []).forEach((row) => {
    const tr = document.createElement("tr");
    [row.period, formatDecimal(row.financial_normalized, 2), formatDecimal(row.production_normalized, 2), formatDecimal(row.unit_cost, 2)].forEach((value) => {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    });
    tbody?.appendChild(tr);
  });
  renderFinProdTrend(payload);
  renderFinProdOutliers(payload.outliers || []);
  renderFinProdBenchmark(payload.entity_benchmark || []);
  const norm = payload.normalization || {};
  const warnings = payload.diagnostics?.warnings || [];
  finProdDiagnostics.textContent = `Escalas: financeiro em ${norm.financial?.unit_label || "-"} · produção em ${norm.production?.unit_label || "-"}. ${warnings.length ? `Avisos: ${warnings.join(" · ")}` : "Sem avisos críticos."}`;
}

function renderFinProdChecks(checks) {
  if (!checks.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Checklist indisponível.";
    finProdChecks.appendChild(empty);
    return;
  }
  checks.forEach((check) => {
    const row = document.createElement("div");
    row.className = `finprod-check is-${check.status || "warning"}`;
    const title = document.createElement("strong");
    title.textContent = check.label || "Validação";
    const detail = document.createElement("small");
    detail.textContent = check.detail || "-";
    row.append(title, detail);
    finProdChecks.appendChild(row);
  });
}

function renderFinProdTrend(payload) {
  const rows = payload.rows || [];
  const width = Math.max(560, finProdTrendChart.closest(".data-trend-wrap")?.clientWidth || 560);
  const height = 250;
  finProdTrendChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  if (rows.length < 2) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem períodos suficientes para tendência.";
    finProdTrendChart.appendChild(empty);
    return;
  }
  const values = rows.map((row) => Number(row.unit_cost)).filter((value) => Number.isFinite(value));
  if (values.length < 2) return;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const left = 42;
  const right = width - 22;
  const top = 20;
  const bottom = height - 34;
  const step = (right - left) / Math.max(rows.length - 1, 1);
  const coords = rows.map((row, idx) => ({
    x: left + (idx * step),
    y: bottom - (((Number(row.unit_cost) - min) / span) * (bottom - top)),
    period: row.period,
    unitCost: row.unit_cost,
  }));
  finProdTrendChart.appendChild(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom, class: "data-trend-axis"}));
  finProdTrendChart.appendChild(svgNode("line", {x1: left, y1: top, x2: left, y2: bottom, class: "data-trend-axis"}));
  finProdTrendChart.appendChild(svgNode("path", {
    class: "data-trend-line",
    d: coords.map((coord, idx) => `${idx ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  }));
  coords.forEach((coord) => {
    const group = svgNode("g", {class: "data-trend-point", transform: `translate(${coord.x}, ${coord.y})`});
    group.appendChild(svgNode("circle", {r: 3.8}));
    const title = svgNode("title");
    title.textContent = `${coord.period}: ${formatDecimal(coord.unitCost, 2)}`;
    group.appendChild(title);
    finProdTrendChart.appendChild(group);
  });
}

function renderFinProdOutliers(rows) {
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem outliers relevantes de custo unitário.";
    finProdOutliers.appendChild(empty);
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "analytics-dimension-row";
    const strong = document.createElement("strong");
    strong.textContent = row.period;
    const meta = document.createElement("small");
    meta.textContent = `Custo ${formatDecimal(row.unit_cost, 2)} · robust-z ${formatDecimal(row.robust_z, 2)}`;
    item.append(strong, meta);
    finProdOutliers.appendChild(item);
  });
}

function renderFinProdBenchmark(rows) {
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem benchmark entidade/região comparável.";
    finProdBenchmark.appendChild(empty);
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "analytics-dimension-row";
    const strong = document.createElement("strong");
    strong.textContent = compactTitle(row.entity, 42);
    const meta = document.createElement("small");
    meta.textContent = `Financeiro ${formatDecimal((row.financial_share || 0) * 100, 1)}% · Produção ${formatDecimal((row.production_share || 0) * 100, 1)}% · gap ${formatDecimal((row.balance_gap || 0) * 100, 1)}pp`;
    item.append(strong, meta);
    finProdBenchmark.appendChild(item);
  });
}

function filteredCorrelations() {
  const payload = state.payload;
  if (!payload) return [];
  const key = activeFilterKey();
  if (state._filterCache.key === key && state._filterCache.correlations) {
    return state._filterCache.correlations;
  }
  const search = state.search.trim().toLowerCase();
  const correlations = (payload.correlations || []).filter((item) => {
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
  state._filterCache.key = key;
  state._filterCache.correlations = correlations;
  state._filterCache.dimensions = null;
  return correlations;
}

function filteredDimensions() {
  const payload = state.payload;
  if (!payload) return [];
  const key = activeFilterKey();
  if (state._filterCache.key === key && state._filterCache.dimensions) {
    return state._filterCache.dimensions;
  }
  const search = state.search.trim().toLowerCase();
  const dimensions = (payload.dimensions || []).filter((item) => {
    if (state.kind && item.kind !== state.kind) return false;
    if (!search) return true;
    return `${item.field} ${item.kind}`.toLowerCase().includes(search);
  });
  if (state._filterCache.key !== key) {
    state._filterCache.key = key;
    state._filterCache.correlations = null;
  }
  state._filterCache.dimensions = dimensions;
  return dimensions;
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

function opportunityPriority(item) {
  const likelihoodRank = {alta: 3, media: 2, baixa: 1};
  const impactRank = {alto: 3, medio: 2, baixo: 1};
  const model = item.public_health_model || {};
  const riskPenalty = (item.risk_flags || []).length * 1.8;
  return Number(item.semantic_score || 0)
    + likelihoodRank[model.likelihood?.level || "baixa"] * 2
    + impactRank[model.impact?.level || "baixo"] * 2
    - riskPenalty;
}

function normalizeSearchText(value) {
  return safeText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function resolveUlsRegion(text) {
  const normalized = normalizeSearchText(text);
  for (const [regionId, label, pattern] of ULS_REGION_RULES) {
    if (pattern.test(normalized)) {
      const geo = PUBLIC_HEALTH_GEO.find((entry) => entry.id === regionId);
      if (geo) {
        return {
          ...geo,
          resolution: "mapped",
          matchedEntity: label,
          mappingConfidence: "alta",
        };
      }
    }
  }
  return null;
}

function priorityDecision(score, row) {
  if ((row.risk || 0) >= 7 || row.viability < 4) return "Validar antes";
  if (score >= 72) return "Priorizar";
  if (score >= 55) return "Acompanhar";
  return "Explorar";
}

function decisionTone(decision) {
  return {
    "Priorizar": "act",
    "Acompanhar": "watch",
    "Explorar": "investigate",
    "Validar antes": "avoid",
  }[decision] || "investigate";
}

function matchedGeo(item) {
  const text = normalizeSearchText([
    item.source_title,
    item.target_title,
    item.source_theme,
    item.target_theme,
    ...(item.shared_fields || []),
    ...((item.public_health_model || {}).matched_areas || []),
  ].join(" "));
  const mapped = resolveUlsRegion(text);
  if (mapped) return mapped;
  const regional = PUBLIC_HEALTH_GEO.find((geo) => geo.match.test(text));
  if (regional) return {...regional, resolution: "direct", mappingConfidence: "media"};
  if (/\b(uls|unidade local de saude|hospital|centro hospitalar|ch |epe|aces|usf|ucsp)\b/.test(text)) {
    return {...PUBLIC_HEALTH_AGGREGATION_GEO, resolution: "pending", mappingConfidence: "baixa"};
  }
  return {...PUBLIC_HEALTH_NATIONAL_GEO, resolution: "national"};
}

function pnsAlignment(item) {
  const text = normalizeSearchText([
    item.source_title,
    item.target_title,
    item.source_theme,
    item.target_theme,
    ...(item.shared_fields || []),
    ...((item.public_health_model || {}).impact?.drivers || []),
  ].join(" "));
  return PNS_TERMS.reduce((count, term) => count + (text.includes(normalizeSearchText(term)) ? 1 : 0), 0);
}

function publicHealthPriorityRows() {
  const key = activeFilterKey() + "|" + state.publicHealthSort + "|" + (state.dataPayload?.generated_at || "");
  if (state._filterCache.publicHealthKey === key && state._filterCache.publicHealthRows) {
    return state._filterCache.publicHealthRows;
  }

  const rows = filteredCorrelations().map((item) => {
    const model = item.public_health_model || {};
    const likelihoodRank = {alta: 3, media: 2, baixa: 1}[model.likelihood?.level || "baixa"];
    const impactRank = {alto: 3, medio: 2, baixo: 1}[model.impact?.level || "baixo"];
    const dimensionKinds = item.dimension_kinds || [];
    const keyCount = (item.join_recipe?.suggested_keys || item.shared_fields || []).length;
    const risk = Math.min(10, (item.risk_flags || []).length * 3 + (dimensionKinds.includes("medida") ? 2 : 0));
    const magnitude = Math.min(20, Number(item.semantic_score || 0) * 1.4 + Math.min(6, keyCount));
    const severity = Math.min(20, impactRank * 6 + pnsAlignment(item));
    const trend = state.dataPayload?.trends?.length ? 8 : 4;
    const inequity = Math.min(16, (dimensionKinds.includes("territorial") ? 6 : 0) + (dimensionKinds.includes("entidade") ? 4 : 0) + ((model.matched_areas || []).length * 2));
    const viability = Math.max(0, Math.min(18, likelihoodRank * 5 + (item.confidence === "alta" ? 3 : item.confidence === "media" ? 1 : 0) - risk));
    const alignment = Math.min(12, pnsAlignment(item) * 2);
    const priority = Math.round(magnitude + severity + trend + inequity + viability + alignment);
    const geo = matchedGeo(item);
    const row = {
      item,
      key: relationKey(item),
      priority,
      magnitude,
      severity,
      trend,
      inequity,
      viability,
      alignment,
      risk,
      geo,
      decision: "",
      reason: "",
    };
    row.decision = priorityDecision(priority, row);
    row.reason = publicHealthReason(row);
    return row;
  });

  const sorters = {
    priority: (a, b) => b.priority - a.priority || b.viability - a.viability,
    impact: (a, b) => b.severity - a.severity || b.priority - a.priority,
    risk: (a, b) => b.risk - a.risk || b.priority - a.priority,
    viability: (a, b) => b.viability - a.viability || b.priority - a.priority,
  };
  const result = rows.sort(sorters[state.publicHealthSort] || sorters.priority);
  state._filterCache.publicHealthKey = key;
  state._filterCache.publicHealthRows = result;
  return result;
}

function publicHealthReason(row) {
  const model = row.item.public_health_model || {};
  const drivers = model.impact?.drivers || [];
  const parts = [];
  if (row.decision === "Priorizar") parts.push("alto valor potencial e boa viabilidade");
  if (row.decision === "Acompanhar") parts.push("sinal relevante para acompanhamento");
  if (row.decision === "Explorar") parts.push("potencial exploratório ainda incompleto");
  if (row.decision === "Validar antes") parts.push("validar risco, granularidade ou viabilidade");
  if (drivers.length) parts.push(`drivers: ${drivers.slice(0, 2).join(", ")}`);
  if (row.geo.resolution === "mapped") parts.push(`entidade reconhecida: ${row.geo.matchedEntity}`);
  if (row.geo.id !== "nacional") parts.push(`área: ${row.geo.label}`);
  return parts.join(" · ");
}

function questionForPriority(row) {
  const model = row.item.public_health_model || {};
  const areas = model.matched_areas || [];
  const area = areas[0] || row.geo.label;
  const fields = row.item.join_recipe?.suggested_keys || row.item.shared_fields || [];
  if ((row.item.dimension_kinds || []).includes("territorial")) {
    return `Onde há maior desigualdade territorial em ${area.toLowerCase()} quando cruzamos ${fields.slice(0, 2).join(" + ") || "as chaves disponíveis"}?`;
  }
  if ((row.item.dimension_kinds || []).includes("entidade")) {
    return `Que ULS, hospital ou entidade fica fora do padrão esperado neste cruzamento?`;
  }
  if ((row.item.dimension_kinds || []).includes("temporal")) {
    return `O sinal está a piorar no tempo ou é um efeito pontual de reporte?`;
  }
  return `Que decisão muda se esta relação for validada com dados reais?`;
}

function aggregateHealthGeo(rows) {
  const base = new Map(PUBLIC_HEALTH_GEO.map((geo) => [geo.id, {...geo, count: 0, priority: 0, impact: 0, likelihood: 0, risk: 0, entities: 0}]));
  base.set("agregar", {...PUBLIC_HEALTH_AGGREGATION_GEO, count: 0, priority: 0, impact: 0, likelihood: 0, risk: 0, entities: 0});
  base.set("nacional", {...PUBLIC_HEALTH_NATIONAL_GEO, count: 0, priority: 0, impact: 0, likelihood: 0, risk: 0, entities: 0});
  rows.forEach((row) => {
    const entry = base.get(row.geo.id) || base.get("nacional");
    const model = row.item.public_health_model || {};
    entry.count += 1;
    entry.priority += row.priority;
    entry.impact += {alto: 3, medio: 2, baixo: 1}[model.impact?.level || "baixo"];
    entry.likelihood += {alta: 3, media: 2, baixa: 1}[model.likelihood?.level || "baixa"];
    entry.risk += row.risk;
    entry.entities += (row.item.dimension_kinds || []).includes("entidade") ? 1 : 0;
  });
  return [...base.values()].map((entry) => ({
    ...entry,
    avgPriority: entry.count ? entry.priority / entry.count : 0,
    avgImpact: entry.count ? entry.impact / entry.count : 0,
    avgLikelihood: entry.count ? entry.likelihood / entry.count : 0,
    avgRisk: entry.count ? entry.risk / entry.count : 0,
  }));
}

function renderTopOpportunities() {
  if (!topOpportunities) return;
  clearElement(topOpportunities);
  const rows = filteredCorrelations()
    .slice()
    .sort((a, b) => opportunityPriority(b) - opportunityPriority(a) || Number(b.semantic_score || 0) - Number(a.semantic_score || 0))
    .slice(0, 5);
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem oportunidades para os filtros atuais.";
    topOpportunities.appendChild(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  rows.forEach((item, index) => {
    const model = item.public_health_model || {};
    const card = document.createElement("button");
    card.type = "button";
    card.className = "top-opportunity-card";
    const rank = document.createElement("span");
    rank.textContent = `#${index + 1}`;
    const title = document.createElement("strong");
    title.textContent = `${compactTitle(item.source_title, 34)} / ${compactTitle(item.target_title, 34)}`;
    const meta = document.createElement("small");
    meta.textContent = `${likelihoodLabel(model.likelihood?.level || "baixa")} viabilidade · ${impactLabel(model.impact?.level || "baixo")} impacto · ${(item.risk_flags || []).length ? "validar risco" : "baixo risco"}`;
    const keys = document.createElement("em");
    keys.textContent = (item.join_recipe?.suggested_keys || item.shared_fields || []).slice(0, 3).join(", ") || "chave a validar";
    card.append(rank, title, meta, keys);
    card.addEventListener("click", () => {
      state.selectedCorrelationKey = relationKey(item);
      setActiveTab("tables");
      requestAnimationFrame(() => correlationTable?.scrollIntoView({block: "start", behavior: "smooth"}));
    });
    fragment.appendChild(card);
  });
  topOpportunities.appendChild(fragment);
}

function renderDataAnalytics() {
  const payload = state.dataPayload;
  if (!payload) {
    dataAnalyticsMeta.textContent = "Escolhe um dataset para medir sinais reais.";
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
    empty.textContent = "Sem correlações fortes nesta amostra.";
    dataCorrelationList.appendChild(empty);
    return;
  }
  rows.slice(0, 10).forEach((row) => {
    const item = document.createElement("div");
    item.className = row.correlation >= 0 ? "data-correlation-row is-positive" : "data-correlation-row is-negative";
    const label = document.createElement("span");
    label.textContent = `${compactTitle(row.label_a, 28)} / ${compactTitle(row.label_b, 28)}`;
    label.title = `${safeText(row.label_a)} / ${safeText(row.label_b)}`;
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
  clearElement(featureDetailPanel);
  const rows = payload.feature_importance || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Poucos dados-chave nesta amostra.";
    dataFeatureImportance.appendChild(empty);
    const detail = document.createElement("p");
    detail.className = "meta";
    detail.textContent = "Escolhe um dataset mais rico ou aumenta a amostra.";
    featureDetailPanel.append(detail);
    return;
  }
  const maxScore = Math.max(...rows.map((row) => row.score), 1);
  const selected = rows.find((row) => row.field === state.selectedFeatureKey) || rows[0];
  state.selectedFeatureKey = selected.field;
  rows.slice(0, 9).forEach((row, index) => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `feature-row is-${row.kind} is-${row.selection}`;
    if (row.field === state.selectedFeatureKey) item.classList.add("is-active");
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
      state.selectedFeatureKey = row.field;
      renderFeatureImportance(payload);
    });
    dataFeatureImportance.appendChild(item);
  });
  renderFeatureDetail(selected);
}

function renderFeatureDetail(row) {
  clearElement(featureDetailPanel);
  if (!row) return;
  const title = document.createElement("strong");
  title.textContent = row.label || row.field;
  const meta = document.createElement("p");
  meta.className = "meta";
  meta.textContent = `${row.kind} · ${row.selection === "forte" ? "sinal forte" : "sinal exploratório"} · score ${formatDecimal(row.score, 1)}`;
  const drivers = document.createElement("div");
  drivers.className = "feature-driver-list";
  (row.drivers || ["sinal exploratório"]).forEach((driver) => {
    const chip = document.createElement("span");
    chip.textContent = driver;
    drivers.appendChild(chip);
  });
  const action = document.createElement("button");
  action.type = "button";
  action.className = "ghost-button";
  action.textContent = "Filtrar ligações";
  action.addEventListener("click", () => {
    analyticsSearch.value = row.field;
    state.search = row.field;
    invalidateFilterCache();
    setActiveTab("semantic");
  });
  featureDetailPanel.append(title, meta, drivers, action);
}

function renderPcaChart(payload) {
  clearElement(dataPcaChart);
  const pca = payload.pca_summary || {};
  const width = Math.max(420, dataPcaChart.closest(".pca-wrap")?.clientWidth || 420);
  const height = 280;
  dataPcaChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  if (!pca.available || !pca.loadings?.length) {
    dataPcaMeta.textContent = pca.reason || "PCA indisponível para esta amostra. Usa Dataset rico para procurar uma amostra com pelo menos duas medidas.";
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "PCA requer duas ou mais medidas numéricas.";
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
    const titleNode = svgNode("title");
    titleNode.textContent = `${item.label || item.field}: PC1 ${formatDecimal(item.pc1, 2)}, PC2 ${formatDecimal(item.pc2, 2)}, magnitude ${formatDecimal(item.magnitude, 2)}`;
    group.appendChild(titleNode);
    group.appendChild(svgNode("circle", {r: Math.max(5, Math.min(14, item.magnitude * 12))}));
    const label = svgNode("text", {x: 10, y: 4});
    label.textContent = compactTitle(item.label, 20);
    group.appendChild(label);
    group.addEventListener("click", () => {
      analyticsSearch.value = item.field || item.label;
      state.search = item.field || item.label;
      invalidateFilterCache();
      setActiveTab("semantic");
    });
    dataPcaChart.appendChild(group);
  });
}

function renderNumericProfiles(payload) {
  clearElement(dataNumericProfiles);
  const rows = payload.numeric_profiles || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem medidas numéricas consistentes.";
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
      chip.textContent = `${label}: ${formatDisplayValue(value)}`;
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
    empty.textContent = "Sem dimensões úteis nesta amostra.";
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
      name.textContent = compactTitle(formatDisplayValue(value.value), 36);
      name.title = formatDisplayValue(value.value);
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
    empty.textContent = "Tendência indisponível: faltam períodos.";
    dataTrendChart.appendChild(empty);
    return;
  }
  const points = trend.points;
  const values = points.map((point) => point.avg);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const left = 66;
  const right = width - 42;
  const top = 48;
  const bottom = height - 54;
  const xStep = points.length > 1 ? (right - left) / (points.length - 1) : 0;
  const coords = points.map((point, index) => ({
    x: left + index * xStep,
    y: bottom - ((point.avg - min) / span) * (bottom - top),
    point,
  }));
  const axis = svgNode("g", {class: "data-trend-axis"});
  axis.appendChild(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom}));
  axis.appendChild(svgNode("line", {x1: left, y1: top, x2: left, y2: bottom}));
  const title = svgNode("text", {x: left, y: 22});
  title.textContent = compactTitle(trend.label, 42);
  axis.appendChild(title);
  const minLabel = svgNode("text", {x: left - 8, y: bottom, "text-anchor": "end"});
  minLabel.textContent = formatDecimal(min, 1);
  const maxLabel = svgNode("text", {x: left - 8, y: top + 4, "text-anchor": "end"});
  maxLabel.textContent = formatDecimal(max, 1);
  axis.append(minLabel, maxLabel);
  dataTrendChart.appendChild(axis);

  if (points.length === 2) {
    const [first, last] = points;
    const delta = last.avg - first.avg;
    const pct = first.avg ? (delta / Math.abs(first.avg)) * 100 : null;
    const note = svgNode("text", {x: right, y: 22, "text-anchor": "end", class: "data-trend-note"});
    note.textContent = `2 pontos · variação ${delta >= 0 ? "+" : ""}${formatDecimal(delta, 1)}${pct === null ? "" : ` (${delta >= 0 ? "+" : ""}${formatDecimal(pct, 1)}%)`}`;
    dataTrendChart.appendChild(note);
  }

  const path = svgNode("path", {
    class: `data-trend-line ${points.length <= 2 ? "is-sparse" : ""}`.trim(),
    d: coords.map((coord, index) => `${index ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  });
  dataTrendChart.appendChild(path);
  coords.forEach((coord) => {
    const group = svgNode("g", {class: "data-trend-point", transform: `translate(${coord.x}, ${coord.y})`});
    const tooltip = svgNode("title");
    tooltip.textContent = `${coord.point.period}: ${formatDecimal(coord.point.avg, 2)} · ${formatNumber(coord.point.count || 0)} registos`;
    group.appendChild(tooltip);
    group.appendChild(svgNode("circle", {r: 4}));
    const label = svgNode("text", {"text-anchor": coord.x > width - 90 ? "end" : "middle", y: 19});
    label.textContent = coord.point.period;
    group.appendChild(label);
    dataTrendChart.appendChild(group);
  });
}

function trendPoints(trend) {
  return (trend?.points || [])
    .map((point, index) => ({
      x: index,
      period: point.period,
      value: Number(point.avg),
      observed: true,
    }))
    .filter((point) => Number.isFinite(point.value));
}

function predictiveEligibility(trend, payload = null) {
  const points = trendPoints(trend);
  const sampleSize = payload?.sample_size || 0;
  const recordsPerPeriod = points.length ? sampleSize / points.length : 0;
  if (points.length < 4) {
    return {ok: false, points, reason: "Poucos períodos para projetar com fiabilidade.", score: 0};
  }
  if (points.length > 50) {
    return {ok: false, points, reason: "Série longa: precisa de agregação, sazonalidade ou modelo próprio.", score: 0};
  }
  if (sampleSize && recordsPerPeriod < 8) {
    return {ok: false, points, reason: "Volume baixo por período: comparação instável.", score: 0};
  }
  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const mean = values.reduce((acc, value) => acc + value, 0) / values.length;
  const span = max - min;
  const relativeSpan = Math.abs(span / (Math.abs(mean) || 1));
  const uniqueValues = new Set(values.map((value) => formatDecimal(value, 6))).size;
  if (uniqueValues < 3) {
    return {ok: false, points, reason: "Valores quase constantes: não há sinal útil para projetar.", score: 0};
  }
  if (!span || relativeSpan < 0.015) {
    return {ok: false, points, reason: "Variação baixa: a projeção não acrescenta valor.", score: 0};
  }
  const score = Math.min(
    100,
    Math.round(
      Math.min(40, points.length * 5)
      + Math.min(30, recordsPerPeriod || sampleSize / 4)
      + Math.min(20, relativeSpan * 180)
      + Math.min(10, uniqueValues * 1.5)
    )
  );
  return {ok: true, points, reason: "", score};
}

function predictiveIndicatorCandidates(payload) {
  const trends = (payload?.trends || []).filter((trend) => (trend.points || []).length >= 2);
  return trends
    .map((trend) => {
      const eligibility = predictiveEligibility(trend, payload);
      return {
        trend,
        eligibility,
        label: trend.label || "Série temporal",
        points: eligibility.points.length,
      };
    })
    .filter((row) => row.eligibility.ok)
    .sort((a, b) => (b.eligibility.score - a.eligibility.score) || (b.points - a.points));
}

function predictiveDatasetCandidatesFromCatalog() {
  const datasets = state.payload?.datasets || [];
  return datasets
    .map((dataset) => {
      const text = normalizeSearchText(`${dataset.dataset_id || ""} ${dataset.title || ""}`);
      let score = 0;
      const reasons = [];
      if (/mensal|mes|m[eê]s/.test(text)) {
        score += 45;
        reasons.push("mensal");
      }
      if (/trimestr|quarter/.test(text)) {
        score += 36;
        reasons.push("trimestral");
      }
      if (/diari|dia|tempo real/.test(text)) {
        score += 30;
        reasons.push("diário");
      }
      if (/evolucao|evolu[cç][aã]o|historico|hist[oó]rico|sazonal|serie|s[eé]rie/.test(text)) {
        score += 26;
        reasons.push("evolução");
      }
      if ((dataset.metric_candidate_count || 0) > 0) {
        score += Math.min(20, dataset.metric_candidate_count * 4);
        reasons.push(`${dataset.metric_candidate_count} medida(s) provável(eis)`);
      }
      if ((dataset.field_count || 0) >= 6) {
        score += 8;
        reasons.push("schema rico");
      }
      return {dataset, score, reasons};
    })
    .filter((row) => row.score >= 30)
    .sort((a, b) => b.score - a.score || (b.dataset.metric_candidate_count || 0) - (a.dataset.metric_candidate_count || 0))
    .slice(0, 10);
}

function renderPredictiveDatasetCandidates() {
  if (!predictiveDatasetCandidates) return;
  clearElement(predictiveDatasetCandidates);
  const rows = predictiveDatasetCandidatesFromCatalog();
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Ainda não há sugestões temporais no catálogo carregado.";
    predictiveDatasetCandidates.appendChild(empty);
    return;
  }
  rows.forEach(({dataset, score, reasons}) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = dataset.dataset_id === state.selectedDataDataset ? "predictive-dataset-card is-active" : "predictive-dataset-card";
    const title = document.createElement("strong");
    title.textContent = compactTitle(dataset.title || dataset.dataset_id, 70);
    title.title = dataset.title || dataset.dataset_id;
    const meta = document.createElement("small");
    meta.textContent = `${dataset.mega_theme || "Catálogo"} · ${reasons.slice(0, 3).join(" · ")} · score ${formatDecimal(score, 0)}`;
    const action = document.createElement("span");
    action.textContent = dataset.dataset_id === state.selectedDataDataset ? "em análise" : "analisar";
    button.append(title, meta, action);
    button.addEventListener("click", () => {
      state.selectedDataDataset = dataset.dataset_id;
      if (dataDatasetSelect) dataDatasetSelect.value = dataset.dataset_id;
      state.datasetMode = "manual";
      renderDatasetMode();
      loadDataAnalytics().then(() => setActiveTab("predictive")).catch(showDataError);
    });
    predictiveDatasetCandidates.appendChild(button);
  });
}

function bestPredictiveTrend(payload, selectedLabel = "") {
  const candidates = predictiveIndicatorCandidates(payload);
  if (selectedLabel) {
    const selected = candidates.find((row) => row.label === selectedLabel);
    if (selected) return selected.trend;
  }
  return candidates[0]?.trend || null;
}

function forecastFromTrend(trend, payload = null) {
  const eligibility = predictiveEligibility(trend, payload);
  const points = eligibility.points;
  if (!eligibility.ok) return {available: false, label: trend?.label || "Série temporal", points, reason: eligibility.reason};
  const n = points.length;
  const sumX = points.reduce((acc, point) => acc + point.x, 0);
  const sumY = points.reduce((acc, point) => acc + point.value, 0);
  const sumXY = points.reduce((acc, point) => acc + point.x * point.value, 0);
  const sumXX = points.reduce((acc, point) => acc + point.x * point.x, 0);
  const denominator = (n * sumXX) - (sumX * sumX) || 1;
  const slope = ((n * sumXY) - (sumX * sumY)) / denominator;
  const intercept = (sumY - slope * sumX) / n;
  const residuals = points.map((point) => Math.abs(point.value - (intercept + slope * point.x)));
  const error = residuals.reduce((acc, value) => acc + value, 0) / residuals.length;
  const forecast = Array.from({length: 3}, (_, index) => {
    const x = points.length + index;
    return {
      x,
      period: `+${index + 1}`,
      value: intercept + slope * x,
      observed: false,
    };
  });
  return {
    available: true,
    label: trend.label,
    points: [...points, ...forecast],
    slope,
    error,
    last: points.at(-1),
  };
}

function renderPredictiveIndicatorFilter(candidates) {
  if (!predictiveIndicatorFilter) return;
  clearElement(predictiveIndicatorFilter);
  if (!candidates.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Sem indicadores com fiabilidade suficiente";
    predictiveIndicatorFilter.appendChild(option);
    predictiveIndicatorFilter.disabled = true;
    state.selectedPredictiveIndicator = "";
    return;
  }
  predictiveIndicatorFilter.disabled = false;
  candidates.forEach((row) => {
    const option = document.createElement("option");
    option.value = row.label;
    option.textContent = `${compactTitle(row.label, 54)} · ${row.points} períodos · score ${row.eligibility.score}`;
    predictiveIndicatorFilter.appendChild(option);
  });
  if (!candidates.some((row) => row.label === state.selectedPredictiveIndicator)) {
    state.selectedPredictiveIndicator = candidates[0].label;
  }
  predictiveIndicatorFilter.value = state.selectedPredictiveIndicator;
}

function predictiveConfidence(payload, forecast) {
  let score = 0;
  if ((payload?.sample_size || 0) >= 60) score += 2;
  if ((payload?.numeric_profiles || []).length >= 3) score += 2;
  if (forecast?.available && forecast.points?.filter((point) => point.observed).length >= 4) score += 3;
  if ((payload?.correlations || []).length) score += 1;
  if ((payload?.categorical_profiles || []).length) score += 1;
  if (score >= 7) return "média";
  if (score >= 4) return "baixa-média";
  return "baixa";
}

function clearPredictive() {
  [predictiveKpis, predictiveForecastChart, predictiveDrivers, predictiveScenarios, predictiveRisk].forEach((node) => {
    if (node) clearElement(node);
  });
}

function addPredictiveKpi(label, value, detail) {
  const card = document.createElement("div");
  const span = document.createElement("span");
  span.textContent = label;
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = detail;
  card.append(span, strong, small);
  predictiveKpis.appendChild(card);
}

function renderPredictiveForecast(forecast) {
  clearElement(predictiveForecastChart);
  const width = Math.max(520, predictiveForecastChart.closest(".predictive-chart-wrap")?.clientWidth || 520);
  const height = 270;
  predictiveForecastChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  if (!forecast?.available) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = forecast?.reason || "Projeção linear não aplicável.";
    predictiveForecastChart.appendChild(empty);
    if (forecast?.points?.length) {
      const note = svgNode("text", {x: width / 2, y: (height / 2) + 24, "text-anchor": "middle", fill: "#657489"});
      note.textContent = `${forecast.points.length} períodos observados.`;
      predictiveForecastChart.appendChild(note);
    }
    return;
  }
  const values = forecast.points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const left = 58;
  const right = width - 32;
  const top = 34;
  const bottom = height - 46;
  const xStep = forecast.points.length > 1 ? (right - left) / (forecast.points.length - 1) : 0;
  const coords = forecast.points.map((point, index) => ({
    x: left + index * xStep,
    y: bottom - ((point.value - min) / span) * (bottom - top),
    point,
  }));
  const axis = svgNode("g", {class: "predictive-axis"});
  axis.append(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom}), svgNode("line", {x1: left, y1: top, x2: left, y2: bottom}));
  const title = svgNode("text", {x: left, y: 20});
  title.textContent = compactTitle(forecast.label, 58);
  axis.appendChild(title);
  predictiveForecastChart.appendChild(axis);

  const observed = coords.filter((coord) => coord.point.observed);
  const projected = coords.filter((coord, index) => !coord.point.observed || index >= observed.length - 1);
  predictiveForecastChart.appendChild(svgNode("path", {
    class: "predictive-line observed",
    d: observed.map((coord, index) => `${index ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  }));
  predictiveForecastChart.appendChild(svgNode("path", {
    class: "predictive-line projected",
    d: projected.map((coord, index) => `${index ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  }));
  coords.forEach((coord) => {
    const group = svgNode("g", {class: `predictive-point ${coord.point.observed ? "observed" : "projected"}`, transform: `translate(${coord.x}, ${coord.y})`});
    const titleNode = svgNode("title");
    titleNode.textContent = `${coord.point.period}: ${formatDecimal(coord.point.value, 2)}`;
    group.append(titleNode, svgNode("circle", {r: coord.point.observed ? 4 : 3.5}));
    const label = svgNode("text", {y: 18, "text-anchor": coord.x > width - 70 ? "end" : "middle"});
    label.textContent = coord.point.period;
    group.appendChild(label);
    predictiveForecastChart.appendChild(group);
  });
}

function renderPredictiveDrivers(payload) {
  clearElement(predictiveDrivers);
  const rows = [
    ...(payload.feature_importance || []).slice(0, 4).map((row) => ({
      label: row.label || row.field,
      value: formatDecimal(row.score || 0, 1),
      detail: `${row.selection || "sinal"} · ${(row.drivers || []).slice(0, 2).join(" · ") || "driver exploratório"}`,
    })),
    ...(payload.correlations || []).slice(0, 3).map((row) => ({
      label: `${row.label_a} / ${row.label_b}`,
      value: formatDecimal(row.correlation, 2),
      detail: `${formatNumber(row.samples || 0)} pares · correlação observada`,
    })),
  ].slice(0, 7);
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem drivers fortes nesta amostra.";
    predictiveDrivers.appendChild(empty);
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "predictive-list-item";
    const strong = document.createElement("strong");
    strong.textContent = compactTitle(row.label, 58);
    strong.title = row.label;
    const value = document.createElement("em");
    value.textContent = row.value;
    const small = document.createElement("small");
    small.textContent = row.detail;
    item.append(strong, value, small);
    predictiveDrivers.appendChild(item);
  });
}

function renderPredictiveScenarios(forecast) {
  clearElement(predictiveScenarios);
  if (!forecast?.available) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = forecast?.reason || "Cenários desligados para evitar falsa precisão.";
    predictiveScenarios.appendChild(empty);
    return;
  }
  const last = forecast.last?.value || 0;
  const baseline = forecast.points.at(-1)?.value || last;
  const scenarios = [
    ["Conservador", baseline - forecast.error, "projeta com erro médio subtraído"],
    ["Base", baseline, "continua o declive observado"],
    ["Stress", baseline + forecast.error, "projeta com erro médio somado"],
  ];
  scenarios.forEach(([label, value, detail]) => {
    const card = document.createElement("div");
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = formatDecimal(value, 1);
    const small = document.createElement("small");
    small.textContent = detail;
    card.append(span, strong, small);
    predictiveScenarios.appendChild(card);
  });
}

function renderPredictiveRisk(payload, forecast) {
  clearElement(predictiveRisk);
  const risks = [];
  if (!forecast?.available) risks.push(["Sem projeção linear", forecast?.reason || "Não há série temporal elegível."]);
  if ((payload.sample_size || 0) < 40) risks.push(["Amostra curta", "A projeção pode oscilar com poucos registos."]);
  if ((payload.numeric_profiles || []).length < 2) risks.push(["Poucas medidas", "Faltam variáveis contínuas para comparar drivers."]);
  if (!(payload.correlations || []).length) risks.push(["Sem correlações", "O modelo não tem pares fortes para explicar variação."]);
  risks.push(["Uso correto", "Exploratório: serve para triagem, não para decisão causal."]);
  risks.forEach(([label, detail]) => {
    const item = document.createElement("div");
    item.className = "predictive-list-item";
    const strong = document.createElement("strong");
    strong.textContent = label;
    const small = document.createElement("small");
    small.textContent = detail;
    item.append(strong, small);
    predictiveRisk.appendChild(item);
  });
}

function renderPredictiveAnalytics() {
  clearPredictive();
  renderPredictiveDatasetCandidates();
  const payload = state.dataPayload;
  if (!payload) {
    predictiveMeta.textContent = "Escolhe ou analisa um dataset em Dados reais para ativar a projeção.";
    renderPredictiveIndicatorFilter([]);
    if (state.selectedDataDataset && state._predictiveLoadDataset !== state.selectedDataDataset) {
      state._predictiveLoadDataset = state.selectedDataDataset;
      loadDataAnalytics().catch(showDataError);
    }
    addPredictiveKpi("Estado", "sem dados", "usa a amostra real");
    return;
  }
  state._predictiveLoadDataset = "";
  const candidates = predictiveIndicatorCandidates(payload);
  renderPredictiveIndicatorFilter(candidates);
  const trend = bestPredictiveTrend(payload, state.selectedPredictiveIndicator);
  const forecast = forecastFromTrend(trend, payload);
  const confidence = predictiveConfidence(payload, forecast);
  const slope = forecast?.available ? forecast.slope : null;
  predictiveMeta.textContent = `${payload.title} · ${formatNumber(payload.sample_size)} registos · fiabilidade ${confidence}.`;
  addPredictiveKpi("Série", trend ? compactTitle(trend.label, 26) : "indisponível", trend ? `${trend.points.length} períodos` : "sem eixo temporal");
  addPredictiveKpi("Projeção", forecast?.available ? "ativa" : "não aplicável", forecast?.available ? "linear curta" : (forecast?.reason || "sem série elegível"));
  addPredictiveKpi("Elegíveis", formatNumber(candidates.length), "indicadores com volume suficiente");
  addPredictiveKpi("Tendência", slope === null ? "-" : `${slope >= 0 ? "+" : ""}${formatDecimal(slope, 2)}`, "variação média por período");
  renderPredictiveForecast(forecast);
  renderPredictiveDrivers(payload);
  renderPredictiveScenarios(forecast);
  renderPredictiveRisk(payload, forecast);
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
      meta: state.lowRiskOnly ? "Filtro ativo · clicar para limpar" : `${formatNumber(correlations.length)} ligações visíveis`,
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
      meta: `${formatNumber(dominantKindCount)} ocorrências nas ligações`,
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
  distributionMeta.textContent = `${formatNumber(correlations.length)} ligações alimentam esta distribuição.`;
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
  modelMeta.textContent = `${formatNumber(correlations.length)} ligações filtradas · temas no arco exterior, dimensões no núcleo.`;

  if (!correlations.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem ligações para desenhar o modelo.";
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

function createSmallButton(text, onClick, className = "ghost-button") {
  const button = document.createElement("button");
  button.type = "button";
  button.className = className;
  button.textContent = text;
  button.addEventListener("click", onClick);
  return button;
}

function renderPublicHealthCockpit(rows) {
  const visibleRows = rows.slice(0, state.publicHealthRenderLimit);
  if (!state.selectedPublicHealthKey && rows.length) {
    state.selectedPublicHealthKey = rows[0].key;
  }
  const selected = rows.find((row) => row.key === state.selectedPublicHealthKey) || rows[0];
  renderPublicHealthKpis(rows);
  renderPublicHealthLayerControls();
  renderPublicHealthPriorityList(visibleRows, rows.length);
  renderPublicHealthMap(rows);
  renderPublicHealthDecisionDetail(selected);
  renderPublicHealthQuestions(rows);
  renderPublicHealthPriorityTable(rows, rows.length);
}

function renderPublicHealthKpis(rows) {
  clearElement(publicHealthKpis);
  const priorityCount = rows.filter((row) => row.decision === "Priorizar").length;
  const nationalCount = rows.filter((row) => row.geo.id === "nacional").length;
  const aggregateCount = rows.filter((row) => row.geo.id === "agregar").length;
  const mappedCount = rows.filter((row) => row.geo.resolution === "mapped").length;
  const avgPriority = rows.length ? rows.reduce((sum, row) => sum + row.priority, 0) / rows.length : 0;
  const geoCount = new Set(rows.map((row) => row.geo.id)).size;
  [
    ["Score médio", formatDecimal(avgPriority, 0)],
    ["Priorizar", formatNumber(priorityCount)],
    ["Âmbitos", formatNumber(geoCount)],
    ["Agregados", formatNumber(mappedCount)],
    ["Por validar", formatNumber(aggregateCount)],
    ["Nacional", formatNumber(nationalCount)],
  ].forEach(([label, value]) => {
    const card = document.createElement("div");
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = value;
    card.append(span, strong);
    publicHealthKpis.appendChild(card);
  });
}

function renderPublicHealthLayerControls() {
  clearElement(publicHealthLayerControls);
  PUBLIC_HEALTH_LAYERS.forEach(([key, label]) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = key === state.activePublicHealthLayer ? "is-active" : "";
    button.textContent = label;
    button.addEventListener("click", () => {
      state.activePublicHealthLayer = key;
      renderPublicHealthMatrix();
    });
    publicHealthLayerControls.appendChild(button);
  });
}

function renderPublicHealthPriorityList(rows, total) {
  clearElement(publicHealthPriorityList);
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem prioridades para os filtros atuais.";
    publicHealthPriorityList.appendChild(empty);
    return;
  }
  const fragment = document.createDocumentFragment();
  rows.slice(0, 12).forEach((row, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `priority-card tone-${decisionTone(row.decision)}`;
    if (row.key === state.selectedPublicHealthKey) button.classList.add("is-active");
    const head = document.createElement("div");
    const rank = document.createElement("span");
    rank.textContent = `#${index + 1}`;
    const decision = document.createElement("em");
    decision.textContent = row.decision;
    head.append(rank, decision);
    const title = document.createElement("strong");
    title.textContent = `${compactTitle(row.item.source_title, 34)} / ${compactTitle(row.item.target_title, 34)}`;
    title.title = `${row.item.source_title} / ${row.item.target_title}`;
    const meta = document.createElement("small");
    const scope = row.geo.id === "nacional"
      ? "Nacional · não regional"
      : row.geo.id === "agregar"
        ? "Entidade territorial · por validar"
        : row.geo.resolution === "mapped"
          ? `${row.geo.label} · ${row.geo.matchedEntity}`
          : `${row.geo.label} · região inferida`;
    meta.textContent = `${scope} · score ${row.priority} · viabilidade ${formatDecimal(row.viability, 0)}`;
    button.append(head, title, meta);
    button.addEventListener("click", () => {
      state.selectedPublicHealthKey = row.key;
      renderPublicHealthMatrix();
    });
    fragment.appendChild(button);
  });
  publicHealthPriorityList.appendChild(fragment);
  if (rows.length < total) {
    publicHealthPriorityList.appendChild(createSmallButton(
      `Mostrar mais ${Math.min(40, total - rows.length)} prioridades`,
      () => {
        state.publicHealthRenderLimit = Math.min(total, state.publicHealthRenderLimit + 40);
        renderPublicHealthMatrix();
      },
      "show-more-button",
    ));
  }
}

function mapLayerValue(entry) {
  if (state.activePublicHealthLayer === "impact") return entry.avgImpact / 3;
  if (state.activePublicHealthLayer === "likelihood") return entry.avgLikelihood / 3;
  if (state.activePublicHealthLayer === "risk") return Math.min(1, entry.avgRisk / 10);
  if (state.activePublicHealthLayer === "entities") return Math.min(1, entry.entities / Math.max(1, entry.count));
  return Math.min(1, entry.avgPriority / 90);
}

function renderNationalSummary(rows, nationalEntry) {
  if (!publicHealthNationalSummary) return;
  clearElement(publicHealthNationalSummary);
  const selected = rows.find((row) => row.key === state.selectedPublicHealthKey);
  const nationalRows = rows.filter((row) => row.geo.id === "nacional");
  const aggregateRows = rows.filter((row) => row.geo.id === "agregar");
  const mappedRows = rows.filter((row) => row.geo.resolution === "mapped");
  const pendingCount = aggregateRows.length + nationalRows.length;
  const pendingRatio = rows.length ? pendingCount / rows.length : 0;
  const card = document.createElement("button");
  card.type = "button";
  card.className = `national-summary-card ${pendingRatio >= 0.7 ? "is-warning" : ""}`.trim();
  if (selected?.geo.id === "nacional" || selected?.geo.id === "agregar") card.classList.add("is-active");
  const label = document.createElement("span");
  label.textContent = aggregateRows.length ? "Entidade territorial por validar" : "Vista nacional";
  const value = document.createElement("strong");
  value.textContent = aggregateRows.length
    ? `${formatNumber(aggregateRows.length)} hipóteses sem região segura`
    : `${formatNumber(nationalEntry?.count || 0)} hipóteses nacionais`;
  const detail = document.createElement("small");
  detail.textContent = aggregateRows.length
    ? `${formatNumber(mappedRows.length)} hipóteses já foram agregadas por correspondência ULS/hospital→Região. As restantes têm sinal territorial indireto, mas faltam nomes reconhecíveis para classificar com segurança.`
    : "Não é erro: são hipóteses de leitura nacional. Só devem entrar no mapa regional quando houver ULS, hospital, ARS, concelho/distrito ou região.";
  card.append(label, value, detail);
  card.addEventListener("click", () => {
    const candidate = aggregateRows[0] || mappedRows[0] || nationalRows[0];
    if (candidate) {
      state.selectedPublicHealthKey = candidate.key;
      renderPublicHealthMatrix();
    }
  });
  publicHealthNationalSummary.appendChild(card);
}

function renderPublicHealthMap(rows) {
  clearElement(publicHealthMap);
  const width = 500;
  const height = 510;
  publicHealthMap.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const layerTitle = PUBLIC_HEALTH_LAYERS.find(([key]) => key === state.activePublicHealthLayer)?.[1] || "Impacto";
  const title = svgNode("text", {x: 22, y: 28, class: "health-map-title"});
  title.textContent = `Cartograma · layer: ${layerTitle}`;
  publicHealthMap.appendChild(title);
  const subtitle = svgNode("text", {x: 22, y: 48, class: "health-map-subtitle"});
  subtitle.textContent = "Blocos aproximados: regiões de saúde e ilhas. Nacional não é região no mapa.";
  publicHealthMap.appendChild(subtitle);

  const selected = rows.find((row) => row.key === state.selectedPublicHealthKey);
  const geoRows = aggregateHealthGeo(rows);
  const regionalRows = geoRows.filter((entry) => entry.id !== "nacional");
  const nationalEntry = geoRows.find((entry) => entry.id === "nacional");
  renderNationalSummary(rows, nationalEntry);
  regionalRows.forEach((entry) => {
    const value = mapLayerValue(entry);
    const level = Math.max(0, Math.min(5, Math.ceil(value * 5)));
    const group = svgNode("g", {
      class: `health-map-region layer-${state.activePublicHealthLayer} level-${level} ${selected?.geo.id === entry.id ? "is-selected" : ""}`.trim(),
    });
    const titleNode = svgNode("title");
    titleNode.textContent = entry.id === "agregar"
      ? `${entry.label}: ${formatNumber(entry.count)} hipóteses com entidade territorial por validar · score médio ${formatDecimal(entry.avgPriority, 0)}`
      : `${entry.label}: ${formatNumber(entry.count)} matches regionais · score médio ${formatDecimal(entry.avgPriority, 0)}`;
    group.appendChild(titleNode);
    group.appendChild(svgNode("rect", {x: entry.x, y: entry.y, width: entry.w, height: entry.h, rx: 8}));
    const labelX = entry.labelX || entry.x + entry.w / 2;
    const labelY = entry.labelY || entry.y + entry.h / 2;
    const label = svgNode("text", {x: labelX, y: labelY - 3, "text-anchor": "middle"});
    label.textContent = compactTitle(entry.label, entry.id === "lisboa" ? 15 : 18);
    const count = svgNode("text", {x: labelX, y: labelY + 15, "text-anchor": "middle", class: "health-map-count"});
    count.textContent = entry.count
      ? `${formatNumber(entry.count)} ${entry.id === "agregar" ? "validar" : "match"}`
      : "sem match";
    group.append(label, count);
    group.addEventListener("click", () => {
      const candidate = rows.find((row) => row.geo.id === entry.id);
      if (candidate) {
        state.selectedPublicHealthKey = candidate.key;
        renderPublicHealthMatrix();
      }
    });
    publicHealthMap.appendChild(group);
  });

  const dotCounts = new Map();
  rows.slice(0, 48).forEach((row) => {
    const geo = row.geo;
    if (geo.id === "nacional") return;
    const localIndex = dotCounts.get(geo.id) || 0;
    const localLimit = geo.w < 70 ? 2 : 5;
    if (localIndex >= localLimit) return;
    dotCounts.set(geo.id, localIndex + 1);
    const columns = Math.min(3, Math.max(1, Math.floor(geo.w / 34)));
    const col = localIndex % columns;
    const rowIndex = Math.floor(localIndex / columns);
    const x = geo.x + 16 + col * Math.max(18, (geo.w - 32) / Math.max(1, columns - 1));
    const y = geo.id === "nacional" ? geo.y + geo.h - 9 - rowIndex * 13 : geo.y + geo.h - 16 - rowIndex * 15;
    const dot = svgNode("circle", {
      cx: x,
      cy: y,
      r: row.decision === "Priorizar" ? 5 : 3.6,
      class: `health-map-dot tone-${decisionTone(row.decision)} ${row.key === state.selectedPublicHealthKey ? "is-selected" : ""}`.trim(),
    });
    const titleNode = svgNode("title");
    titleNode.textContent = `${row.decision}: ${row.priority} · ${row.item.source_title} / ${row.item.target_title}`;
    dot.appendChild(titleNode);
    dot.addEventListener("click", () => {
      state.selectedPublicHealthKey = row.key;
      renderPublicHealthMatrix();
    });
    publicHealthMap.appendChild(dot);
  });

  const legend = svgNode("g", {class: "health-map-legend"});
  [
    ["Cor", "layer ativo"],
    ["Bloco", "região SNS"],
    ["Ponto", "hipótese regional/validar"],
  ].forEach(([label, value], index) => {
    const x = 22 + index * 145;
    const y = 488;
    legend.appendChild(svgNode("rect", {x, y: y - 10, width: 10, height: 10, rx: 2}));
    const text = svgNode("text", {x: x + 16, y, class: "health-map-legend-text"});
    text.textContent = `${label}: ${value}`;
    legend.appendChild(text);
  });
  publicHealthMap.appendChild(legend);
}

function renderPublicHealthDecisionDetail(row) {
  clearElement(publicHealthDecisionDetail);
  if (!row) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Abre uma prioridade para ver drivers.";
    publicHealthDecisionDetail.appendChild(empty);
    return;
  }
  const title = document.createElement("strong");
  title.textContent = row.decision;
  title.className = `decision-title tone-${decisionTone(row.decision)}`;
  const relation = document.createElement("p");
  relation.textContent = `${row.item.source_title} / ${row.item.target_title}`;
  const scope = document.createElement("p");
  scope.className = "decision-scope";
  scope.textContent = row.geo.id === "nacional"
    ? "Âmbito: vista nacional, sem granularidade regional."
    : row.geo.id === "agregar"
      ? "Âmbito: entidade territorial detetada, mas sem correspondência regional segura."
      : row.geo.resolution === "mapped"
        ? `Âmbito: ${row.geo.label}, agregado por correspondência ${row.geo.matchedEntity} → Região de Saúde (${row.geo.mappingConfidence}).`
        : `Âmbito: ${row.geo.label}, com match regional heurístico (${row.geo.mappingConfidence || "média"}).`;
  const reason = document.createElement("p");
  reason.className = "meta";
  reason.textContent = row.reason;
  const drivers = document.createElement("div");
  drivers.className = "decision-driver-grid";
  [
    ["Magnitude", row.magnitude],
    ["Gravidade", row.severity],
    ["Tendência", row.trend],
    ["Inequidade", row.inequity],
    ["Viabilidade", row.viability],
    ["PNS", row.alignment],
  ].forEach(([label, value]) => {
    const item = document.createElement("div");
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = formatDecimal(value, 0);
    item.append(span, strong);
    drivers.appendChild(item);
  });
  const validation = document.createElement("p");
  validation.className = "meta";
  const keys = (row.item.join_recipe?.suggested_keys || row.item.shared_fields || []).slice(0, 4).join(", ") || "chave a validar";
  validation.textContent = `Validação antes do uso: confirmar tipo, nulos, duplicados, cardinalidade e granularidade das chaves (${keys}).`;
  const geoChecklist = document.createElement("div");
  geoChecklist.className = "geo-validation-note";
  const geoTitle = document.createElement("strong");
  geoTitle.textContent = row.geo.id === "agregar" ? "Como resolver este âmbito" : "Validação territorial";
  const geoText = document.createElement("span");
  geoText.textContent = row.geo.id === "agregar"
    ? "Procurar nome de ULS/hospital/ACES no dataset original ou acrescentar regra à tabela de correspondência antes de pintar uma região."
    : row.geo.resolution === "mapped"
      ? "Confirmar que a entidade pertence à região sugerida e que o período/dataset usa a hierarquia SNS atual."
      : "Confirmar se o campo territorial representa região, ARS, concelho, distrito, ULS ou hospital antes de decidir.";
  geoChecklist.append(geoTitle, geoText);
  publicHealthDecisionDetail.append(title, relation, scope, reason, drivers, geoChecklist, validation);
}

function renderPublicHealthQuestions(rows) {
  clearElement(publicHealthQuestions);
  const questions = [];
  rows.slice(0, 8).forEach((row) => {
    const question = questionForPriority(row);
    if (!questions.includes(question)) questions.push(question);
  });
  if (!questions.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem perguntas geradas para os filtros atuais.";
    publicHealthQuestions.appendChild(empty);
    return;
  }
  questions.slice(0, 6).forEach((question) => {
    const item = document.createElement("div");
    item.className = "public-health-question";
    item.textContent = question;
    publicHealthQuestions.appendChild(item);
  });
}

function renderPublicHealthPriorityTable(rows, total) {
  clearElement(publicHealthHypothesisMatrix);
  clearElement(publicHealthHypothesisDetail);
  const nationalCount = rows.filter((row) => row.geo.id === "nacional").length;
  const aggregateCount = rows.filter((row) => row.geo.id === "agregar").length;
  const mappedCount = rows.filter((row) => row.geo.resolution === "mapped").length;
  const regionalCount = rows.length - nationalCount - aggregateCount;
  publicHealthTableMeta.textContent = `${formatNumber(total)} hipóteses · ${formatNumber(regionalCount)} regionais (${formatNumber(mappedCount)} por entidade reconhecida) · ${formatNumber(aggregateCount)} por validar · ${formatNumber(nationalCount)} nacionais.`;

  const decisions = ["Priorizar", "Acompanhar", "Explorar", "Validar antes"];
  const scopes = [
    ["regional", "Regional"],
    ["agregar", "Por validar"],
    ["nacional", "Nacional"],
  ];
  const buckets = new Map();
  rows.forEach((row) => {
    const scope = row.geo.id === "nacional" ? "nacional" : row.geo.id === "agregar" ? "agregar" : "regional";
    const key = `${row.decision}|${scope}`;
    const bucket = buckets.get(key) || {decision: row.decision, scope, rows: [], score: 0, risk: 0};
    bucket.rows.push(row);
    bucket.score += row.priority;
    bucket.risk += row.risk;
    buckets.set(key, bucket);
  });
  const fallbackKey = [...buckets.values()]
    .sort((a, b) => b.rows.length - a.rows.length || (b.score / Math.max(1, b.rows.length)) - (a.score / Math.max(1, a.rows.length)))[0];
  if (!state.selectedPublicHealthMatrixCell || !buckets.has(state.selectedPublicHealthMatrixCell)) {
    state.selectedPublicHealthMatrixCell = fallbackKey ? `${fallbackKey.decision}|${fallbackKey.scope}` : "";
  }

  const corner = document.createElement("div");
  corner.className = "hypothesis-axis hypothesis-corner";
  corner.textContent = "Leitura × âmbito";
  publicHealthHypothesisMatrix.appendChild(corner);
  scopes.forEach(([, label]) => {
    const axis = document.createElement("div");
    axis.className = "hypothesis-axis";
    axis.textContent = label;
    publicHealthHypothesisMatrix.appendChild(axis);
  });
  decisions.forEach((decision) => {
    const axis = document.createElement("div");
    axis.className = "hypothesis-axis";
    axis.textContent = decision;
    publicHealthHypothesisMatrix.appendChild(axis);
    scopes.forEach(([scope, label]) => {
      const key = `${decision}|${scope}`;
      const bucket = buckets.get(key);
      const count = bucket?.rows.length || 0;
      const avgScore = bucket ? bucket.score / Math.max(1, count) : 0;
      const button = document.createElement("button");
      button.type = "button";
      button.className = `hypothesis-cell tone-${decisionTone(decision)} ${state.selectedPublicHealthMatrixCell === key ? "is-active" : ""}`.trim();
      const strong = document.createElement("strong");
      strong.textContent = formatNumber(count);
      const span = document.createElement("span");
      span.textContent = count ? `${label} · score ${formatDecimal(avgScore, 0)}` : "sem hipóteses";
      button.append(strong, span);
      button.disabled = count === 0;
      button.addEventListener("click", () => {
        state.selectedPublicHealthMatrixCell = key;
        const first = bucket?.rows[0];
        if (first) state.selectedPublicHealthKey = first.key;
        renderPublicHealthMatrix();
      });
      publicHealthHypothesisMatrix.appendChild(button);
    });
  });

  renderHypothesisCellDetail(buckets.get(state.selectedPublicHealthMatrixCell), rows);
}

function renderHypothesisCellDetail(bucket, allRows) {
  clearElement(publicHealthHypothesisDetail);
  if (!bucket || !bucket.rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Seleciona uma célula com hipóteses.";
    publicHealthHypothesisDetail.appendChild(empty);
    return;
  }
  const scopeLabel = bucket.scope === "nacional"
    ? "leitura nacional"
    : bucket.scope === "agregar"
      ? "entidade por validar"
      : "com match regional";
  const title = document.createElement("strong");
  title.className = "hypothesis-detail-title";
  title.textContent = `${bucket.decision} · ${scopeLabel}`;
  const ratio = document.createElement("p");
  ratio.className = "meta";
  ratio.textContent = `${formatNumber(bucket.rows.length)} de ${formatNumber(allRows.length)} hipóteses. Score médio ${formatDecimal(bucket.score / bucket.rows.length, 0)}.`;
  const explanation = document.createElement("p");
  explanation.className = "decision-scope";
  explanation.textContent = bucket.scope === "nacional"
    ? "Estas hipóteses têm leitura nacional. Não devem ser forçadas para uma região sem campo territorial adicional."
    : bucket.scope === "agregar"
      ? "Estas hipóteses têm sinal de ULS/hospital/ACES/USF, mas ainda não têm nome reconhecido ou regra de correspondência suficiente para pintar uma Região de Saúde."
      : "Estas hipóteses já têm sinal territorial suficiente para análise regional inicial. Ainda assim, valida granularidade, duplicados e período antes de cruzar datasets.";
  const list = document.createElement("div");
  list.className = "hypothesis-detail-list";
  bucket.rows.slice(0, 6).forEach((row) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = row.key === state.selectedPublicHealthKey ? "is-active" : "";
    const strong = document.createElement("strong");
    strong.textContent = `${compactTitle(row.item.source_title, 34)} / ${compactTitle(row.item.target_title, 34)}`;
    const small = document.createElement("small");
    small.textContent = `${row.reason} · validação: ${(row.item.risk_flags || []).join(", ") || "tipo, nulos, duplicados, cardinalidade"}`;
    button.append(strong, small);
    button.addEventListener("click", () => {
      state.selectedPublicHealthKey = row.key;
      renderPublicHealthMatrix();
    });
    list.appendChild(button);
  });
  publicHealthHypothesisDetail.append(title, ratio, explanation, list);
}

function pnsAxesForItem(item) {
  const text = normalizeSearchText([
    item.source_title,
    item.target_title,
    item.source_theme,
    item.target_theme,
    ...(item.shared_fields || []),
    ...((item.public_health_model || {}).impact?.drivers || []),
    ...((item.public_health_model || {}).matched_areas || []),
  ].join(" "));
  return LOCAL_PNS_AXES.filter(([, pattern]) => pattern.test(text)).map(([label]) => label);
}

function localPlanRows() {
  return publicHealthPriorityRows().map((row) => {
    const item = row.item;
    const model = item.public_health_model || {};
    const dimensionKinds = item.dimension_kinds || [];
    const suggestedKeys = item.join_recipe?.suggested_keys || item.shared_fields || [];
    const confidenceScore = item.confidence === "alta" ? 18 : item.confidence === "media" ? 10 : 4;
    const riskPenalty = Math.min(22, (item.risk_flags || []).length * 7 + row.risk);
    const linkabilityScore = Math.max(0, Math.min(100,
      Number(item.semantic_score || 0) * 6
      + confidenceScore
      + Math.min(20, suggestedKeys.length * 5)
      - riskPenalty,
    ));
    const territorialSignals = [
      dimensionKinds.includes("territorial"),
      dimensionKinds.includes("entidade"),
      row.geo.id !== "nacional",
      (model.matched_areas || []).some((area) => /ars|regiao|concelho|distrito|uls|hospital/i.test(area)),
      suggestedKeys.some((key) => /ars|regiao|região|local|geo|concelho|distrito|hospital|uls/i.test(key)),
    ].filter(Boolean).length;
    const territorialReadiness = Math.min(100, territorialSignals * 22);
    const temporalText = normalizeSearchText([item.source_title, item.target_title, ...suggestedKeys].join(" "));
    const surveillanceReadiness = Math.min(100,
      (dimensionKinds.includes("temporal") ? 35 : 0)
      + (/ano|periodo|período|mensal|data|evolucao|evolução|sazonal/.test(temporalText) ? 25 : 0)
      + (state.dataPayload?.trends?.length ? 20 : 0)
      + (item.confidence === "alta" ? 12 : 6),
    );
    const likelihoodRank = {alta: 3, media: 2, baixa: 1}[model.likelihood?.level || "baixa"];
    const impactRank = {alto: 3, medio: 2, baixo: 1}[model.impact?.level || "baixo"];
    const riskRelevance = Math.min(100, likelihoodRank * 18 + impactRank * 20 + row.inequity + Math.min(12, (model.impact?.drivers || []).length * 3));
    const pnsAxes = pnsAxesForItem(item);
    const pnsScore = Math.min(100, row.alignment * 7 + pnsAxes.length * 12);
    const needScore = Math.min(100, row.severity * 2.1 + row.inequity * 1.5 + riskRelevance * 0.32 + pnsScore * 0.25);
    const analysisCapacity = Math.min(100, linkabilityScore * 0.38 + territorialReadiness * 0.24 + surveillanceReadiness * 0.2 + row.viability * 1.0);
    const localScore = Math.round(needScore * 0.58 + analysisCapacity * 0.42);
    const readiness = territorialReadiness >= 45 ? "regional" : "sem-territorio";
    const priorityBand = needScore >= 72 && analysisCapacity >= 58
      ? "Pronto para plano"
      : needScore >= 72
        ? "Necessita dados"
        : analysisCapacity >= 58
          ? "Monitorizar"
          : "Explorar";
    return {
      ...row,
      linkabilityScore,
      territorialReadiness,
      surveillanceReadiness,
      riskRelevance,
      pnsScore,
      pnsAxes,
      needScore,
      analysisCapacity,
      localScore,
      readiness,
      priorityBand,
    };
  }).sort((a, b) => b.localScore - a.localScore || b.priority - a.priority);
}

function renderLocalPlanKpis(rows) {
  clearElement(localPlanKpis);
  const ready = rows.filter((row) => row.priorityBand === "Pronto para plano").length;
  const needsData = rows.filter((row) => row.priorityBand === "Necessita dados").length;
  const regional = rows.filter((row) => row.readiness === "regional").length;
  const surveillance = rows.filter((row) => row.surveillanceReadiness >= 55).length;
  [
    ["Prioridades locais", formatNumber(ready)],
    ["Backlog de dados", formatNumber(needsData)],
    ["Com território", `${formatNumber(regional)}/${formatNumber(rows.length)}`],
    ["Com surveillance", formatNumber(surveillance)],
  ].forEach(([label, value]) => {
    const card = document.createElement("div");
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = value;
    card.append(span, strong);
    localPlanKpis.appendChild(card);
  });
}

function createLocalMetric(label, value, max = 100) {
  const row = document.createElement("div");
  row.className = "local-metric-row";
  const span = document.createElement("span");
  span.textContent = label;
  const bar = document.createElement("i");
  const fill = document.createElement("b");
  fill.style.width = `${Math.max(0, Math.min(100, value / max * 100))}%`;
  bar.appendChild(fill);
  const strong = document.createElement("strong");
  strong.textContent = formatDecimal(value, 0);
  row.append(span, bar, strong);
  return row;
}

function renderLocalDiagnosis(rows) {
  clearElement(localDiagnosis);
  const axisCounts = new Map(LOCAL_PNS_AXES.map(([label]) => [label, 0]));
  rows.forEach((row) => row.pnsAxes.forEach((axis) => axisCounts.set(axis, (axisCounts.get(axis) || 0) + 1)));
  const nationalCount = rows.filter((row) => row.readiness !== "regional").length;
  const regionalCount = rows.length - nationalCount;
  const warning = document.createElement("div");
  warning.className = nationalCount > regionalCount ? "local-warning" : "local-note";
  warning.textContent = nationalCount > regionalCount
    ? "Insuficiente para mapa local: a maioria das hipóteses ainda não tem território classificável."
    : "Cobertura territorial suficiente para leitura local exploratória.";
  localDiagnosis.appendChild(warning);
  [...axisCounts.entries()].sort((a, b) => b[1] - a[1]).forEach(([axis, count]) => {
    localDiagnosis.appendChild(createLocalMetric(axis, count, Math.max(1, rows.length)));
  });
}

function localCellKey(needBand, capacityBand) {
  return `${needBand}|${capacityBand}`;
}

function renderLocalPriorityMatrix(rows) {
  clearElement(localPriorityMatrix);
  clearElement(localPriorityDetail);
  const needBands = [
    ["alta", "Necessidade alta"],
    ["media", "Necessidade média"],
    ["baixa", "Necessidade baixa"],
  ];
  const capacityBands = [
    ["pronta", "Pronta"],
    ["limitada", "A preparar"],
  ];
  const buckets = new Map();
  rows.forEach((row) => {
    const needBand = row.needScore >= 72 ? "alta" : row.needScore >= 48 ? "media" : "baixa";
    const capacityBand = row.analysisCapacity >= 58 ? "pronta" : "limitada";
    const key = localCellKey(needBand, capacityBand);
    const bucket = buckets.get(key) || {needBand, capacityBand, rows: [], score: 0};
    bucket.rows.push(row);
    bucket.score += row.localScore;
    buckets.set(key, bucket);
  });
  const best = [...buckets.values()].sort((a, b) => b.rows.length - a.rows.length || b.score - a.score)[0];
  if (!state.selectedLocalPriorityCell || !buckets.has(state.selectedLocalPriorityCell)) {
    state.selectedLocalPriorityCell = best ? localCellKey(best.needBand, best.capacityBand) : "";
  }
  const corner = document.createElement("div");
  corner.className = "local-priority-axis";
  corner.textContent = "Necessidade / capacidade";
  localPriorityMatrix.appendChild(corner);
  capacityBands.forEach(([, label]) => {
    const axis = document.createElement("div");
    axis.className = "local-priority-axis";
    axis.textContent = label;
    localPriorityMatrix.appendChild(axis);
  });
  needBands.forEach(([needBand, needLabel]) => {
    const axis = document.createElement("div");
    axis.className = "local-priority-axis";
    axis.textContent = needLabel;
    localPriorityMatrix.appendChild(axis);
    capacityBands.forEach(([capacityBand, capacityLabel]) => {
      const key = localCellKey(needBand, capacityBand);
      const bucket = buckets.get(key);
      const button = document.createElement("button");
      button.type = "button";
      button.className = state.selectedLocalPriorityCell === key ? "local-priority-cell is-active" : "local-priority-cell";
      button.disabled = !bucket;
      const strong = document.createElement("strong");
      strong.textContent = formatNumber(bucket?.rows.length || 0);
      const span = document.createElement("span");
      span.textContent = bucket ? `${capacityLabel} · score ${formatDecimal(bucket.score / bucket.rows.length, 0)}` : "sem hipóteses";
      button.append(strong, span);
      button.addEventListener("click", () => {
        state.selectedLocalPriorityCell = key;
        renderLocalPlans();
      });
      localPriorityMatrix.appendChild(button);
    });
  });
  renderLocalPriorityDetail(buckets.get(state.selectedLocalPriorityCell), rows);
}

function renderLocalPriorityDetail(bucket, rows) {
  clearElement(localPriorityDetail);
  if (!bucket) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Seleciona uma célula com hipóteses.";
    localPriorityDetail.appendChild(empty);
    return;
  }
  const title = document.createElement("strong");
  title.className = "local-priority-detail-title";
  title.textContent = `${formatNumber(bucket.rows.length)} hipóteses para planeamento`;
  const explanation = document.createElement("p");
  explanation.className = "decision-scope";
  explanation.textContent = bucket.capacityBand === "pronta"
    ? "Pode alimentar diagnóstico local exploratório: há sinal analítico suficiente para preparar validações e discussão com equipas locais."
    : "Há necessidade potencial, mas falta capacidade analítica: melhorar território, período, chave de join ou qualidade antes de usar no plano.";
  const list = document.createElement("div");
  list.className = "local-plan-list";
  bucket.rows.slice(0, 6).forEach((row) => {
    const item = document.createElement("button");
    item.type = "button";
    const strong = document.createElement("strong");
    strong.textContent = `${compactTitle(row.item.source_title, 36)} / ${compactTitle(row.item.target_title, 36)}`;
    const small = document.createElement("small");
    small.textContent = `PNS: ${row.pnsAxes.slice(0, 2).join(", ") || "por classificar"} · território ${formatDecimal(row.territorialReadiness, 0)} · linkability ${formatDecimal(row.linkabilityScore, 0)}`;
    item.append(strong, small);
    item.addEventListener("click", () => {
      state.selectedPublicHealthKey = row.key;
      setActiveTab("health");
    });
    list.appendChild(item);
  });
  localPriorityDetail.append(title, explanation, list);
}

function renderLocalSurveillance(rows) {
  clearElement(localSurveillance);
  const candidates = rows.slice().sort((a, b) => b.surveillanceReadiness - a.surveillanceReadiness || b.localScore - a.localScore).slice(0, 6);
  const meta = document.createElement("div");
  meta.className = "local-note";
  meta.textContent = state.dataPayload?.trends?.length
    ? "Existe amostra temporal ativa: estes sinais podem apoiar monitorização exploratória."
    : "A vigilância depende sobretudo de campos temporais nos metadados; carrega uma amostra real para validar tendências.";
  localSurveillance.appendChild(meta);
  candidates.forEach((row) => {
    const card = document.createElement("div");
    card.className = "local-signal-card";
    const strong = document.createElement("strong");
    strong.textContent = compactTitle(`${row.item.source_title} / ${row.item.target_title}`, 72);
    const small = document.createElement("small");
    small.textContent = `vigilância ${formatDecimal(row.surveillanceReadiness, 0)} · ${row.readiness === "regional" ? "com leitura territorial" : "sem território local"} · ${row.item.confidence}`;
    card.append(strong, small, createLocalMetric("Sinal temporal", row.surveillanceReadiness));
    localSurveillance.appendChild(card);
  });
}

function renderLocalRiskMatrix(rows) {
  clearElement(localRiskMatrix);
  const bands = [
    ["alto", "Impacto alto"],
    ["medio", "Impacto médio"],
    ["baixo", "Impacto baixo"],
  ];
  const likelihoods = [
    ["alta", "Probabilidade alta"],
    ["media", "Probabilidade média"],
    ["baixa", "Probabilidade baixa"],
  ];
  const buckets = new Map();
  rows.forEach((row) => {
    const model = row.item.public_health_model || {};
    const impact = model.impact?.level || "baixo";
    const likelihood = model.likelihood?.level || "baixa";
    const key = `${impact}|${likelihood}`;
    const bucket = buckets.get(key) || {impact, likelihood, rows: [], residual: 0};
    bucket.rows.push(row);
    bucket.residual += Math.max(0, row.riskRelevance - row.analysisCapacity * 0.45);
    buckets.set(key, bucket);
  });
  const corner = document.createElement("div");
  corner.className = "local-risk-axis";
  corner.textContent = "Impacto / probabilidade";
  localRiskMatrix.appendChild(corner);
  likelihoods.forEach(([, label]) => {
    const axis = document.createElement("div");
    axis.className = "local-risk-axis";
    axis.textContent = label;
    localRiskMatrix.appendChild(axis);
  });
  bands.forEach(([impact, impactLabel]) => {
    const axis = document.createElement("div");
    axis.className = "local-risk-axis";
    axis.textContent = impactLabel;
    localRiskMatrix.appendChild(axis);
    likelihoods.forEach(([likelihood]) => {
      const bucket = buckets.get(`${impact}|${likelihood}`);
      const cell = document.createElement("div");
      cell.className = `local-risk-cell impact-${impact} likelihood-${likelihood}`;
      const strong = document.createElement("strong");
      strong.textContent = formatNumber(bucket?.rows.length || 0);
      const small = document.createElement("small");
      small.textContent = bucket ? `risco residual ${formatDecimal(bucket.residual / bucket.rows.length, 0)}` : "sem sinal";
      cell.append(strong, small);
      localRiskMatrix.appendChild(cell);
    });
  });
}

function renderLocalActionPlan(rows) {
  clearElement(localActionPlan);
  const top = rows.slice(0, 5);
  const validations = [
    "Confirmar granularidade territorial: ULS, hospital, ARS, concelho/distrito ou região.",
    "Validar chaves de join: tipo, nulos, duplicados e cardinalidade.",
    "Confirmar período e comparabilidade temporal antes de usar tendências.",
    "Separar hipóteses nacionais de análises locais quando não há match territorial.",
  ];
  const questions = top.map((row) => questionForPriority(row));
  const columns = [
    ["Perguntas acionáveis", [...new Set(questions)].slice(0, 5)],
    ["Validações antes do plano", validations],
    ["Datasets prioritários", top.map((row) => compactTitle(row.item.source_title, 52))],
  ];
  columns.forEach(([titleText, items]) => {
    const section = document.createElement("div");
    const title = document.createElement("strong");
    title.textContent = titleText;
    const list = document.createElement("ul");
    items.forEach((text) => {
      const item = document.createElement("li");
      item.textContent = text;
      list.appendChild(item);
    });
    section.append(title, list);
    localActionPlan.appendChild(section);
  });
}

function renderLocalPlans() {
  if (!localPlanMeta) return;
  const rows = localPlanRows();
  const regional = rows.filter((row) => row.readiness === "regional").length;
  const national = rows.length - regional;
  localPlanMeta.textContent = `${formatNumber(rows.length)} hipóteses filtradas · ${formatNumber(regional)} com território · ${formatNumber(national)} insuficientes para mapa local.`;
  renderLocalPlanKpis(rows);
  renderLocalDiagnosis(rows);
  renderLocalPriorityMatrix(rows);
  renderLocalSurveillance(rows);
  renderLocalRiskMatrix(rows);
  renderLocalActionPlan(rows);
}

function renderPublicHealthMatrix() {
  clearElement(publicHealthMatrix);
  clearElement(publicHealthStrata);
  const correlations = filteredCorrelations();
  const priorityRows = publicHealthPriorityRows();
  const cells = summarizePublicHealthCells(correlations);
  publicHealthMeta.textContent = `${formatNumber(correlations.length)} hipóteses de cruzamento filtradas · ${formatNumber(priorityRows.filter((row) => row.decision === "Priorizar").length)} a validar primeiro · layer ativo: ${PUBLIC_HEALTH_LAYERS.find(([key]) => key === state.activePublicHealthLayer)?.[1] || "Impacto"}.`;
  renderPublicHealthCockpit(priorityRows);

  const likelihoodLevels = ["alta", "media", "baixa"];
  const impactLevels = ["baixo", "medio", "alto"];
  const maxCount = Math.max(...[...cells.values()].map((cell) => cell.count), 1);

  const corner = document.createElement("div");
  corner.className = "public-health-axis public-health-corner";
  corner.textContent = "Viabilidade × impacto";
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
      label.textContent = "hipóteses";
      const dominantArea = dominantEntry([...(cell?.areas || new Map()).entries()]);
      const area = document.createElement("small");
      area.textContent = dominantArea[1] ? `${dominantArea[0]} · ${formatNumber(dominantArea[1])}` : "sem hipóteses";
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
  titleLabel.textContent = `${likelihoodLabel(likelihood)} viabilidade × ${impactLabel(impact)} impacto`;
  const titleCount = document.createElement("span");
  titleCount.textContent = `${formatNumber(cell?.count || 0)} hipóteses`;
  title.append(titleLabel, titleCount);
  publicHealthStrata.appendChild(title);

  if (!cell || !cell.count) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem hipóteses nesta célula para os filtros atuais.";
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
  chartMeta.textContent = `${formatNumber(dimensions.length)} dimensões visíveis · raio por cobertura, eixo X por datasets, eixo Y por ligações.`;

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
  rows.slice(0, 10).forEach((row) => {
    const item = document.createElement("div");
    item.className = "analytics-matrix-row";
    const strength = Math.max(row.score_sum || row.count || row.link_count || 1, 1);
    item.style.setProperty("--matrix-alpha", String(Math.max(0.12, Math.min(0.92, strength / maxScore))));
    const left = document.createElement("span");
    const right = document.createElement("strong");
    const detail = formatter(row);
    left.textContent = compactTitle(detail.label, 54);
    left.title = detail.label;
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
    value: `${formatNumber(row.count)} ligações`,
  }));
}

function renderCorrelationTable() {
  const allRows = filteredCorrelations();
  const rows = allRows.slice(0, 80);
  state.filteredCorrelations = allRows;
  correlationMeta.textContent = `${formatNumber(rows.length)}/${formatNumber(allRows.length)} ligações na tabela · exporta o filtro completo.`;
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
  if (state.activeTab === "data") {
    renderDataAnalytics();
    return;
  }
  if (state.activeTab === "finprod") {
    renderFinProdAnalytics();
    if (!state.finProdPayload && state.selectedFinancialDataset && state.selectedProductionDataset) {
      loadFinProdAnalytics().catch(showError);
    }
    return;
  }
  if (state.activeTab === "predictive") {
    renderPredictiveAnalytics();
    return;
  }
  if (state.activeTab === "semantic") {
    renderSummary();
    renderTopOpportunities();
    renderInsightCards();
    renderDistribution();
    renderSemanticModel();
    renderBubbleChart();
    renderDimensionList();
    return;
  }
  if (state.activeTab === "health") {
    renderSummary();
    renderPublicHealthMatrix();
    return;
  }
  if (state.activeTab === "local") {
    renderSummary();
    renderLocalPlans();
    return;
  }
  if (state.activeTab === "tables") {
    renderSummary();
    renderMatrices();
    renderCorrelationTable();
  }
}

function setupEvents() {
  analyticsMinScore.value = state.minScore;
  analyticsMinScoreValue.textContent = state.minScore;
  dataSampleLimit.value = state.dataLimit;
  dataSampleLimitValue.textContent = state.dataLimit;
  const initialTab = new URLSearchParams(window.location.search).get("tab");
  if (["data", "finprod", "predictive", "semantic", "health", "local", "tables"].includes(initialTab)) {
    state.activeTab = initialTab;
  }
  renderDatasetMode();

  analyticsTabs.forEach((button) => {
    button.addEventListener("click", () => setActiveTab(button.dataset.analyticsTab));
  });
  predictiveDataButton?.addEventListener("click", () => setActiveTab("data"));
  predictiveIndicatorFilter?.addEventListener("change", () => {
    state.selectedPredictiveIndicator = predictiveIndicatorFilter.value;
    renderPredictiveAnalytics();
  });
  finProdRefreshButton?.addEventListener("click", () => {
    state.selectedFinancialDataset = finProdFinancialDataset.value;
    state.selectedProductionDataset = finProdProductionDataset.value;
    loadFinProdAnalytics().catch(showError);
  });
  finProdFinancialDataset?.addEventListener("change", () => {
    state.selectedFinancialDataset = finProdFinancialDataset.value;
    state.finProdPayload = null;
    renderFinProdAnalytics();
  });
  finProdProductionDataset?.addEventListener("change", () => {
    state.selectedProductionDataset = finProdProductionDataset.value;
    state.finProdPayload = null;
    renderFinProdAnalytics();
  });
  setActiveTab(state.activeTab, {syncUrl: false, force: true});

  analyticsSearch.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = analyticsSearch.value;
      invalidateFilterCache();
      renderAll();
    }, 120);
  });

  analyticsKind.addEventListener("change", () => {
    state.kind = analyticsKind.value;
    invalidateFilterCache();
    renderAll();
  });

  analyticsConfidence.addEventListener("change", () => {
    state.confidence = analyticsConfidence.value;
    invalidateFilterCache();
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
    state.datasetMode = "manual";
    renderDatasetMode();
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

  dataRichMode?.addEventListener("click", () => {
    state.datasetMode = "rich";
    state.dataPayload = null;
    populateDataDatasetOptions();
    renderDataAnalytics();
    loadDataAnalytics().catch(showDataError);
  });

  dataManualMode?.addEventListener("click", () => {
    state.datasetMode = "manual";
    renderDatasetMode();
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
