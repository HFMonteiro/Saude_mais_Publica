const state = {
  payload: null,
  filteredCorrelations: [],
  minScore: 4,
  search: "",
  kind: "",
  localScope: "",
  confidence: "",
  lowRiskOnly: false,
  selectedCorrelationKey: "",
  selectedDataDataset: "",
  selectedFeatureKey: "",
  selectedPublicHealthKey: "",
  selectedPublicHealthMatrixCell: "",
  selectedLocalPriorityCell: "",
  selectedCareOpsKey: "",
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
  finProdRecommendationPayload: null,
  predictiveRecommendationPayload: null,
  _predictiveRecommendationKey: "",
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
const analyticsLocalScope = document.getElementById("analyticsLocalScope");
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
const dataStoryStrip = document.getElementById("dataStoryStrip");
const dataStatRows = document.getElementById("dataStatRows");
const dataStatCompletenessValue = document.getElementById("dataStatCompletenessValue");
const dataStatCompletenessBar = document.getElementById("dataStatCompletenessBar");
const dataStatCounts = document.getElementById("dataStatCounts");
const dataStatSuitability = document.getElementById("dataStatSuitability");
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
const finProdStoryStrip = document.getElementById("finProdStoryStrip");
const finProdFinancialDataset = document.getElementById("finProdFinancialDataset");
const finProdProductionDataset = document.getElementById("finProdProductionDataset");
const finProdRefreshButton = document.getElementById("finProdRefreshButton");
const finProdRecommendations = document.getElementById("finProdRecommendations");
const finProdKpis = document.getElementById("finProdKpis");
const finProdTable = document.getElementById("finProdTable");
const finProdChecks = document.getElementById("finProdChecks");
const finProdTrendChart = document.getElementById("finProdTrendChart");
const finProdOutliers = document.getElementById("finProdOutliers");
const finProdBenchmark = document.getElementById("finProdBenchmark");
const finProdDiagnostics = document.getElementById("finProdDiagnostics");
const predictiveMeta = document.getElementById("predictiveMeta");
const predictiveStoryStrip = document.getElementById("predictiveStoryStrip");
const predictiveDataButton = document.getElementById("predictiveDataButton");
const predictiveKpis = document.getElementById("predictiveKpis");
const predictiveIndicatorFilter = document.getElementById("predictiveIndicatorFilter");
const predictiveForecastChart = document.getElementById("predictiveForecastChart");
const predictiveFitGuide = document.getElementById("predictiveFitGuide");
const predictiveClosestOpportunities = document.getElementById("predictiveClosestOpportunities");
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
const publicHealthStoryStrip = document.getElementById("publicHealthStoryStrip");
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
const localPlanStoryStrip = document.getElementById("localPlanStoryStrip");
const localPlanKpis = document.getElementById("localPlanKpis");
const localDiagnosis = document.getElementById("localDiagnosis");
const localPriorityMatrix = document.getElementById("localPriorityMatrix");
const localPriorityDetail = document.getElementById("localPriorityDetail");
const localSurveillance = document.getElementById("localSurveillance");
const localRiskMatrix = document.getElementById("localRiskMatrix");
const localActionPlan = document.getElementById("localActionPlan");
const careOpsMeta = document.getElementById("careOpsMeta");
const careOpsStoryStrip = document.getElementById("careOpsStoryStrip");
const careOpsKpis = document.getElementById("careOpsKpis");
const careOpsMatrix = document.getElementById("careOpsMatrix");
const careOpsTable = document.getElementById("careOpsTable");
const careOpsWorkflow = document.getElementById("careOpsWorkflow");
const careOpsGuardrails = document.getElementById("careOpsGuardrails");
const bubbleChart = document.getElementById("analyticsBubbleChart");
const dimensionList = document.getElementById("analyticsDimensionList");
const themeMatrix = document.getElementById("analyticsThemeMatrix");
const dimensionMatrix = document.getElementById("analyticsDimensionMatrix");
const correlationMeta = document.getElementById("analyticsCorrelationMeta");
const correlationTable = document.getElementById("analyticsCorrelationTable");
const SVG_NS = "http://www.w3.org/2000/svg";
const PUBLIC_HEALTH_LAYERS = [
  ["impact", "Impacto"],
  ["likelihood", "Viabilidade"],
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
    match: /(regiao centro|região centro|ars centro|coimbra|leiria|viseu|guarda|castelo branco|aveiro|cova da beira|baixo mondego|dao lafoes|dão lafões|beira interior|baixo vouga)/,
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
  label: "Território por resolver",
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
let activeFinProdRequest = 0;
let activeFinProdRecommendationRequest = 0;
let activePredictiveRecommendationRequest = 0;

function primaryPanelForTab(tab) {
  return analyticsPanels.find((panel) => (panel.dataset.tabPanel || "").split(/\s+/).includes(tab)) || null;
}

function setupTabAccessibility() {
  const tabByName = new Map();
  analyticsTabs.forEach((button) => {
    const tab = button.dataset.analyticsTab;
    if (!tab) return;
    tabByName.set(tab, button);
    button.id = button.id || `analytics-tab-${tab}`;
    button.setAttribute("role", "tab");
    const primaryPanel = primaryPanelForTab(tab);
    if (primaryPanel) {
      primaryPanel.id = primaryPanel.id || `analytics-panel-${tab}`;
      button.setAttribute("aria-controls", primaryPanel.id);
    }
  });
  analyticsPanels.forEach((panel, index) => {
    const tabs = (panel.dataset.tabPanel || "").split(/\s+/).filter(Boolean);
    panel.id = panel.id || `analytics-panel-${tabs[0] || index}`;
    panel.setAttribute("role", "tabpanel");
    if (tabs.length === 1 && tabByName.has(tabs[0])) {
      panel.setAttribute("aria-labelledby", tabByName.get(tabs[0]).id);
    }
  });
}

function clearElement(element) {
  element.replaceChildren();
}

function renderAnalyticsStory(container, items = []) {
  if (!container) return;
  clearElement(container);
  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = `analytics-story-card ${item.tone ? `is-${item.tone}` : ""}`.trim();
    const label = document.createElement("span");
    label.textContent = item.label;
    const value = document.createElement("strong");
    value.textContent = item.value;
    value.title = item.value;
    const detail = document.createElement("small");
    detail.textContent = item.detail;
    card.append(label, value, detail);
    container.appendChild(card);
  });
}

function firstWarning(warnings = [], fallback = "Validar fonte, denominador, unidade e período antes de concluir.") {
  return warnings.length ? shortSentence(String(warnings[0]).replace(/[.。]+$/g, ""), 96) : fallback;
}

function renderEmptyPanel(container, message) {
  clearElement(container);
  const wrap = document.createElement("div");
  wrap.className = "empty-panel-state";
  const img = document.createElement("img");
  img.src = "visual_assets/empty_state.png";
  img.className = "empty-panel-img";
  img.alt = "Vazio";
  const text = document.createElement("span");
  text.className = "meta";
  text.textContent = message;
  wrap.append(img, text);
  container.appendChild(wrap);
}

function repairEncoding(value) {
  return String(value)
    .replace(/Ã¡/g, "á").replace(/Ã /g, "à").replace(/Ã¢/g, "â").replace(/Ã£/g, "ã")
    .replace(/Ã©/g, "é").replace(/Ãª/g, "ê").replace(/Ã­/g, "í")
    .replace(/Ã³/g, "ó").replace(/Ã´/g, "ô").replace(/Ãõ/g, "õ")
    .replace(/Ãº/g, "ú").replace(/Ã§/g, "ç")
    .replace(/Ã /g, "Á").replace(/Ã‰/g, "É").replace(/Ã /g, "Í")
    .replace(/Ã“/g, "Ó").replace(/Ãš/g, "Ú").replace(/Ã‡/g, "Ç")
    .replace(/Ã—/g, "×").replace(/Â·/g, "·").replace(/Â /g, " ")
    .replace(/â€¦/g, "...").replace(/â€”/g, "-").replace(/â€“/g, "-")
    .replace(/â†’/g, "→").replace(/â† /g, "←");
}

function safeText(value) {
  return value == null ? "" : repairEncoding(value);
}

function normalizeSearch(value) {
  return safeText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

const SEARCH_SYNONYM_GROUPS = [
  ["icpc", "problema", "problemas", "diagnostico", "diagnosticos", "morbilidade"],
  ["utente", "utentes", "inscrito", "inscritos", "populacao", "populacao inscrita"],
  ["uf", "unidade funcional", "usf", "ucsp", "aces", "csp", "cuidados primarios"],
  ["uls", "unidade local de saude"],
  ["regiao", "ars", "norte", "centro", "lisboa", "alentejo", "algarve"],
  ["hospital", "hospitais", "centro hospitalar", "internamento", "urgencia"],
  ["fragilidade", "multimorbilidade", "polimedicacao", "idra", "gra", "pic", "plano individual"],
  ["rastreio", "rastreios", "vacina", "vacinacao", "prevencao"],
];

function searchAlternatives(token) {
  const normalized = normalizeSearch(token).trim();
  if (!normalized) return [];
  const group = SEARCH_SYNONYM_GROUPS.find((items) => items.some((item) => normalizeSearch(item) === normalized));
  return group ? group.map(normalizeSearch) : [normalized];
}

function matchesExpandedSearch(text, rawSearch) {
  const tokens = normalizeSearch(rawSearch).split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;
  const normalizedText = normalizeSearch(text);
  return tokens.every((token) => searchAlternatives(token).some((candidate) => normalizedText.includes(candidate)));
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

function shortSentence(value, max = 92) {
  const text = safeText(value).replace(/\s+/g, " ").trim();
  if (text.length <= max) return text;
  const cut = text.slice(0, max - 1).replace(/\s+\S*$/, "");
  return `${cut}...`;
}

function readinessInfo(payload) {
  return payload?.analysis_readiness || {score: 0, band: "fragil", label: "Frágil", gaps: ["validar fonte"]};
}

function readinessDetail(info) {
  const gaps = (info.gaps || ["validar fonte"]).slice(0, 3).join(", ");
  return `Validar: ${gaps}`;
}

function kindLabel(kind) {
  return {
    temporal: "Temporal",
    territorial: "Território",
    entidade: "Entidade",
    medida: "Medida",
    clinico: "Clínico",
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

function viabilityLabel(value) {
  return `${likelihoodLabel(value)} viabilidade`;
}

function impactLabel(value) {
  return {
    alto: "Alto",
    medio: "Médio",
    baixo: "Baixo",
  }[value] || value;
}

function measureRoleLabel(value) {
  return {
    taxa: "taxa",
    monetario: "valor monetário",
    contagem: "contagem",
    stock: "stock",
  }[value] || "medida";
}

function formatDecimal(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
  return new Intl.NumberFormat("pt-PT", {maximumFractionDigits: digits}).format(Number(value));
}

function formatSmartNumber(value, options = {}) {
  const number = Number(value);
  if (value === null || value === undefined || Number.isNaN(number)) return "-";
  const abs = Math.abs(number);
  const digits = options.digits ?? (abs > 0 && abs < 0.01 ? 5 : abs < 1 ? 3 : abs < 100 ? 2 : 1);
  return new Intl.NumberFormat("pt-PT", {
    maximumFractionDigits: digits,
    minimumFractionDigits: options.minDigits ?? 0,
  }).format(number);
}

function finProdViability(summary = {}, payload = null) {
  if (summary.blocked) {
    const reason = summary.blocked_reason || (payload?.unit_warnings || [])[0] || "sem validação mínima";
    return {label: "Bloqueado", detail: reason, band: "none"};
  }
  const matched = Number(summary.matched_periods || 0);
  const pairs = Number(summary.sample_pairs || 0);
  if (matched >= 6 && pairs >= 6) return {label: "Usável", detail: "base temporal mínima para triagem", band: "good"};
  if (matched >= 3) return {label: "Frágil", detail: "usar só como sinal exploratório", band: "partial"};
  if (matched > 0) return {label: "Insuficiente", detail: "poucos períodos comuns", band: "weak"};
  return {label: "Bloqueado", detail: "sem base temporal comum", band: "none"};
}

function datasetFinProdRole(dataset = {}) {
  return dataset.finprod_role || dataset.semantic_profile?.finprod_role || "sem_medida";
}

function datasetFinProdBadge(dataset = {}) {
  return {
    monetario: "€",
    volume: "vol.",
    taxa: "%",
    sem_medida: "sem medida",
  }[datasetFinProdRole(dataset)] || "sem medida";
}

function finProdRoleRank(dataset = {}, mode = "financial") {
  const role = datasetFinProdRole(dataset);
  const ranks = mode === "financial"
    ? {monetario: 0, volume: 1, taxa: 2, sem_medida: 3}
    : {volume: 0, taxa: 1, monetario: 2, sem_medida: 3};
  return ranks[role] ?? 4;
}

function finProdMatchBand(row = {}) {
  const matched = Number(row.matched_periods ?? row.summary?.matched_periods ?? 0);
  const samplePairs = Number(row.sample_pairs ?? row.summary?.sample_pairs ?? matched);
  if (matched >= 6 && samplePairs >= 6) return "good";
  if (matched >= 3) return "partial";
  if (matched > 0) return "weak";
  return "none";
}

function finProdMatchLabel(row = {}) {
  const matched = Number(row.matched_periods ?? row.summary?.matched_periods ?? 0);
  const labels = {
    good: `${formatNumber(matched)}p bom`,
    partial: `${formatNumber(matched)}p parcial`,
    weak: `${formatNumber(matched)}p fraco`,
    none: "0p sem match",
  };
  return labels[finProdMatchBand(row)];
}

function finProdMatchTitle(band) {
  return {
    good: "Bom match temporal",
    partial: "Match parcial",
    weak: "Match fraco",
    none: "Sem períodos comuns",
  }[band] || "Por avaliar";
}

function finProdCandidateRows() {
  const payload = state.finProdRecommendationPayload || {};
  const rows = payload.candidates || payload.recommendations || [];
  const byId = new Map();
  rows.forEach((row) => {
    if (row.production_dataset_id) byId.set(row.production_dataset_id, row);
  });
  if (state.finProdPayload?.production_dataset?.dataset_id) {
    byId.set(state.finProdPayload.production_dataset.dataset_id, {
      ...(byId.get(state.finProdPayload.production_dataset.dataset_id) || {}),
      production_dataset_id: state.finProdPayload.production_dataset.dataset_id,
      production_title: state.finProdPayload.production_dataset.title,
      matched_periods: state.finProdPayload.summary?.matched_periods || 0,
      sample_pairs: state.finProdPayload.summary?.sample_pairs || 0,
      robustness: state.finProdPayload.summary?.robustness || "insuficiente",
      is_current: true,
    });
  }
  return byId;
}

function finProdOptionPalette(band) {
  return {
    good: {bg: "#edf7f3", color: "#075f4f"},
    partial: {bg: "#fff7e8", color: "#875214"},
    weak: {bg: "#fff2ea", color: "#9b3b24"},
    none: {bg: "#fff1f1", color: "#9b2424"},
  }[band] || {bg: "", color: ""};
}

function updateFinProdMatchDecorations() {
  if (!finProdProductionDataset) return;
  const candidates = finProdCandidateRows();
  const datasets = state.payload?.datasets || [];
  const titleById = new Map(datasets.map((dataset) => [dataset.dataset_id, dataset.title || dataset.dataset_id]));
  finProdProductionDataset.classList.remove("match-good", "match-partial", "match-weak", "match-none", "match-unknown");
  let selectedBand = "unknown";
  Array.from(finProdProductionDataset.options || []).forEach((option) => {
    const row = candidates.get(option.value);
    const title = titleById.get(option.value) || option.textContent.replace(/^\[[^\]]+\]\s*/, "");
    const band = row ? finProdMatchBand(row) : "unknown";
    if (option.value === state.selectedProductionDataset) selectedBand = band;
    option.textContent = row ? `[${finProdMatchLabel(row)}] ${compactTitle(title, 70)}` : `[por avaliar] ${compactTitle(title, 70)}`;
    option.title = title;
    const palette = finProdOptionPalette(band);
    option.style.backgroundColor = palette.bg;
    option.style.color = palette.color;
  });
  finProdProductionDataset.classList.add(`match-${selectedBand}`);
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
    state.localScope,
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
    .sort((a, b) => finProdRoleRank(a, "financial") - finProdRoleRank(b, "financial")
      || (b.metric_candidate_count || 0) - (a.metric_candidate_count || 0)
      || safeText(a.title).localeCompare(safeText(b.title)));
  const production = datasets
    .filter((dataset) => dataset.mega_theme === "Acesso & Produção")
    .sort((a, b) => finProdRoleRank(a, "production") - finProdRoleRank(b, "production")
      || (b.metric_candidate_count || 0) - (a.metric_candidate_count || 0)
      || safeText(a.title).localeCompare(safeText(b.title)));
  clearElement(finProdFinancialDataset);
  clearElement(finProdProductionDataset);
  financial.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = `[${datasetFinProdBadge(dataset)}] ${compactTitle(dataset.title || dataset.dataset_id, 68)}`;
    option.title = `${dataset.title || dataset.dataset_id} · ${datasetFinProdBadge(dataset)}`;
    finProdFinancialDataset.appendChild(option);
  });
  production.forEach((dataset) => {
    const option = document.createElement("option");
    option.value = dataset.dataset_id;
    option.textContent = `[${datasetFinProdBadge(dataset)}] ${compactTitle(dataset.title || dataset.dataset_id, 68)}`;
    option.title = `${dataset.title || dataset.dataset_id} · ${datasetFinProdBadge(dataset)}`;
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
  updateFinProdMatchDecorations();
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
  const requestId = ++activeFinProdRequest;
  finProdStatus.textContent = "A cruzar datasets...";
  const payload = await fetchJson(
    `/api/finprod?financial_dataset=${encodeURIComponent(state.selectedFinancialDataset)}&production_dataset=${encodeURIComponent(state.selectedProductionDataset)}&limit=${state.dataLimit}`,
    {timeoutMs: 32000},
  );
  if (requestId !== activeFinProdRequest) return;
  state.finProdPayload = payload;
  finProdStatus.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  renderFinProdAnalytics();
  loadFinProdRecommendations(payload).catch(() => {
    renderFinProdRecommendations({recommendations: [], warning: "Não foi possível calcular alternativas agora."});
  });
}

async function loadFinProdRecommendations(payload = null) {
  if (!finProdRecommendations || !state.selectedFinancialDataset) return;
  const requestId = ++activeFinProdRecommendationRequest;
  renderFinProdRecommendations({loading: true});
  const activeProduction = state.selectedProductionDataset || "";
  const url = `/api/finprod/recommendations?financial_dataset=${encodeURIComponent(state.selectedFinancialDataset)}&production_dataset=${encodeURIComponent(activeProduction)}&limit=${state.dataLimit}&candidate_limit=12`;
  const recommendations = await fetchJson(url, {timeoutMs: 45000});
  if (requestId !== activeFinProdRecommendationRequest) return;
  state.finProdRecommendationPayload = recommendations;
  renderFinProdRecommendations(recommendations, payload);
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

function renderFinProdInterpretation(payload) {
  if (!finProdChecks) return;
  const summary = payload.summary || {};
  const viability = finProdViability(summary, payload);
  const warnings = [...(payload.unit_warnings || []), ...(payload.diagnostics?.warnings || [])];
  const note = document.createElement("div");
  note.className = `finprod-interpretation is-${viability.band}`;
  const title = document.createElement("strong");
  title.textContent = `Leitura: ${viability.label}`;
  const detail = document.createElement("small");
  detail.textContent = warnings.length
    ? shortSentence(String(warnings[0]).replace(/[.。]+$/g, ""), 92)
    : "Confirmar unidade, âmbito e denominador.";
  note.append(title, detail);
  finProdChecks.prepend(note);
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
    renderAnalyticsStory(finProdStoryStrip, [
      {
        label: "Pergunta ativa",
        value: "Despesa acompanha produção?",
        detail: "Seleciona um numerador financeiro e um denominador de produção para testar períodos comuns.",
      },
      {
        label: "Leitura recomendada",
        value: "Comparar só com unidade validada",
        detail: "Custo unitário só é útil se houver numerador monetário, produção válida e períodos comuns.",
        tone: "warning",
      },
      {
        label: "Próxima ação",
        value: "Cruzar datasets",
        detail: "Depois lê tendência, outliers e benchmark territorial/entidade.",
      },
    ]);
    finProdMeta.textContent = "Seleciona os dois datasets e clica em Cruzar datasets.";
    finProdDiagnostics.textContent = "Sem cruzamento ativo.";
    renderFinProdRecommendations(state.finProdRecommendationPayload || {recommendations: []});
    return;
  }
  renderFinProdChecks(payload.comparability?.checks || []);
  renderFinProdInterpretation(payload);
  const aggregation = payload.aggregation || {};
  const numerator = payload.numerator || {};
  const denominator = payload.denominator || {};
  finProdMeta.textContent = `${compactTitle(payload.financial_dataset?.title || payload.financial_dataset?.dataset_id, 44)} × ${compactTitle(payload.production_dataset?.title || payload.production_dataset?.dataset_id, 44)} · ${aggregation.period || "período"}`;
  const summary = payload.summary || {};
  const viability = finProdViability(summary, payload);
  const warningsForStory = [...(payload.unit_warnings || []), ...(payload.diagnostics?.warnings || [])];
  renderAnalyticsStory(finProdStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "Qual é o custo por unidade produzida?",
      detail: `${formatNumber(summary.matched_periods || 0)} períodos comuns; numerador ${numerator.role || "desconhecido"} e denominador ${denominator.role || "desconhecido"}.`,
    },
    {
      label: "Leitura recomendada",
      value: summary.avg_unit_cost == null ? "Sem custo validável" : formatSmartNumber(summary.avg_unit_cost),
      detail: summary.avg_unit_cost == null ? "Não calcular rácio sem denominador e unidade validados." : `Média exploratória; mediana ${formatSmartNumber(summary.median_unit_cost)}.`,
      tone: viability.band === "good" ? "ok" : "warning",
    },
    {
      label: "Travão",
      value: viability.label,
      detail: firstWarning(warningsForStory, viability.detail),
      tone: summary.blocked ? "danger" : (warningsForStory.length ? "warning" : "ok"),
    },
    {
      label: "Próxima ação",
      value: "Ver outliers",
      detail: "Confirmar períodos anómalos e benchmark antes de usar em decisão.",
    },
  ]);
  addFinProdKpi("Períodos em comum", formatNumber(summary.matched_periods || 0), "base temporal comparável");
  addFinProdKpi("Viabilidade", viability.label, viability.detail);
  addFinProdKpi("Custo unitário médio", formatSmartNumber(summary.avg_unit_cost), payload.aggregation?.numerator || "soma financeira / soma produção");
  addFinProdKpi("Mediana custo unitário", formatSmartNumber(summary.median_unit_cost), "só com numerador monetário validado");
  addFinProdKpi(
    "Correlação despesa-produção",
    summary.expense_output_correlation == null ? "-" : formatDecimal(summary.expense_output_correlation, 2),
    summary.correlation_strength || "insuficiente",
  );
  addFinProdKpi("Spearman", summary.spearman_correlation == null ? "-" : formatDecimal(summary.spearman_correlation, 2), `robustez ${summary.robustness || "insuficiente"}`);

  const header = document.createElement("tr");
  const financialUnit = payload.normalization?.financial?.unit_label || "valor";
  const productionUnit = payload.normalization?.production?.unit_label || "valor";
  ["Período", `Financeiro (${financialUnit})`, `Produção (${productionUnit})`, "Custo unitário", "Leitura"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    header.appendChild(th);
  });
  thead?.appendChild(header);
  (payload.rows || []).forEach((row) => {
    const tr = document.createElement("tr");
    const values = [
      row.period,
      `${formatSmartNumber(row.financial_normalized)} norm. · ${formatSmartNumber(row.financial_value)} real`,
      `${formatSmartNumber(row.production_normalized)} norm. · ${formatSmartNumber(row.production_value)} real`,
      formatSmartNumber(row.unit_cost),
      Number(row.production_value || 0) > 0 ? "produção válida" : "sem denominador válido",
    ];
    values.forEach((value, index) => {
      const td = document.createElement("td");
      if (index > 0 && index < 4) td.className = "numeric-cell";
      td.textContent = value;
      tr.appendChild(td);
    });
    tbody?.appendChild(tr);
  });
  renderFinProdTrend(payload);
  renderFinProdOutliers(payload.outliers || []);
  renderFinProdBenchmark(payload.entity_benchmark || []);
  const norm = payload.normalization || {};
  const warnings = [...(payload.unit_warnings || []), ...(payload.diagnostics?.warnings || [])];
  const warningText = warnings.length ? ` · Validar: ${shortSentence(String(warnings[0]).replace(/[.。]+$/g, ""), 70)}` : " · sem avisos críticos";
  finProdDiagnostics.textContent = `Numerador: ${compactTitle(numerator.label || numerator.field || "-", 36)} · Denominador: ${compactTitle(denominator.label || denominator.field || "-", 36)} · Escalas ${norm.financial?.unit_label || "-"} / ${norm.production?.unit_label || "-"}${warningText}.`;
}

function renderFinProdRecommendations(payload = {}, currentPayload = null) {
  if (!finProdRecommendations) return;
  clearElement(finProdRecommendations);
  if (payload.loading) {
    const note = document.createElement("div");
    note.className = "finprod-recommendation-note";
    note.textContent = "A procurar alternativas...";
    finProdRecommendations.appendChild(note);
    return;
  }
  if (payload && !payload.loading) {
    state.finProdRecommendationPayload = payload;
  }
  updateFinProdMatchDecorations();
  const candidateRows = payload.candidates || payload.recommendations || [];
  const candidateById = new Map(candidateRows.map((row) => [row.production_dataset_id, row]));
  const activeRow = currentPayload?.summary
    ? {
        production_dataset_id: state.selectedProductionDataset,
        production_title: currentPayload.production_dataset?.title,
        matched_periods: currentPayload.summary?.matched_periods || 0,
        sample_pairs: currentPayload.summary?.sample_pairs || 0,
        robustness: currentPayload.summary?.robustness || "insuficiente",
      }
    : candidateById.get(state.selectedProductionDataset);
  if (activeRow) {
    const band = finProdMatchBand(activeRow);
    const status = document.createElement("div");
    status.className = `finprod-match-status is-${band}`;
    const strong = document.createElement("strong");
    strong.textContent = `${finProdMatchTitle(band)} · ${finProdMatchLabel(activeRow)}`;
    const small = document.createElement("small");
    small.textContent = band === "none"
      ? "Sem períodos comuns. Escolhe verde/âmbar."
      : `${formatNumber(activeRow.sample_pairs || activeRow.matched_periods || 0)} pares válidos · robustez ${activeRow.robustness || "insuficiente"}.`;
    status.append(strong, small);
    finProdRecommendations.appendChild(status);
  }

  const legend = document.createElement("div");
  legend.className = "finprod-match-legend";
  [
    ["good", "verde >=6 períodos"],
    ["partial", "âmbar 3-5"],
    ["weak", "laranja 1-2"],
    ["none", "vermelho 0"],
  ].forEach(([band, label]) => {
    const item = document.createElement("span");
    item.className = `is-${band}`;
    item.textContent = label;
    legend.appendChild(item);
  });
  finProdRecommendations.appendChild(legend);

  const rows = payload.recommendations || candidateRows.filter((row) => Number(row.matched_periods || 0) > 0);
  const currentMatched = currentPayload?.summary?.matched_periods ?? state.finProdPayload?.summary?.matched_periods ?? null;
  const usefulRows = rows.filter((row) => {
    const matched = Number(row.matched_periods || 0);
    if (matched <= 0) return false;
    if (currentMatched == null) return true;
    return matched >= currentMatched || row.production_dataset_id === state.selectedProductionDataset;
  });
  if (!rows.length || !usefulRows.length) {
    const note = document.createElement("div");
    note.className = "finprod-recommendation-note is-warning";
    note.textContent = payload.warning
      || (rows.length ? "Não foram encontradas alternativas com períodos comuns nos candidatos testados." : "Sem alternativas temporais avaliáveis.");
    finProdRecommendations.appendChild(note);
    if (payload.warning) {
      return;
    }
    return;
  }
  const title = document.createElement("div");
  title.className = "finprod-recommendation-title";
  const strong = document.createElement("strong");
  strong.textContent = currentMatched != null && currentMatched < 6
    ? "Alternativas temporais melhores"
    : "Pares temporais candidatos";
  const small = document.createElement("small");
  small.textContent = "Ordenadas por períodos comuns.";
  title.append(strong, small);
  finProdRecommendations.appendChild(title);

  const list = document.createElement("div");
  list.className = "finprod-recommendation-list";
  usefulRows.slice(0, 4).forEach((row) => {
    const band = finProdMatchBand(row);
    const button = document.createElement("button");
    button.type = "button";
    button.className = `finprod-recommendation-card is-${band} ${row.production_dataset_id === state.selectedProductionDataset ? "is-current" : ""}`;
    const name = document.createElement("strong");
    name.textContent = compactTitle(row.production_title || row.production_dataset_id, 68);
    name.title = row.production_title || row.production_dataset_id;
    const meta = document.createElement("small");
    const range = row.financial_range?.start && row.production_range?.start
      ? `fin. ${row.financial_range.start}-${row.financial_range.end}; prod. ${row.production_range.start}-${row.production_range.end}`
      : "";
    meta.textContent = `${formatNumber(row.matched_periods || 0)} períodos · ${formatNumber(row.sample_pairs || 0)} pares · ${row.robustness || "insuficiente"}`;
    meta.title = range;
    const badge = document.createElement("span");
    badge.textContent = row.production_dataset_id === state.selectedProductionDataset ? "Atual" : finProdMatchLabel(row);
    button.append(name, meta, badge);
    button.addEventListener("click", () => {
      if (row.production_dataset_id === state.selectedProductionDataset) return;
      state.selectedProductionDataset = row.production_dataset_id;
      finProdProductionDataset.value = row.production_dataset_id;
      state.finProdPayload = null;
      loadFinProdAnalytics().catch(showError);
    });
    list.appendChild(button);
  });
  finProdRecommendations.appendChild(list);
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
  const height = 286;
  finProdTrendChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  finProdTrendChart.setAttribute("aria-label", "Tendência do custo unitário com escala, períodos e valores");
  if (rows.length < 2) {
    if (rows.length === 1 && Number.isFinite(Number(rows[0].unit_cost))) {
      const x = width / 2;
      const y = height / 2 - 12;
      const axis = svgNode("g", {class: "finprod-chart-axis"});
      axis.appendChild(svgNode("line", {x1: 74, y1: y + 58, x2: width - 34, y2: y + 58, class: "finprod-axis-line"}));
      const title = svgNode("text", {x: 74, y: 24, class: "finprod-axis-title"});
      title.textContent = "1 período comum · não há tendência";
      axis.appendChild(title);
      finProdTrendChart.appendChild(axis);
      const group = svgNode("g", {class: "data-trend-point single-point", transform: `translate(${x}, ${y})`});
      group.appendChild(svgNode("circle", {r: 5.5}));
      const tooltip = svgNode("title");
      tooltip.textContent = `${rows[0].period}: custo ${formatSmartNumber(rows[0].unit_cost)} · fin. ${formatSmartNumber(rows[0].financial_value)} · prod. ${formatSmartNumber(rows[0].production_value)}`;
      group.appendChild(tooltip);
      finProdTrendChart.appendChild(group);
      const value = svgNode("text", {x, y: y - 16, "text-anchor": "middle", class: "finprod-point-label"});
      value.textContent = formatSmartNumber(rows[0].unit_cost);
      finProdTrendChart.appendChild(value);
      const period = svgNode("text", {x, y: y + 78, "text-anchor": "middle", class: "finprod-period-label"});
      period.textContent = rows[0].period;
      finProdTrendChart.appendChild(period);
      const note = svgNode("text", {x: 74, y: height - 18, class: "finprod-chart-note"});
      note.textContent = "É um ponto observado, não uma linha de evolução.";
      finProdTrendChart.appendChild(note);
      return;
    }
    const diagnostics = payload.diagnostics || {};
    const bestCandidate = (diagnostics.trend_candidates || [])[0];
    const rangeText = bestCandidate?.financial_range?.start && bestCandidate?.production_range?.start
      ? `Fin. ${bestCandidate.financial_range.start}–${bestCandidate.financial_range.end}; prod. ${bestCandidate.production_range.start}–${bestCandidate.production_range.end}.`
      : "";
    const lines = [
      rows.length ? "Só existe 1 período comum." : "Sem períodos comuns suficientes.",
      bestCandidate
        ? `Melhor par: ${bestCandidate.shared_periods} período(s) comum(ns).`
        : `Tendências detetadas: financeiro ${diagnostics.financial_trends || 0}, produção ${diagnostics.production_trends || 0}.`,
      rangeText || "Escolhe datasets com eixo temporal compatível para desenhar tendência.",
    ].filter(Boolean);
    lines.forEach((line, index) => {
      const empty = svgNode("text", {
        x: width / 2,
        y: (height / 2) - 18 + (index * 18),
        "text-anchor": "middle",
        fill: index === 0 ? "#102033" : "#657489",
      });
      empty.textContent = line;
      finProdTrendChart.appendChild(empty);
    });
    return;
  }
  const validRows = rows
    .map((row) => ({...row, unitCostNumber: Number(row.unit_cost)}))
    .filter((row) => Number.isFinite(row.unitCostNumber));
  const values = validRows.map((row) => row.unitCostNumber);
  if (values.length < 2) {
    const empty = svgNode("text", {
      x: width / 2,
      y: height / 2,
      "text-anchor": "middle",
      fill: "#657489",
    });
    empty.textContent = "Sem valores de custo unitário válidos para desenhar tendência.";
    finProdTrendChart.appendChild(empty);
    return;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const left = 74;
  const right = width - 34;
  const top = 34;
  const bottom = height - 58;
  const step = (right - left) / Math.max(validRows.length - 1, 1);
  const coords = validRows.map((row, idx) => ({
    x: left + (idx * step),
    y: bottom - (((row.unitCostNumber - min) / span) * (bottom - top)),
    period: row.period,
    unitCost: row.unitCostNumber,
    financial: row.financial_value,
    production: row.production_value,
  }));
  const axis = svgNode("g", {class: "finprod-chart-axis"});
  const ticks = [max, min + span / 2, min];
  ticks.forEach((tick) => {
    const y = bottom - (((tick - min) / span) * (bottom - top));
    axis.appendChild(svgNode("line", {x1: left, y1: y, x2: right, y2: y, class: "finprod-grid-line"}));
    const label = svgNode("text", {x: left - 10, y: y + 4, "text-anchor": "end"});
    label.textContent = formatSmartNumber(tick);
    axis.appendChild(label);
  });
  axis.appendChild(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom, class: "finprod-axis-line"}));
  axis.appendChild(svgNode("line", {x1: left, y1: top, x2: left, y2: bottom, class: "finprod-axis-line"}));
  const yTitle = svgNode("text", {x: left, y: 17, class: "finprod-axis-title"});
  yTitle.textContent = "custo unitário";
  axis.appendChild(yTitle);
  const xTitle = svgNode("text", {x: right, y: height - 14, "text-anchor": "end", class: "finprod-axis-title"});
  xTitle.textContent = "período";
  axis.appendChild(xTitle);
  finProdTrendChart.appendChild(axis);
  finProdTrendChart.appendChild(svgNode("path", {
    class: "data-trend-line",
    d: coords.map((coord, idx) => `${idx ? "L" : "M"} ${coord.x} ${coord.y}`).join(" "),
  }));
  coords.forEach((coord, index) => {
    const group = svgNode("g", {class: "data-trend-point", transform: `translate(${coord.x}, ${coord.y})`});
    group.appendChild(svgNode("circle", {r: 3.8}));
    const title = svgNode("title");
    title.textContent = `${coord.period}: custo ${formatSmartNumber(coord.unitCost)} · fin. ${formatSmartNumber(coord.financial)} · prod. ${formatSmartNumber(coord.production)}`;
    group.appendChild(title);
    finProdTrendChart.appendChild(group);
    const shouldLabelPoint = coords.length <= 8 || index === 0 || index === coords.length - 1;
    if (shouldLabelPoint) {
      const valueLabel = svgNode("text", {
        x: coord.x,
        y: Math.max(14, coord.y - 10),
        "text-anchor": "middle",
        class: "finprod-point-label",
      });
      valueLabel.textContent = formatSmartNumber(coord.unitCost);
      finProdTrendChart.appendChild(valueLabel);
    }
    const shouldLabelPeriod = coords.length <= 6 || index === 0 || index === coords.length - 1;
    if (shouldLabelPeriod) {
      const periodLabel = svgNode("text", {
        x: coord.x,
        y: bottom + 20,
        "text-anchor": index === 0 ? "start" : index === coords.length - 1 ? "end" : "middle",
        class: "finprod-period-label",
      });
      periodLabel.textContent = compactTitle(coord.period, 11);
      finProdTrendChart.appendChild(periodLabel);
    }
  });
  const note = svgNode("text", {x: left, y: height - 14, class: "finprod-chart-note"});
  note.textContent = `${formatNumber(values.length)} ponto(s) válidos · escala real do custo unitário`;
  finProdTrendChart.appendChild(note);
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
  const search = normalizeSearch(state.search).trim();
  const correlations = (payload.correlations || []).filter((item) => {
    if (state.confidence && item.confidence !== state.confidence) return false;
    if (state.kind && !(item.dimension_kinds || []).includes(state.kind)) return false;
    if (state.localScope) {
      const scopes = item.local_scopes || [];
      if (state.localScope === "none") {
        if (scopes.length) return false;
      } else if (!scopes.includes(state.localScope)) {
        return false;
      }
    }
    if (state.lowRiskOnly && (item.risk_flags || []).length) return false;
    if (!search) return true;
    const text = [
      item.source_title,
      item.target_title,
      item.source_theme,
      item.target_theme,
      ...(item.shared_fields || []),
      ...(item.dimension_kinds || []),
      ...(item.local_scopes || []),
      ...(item.institution_types || []),
      ...(item.facet_tags || []),
    ].join(" ");
    return matchesExpandedSearch(text, search);
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
  const search = normalizeSearch(state.search).trim();
  const dimensions = (payload.dimensions || []).filter((item) => {
    if (state.kind && item.kind !== state.kind) return false;
    if (!search) return true;
    return matchesExpandedSearch(`${item.field} ${item.kind}`, search);
  });
  if (state._filterCache.key !== key) {
    state._filterCache.key = key;
    state._filterCache.correlations = null;
  }
  state._filterCache.dimensions = dimensions;
  return dimensions;
}

function clearSemanticFilters() {
  state.search = "";
  state.kind = "";
  state.localScope = "";
  state.confidence = "";
  state.lowRiskOnly = false;
  analyticsSearch.value = "";
  analyticsKind.value = "";
  if (analyticsLocalScope) analyticsLocalScope.value = "";
  analyticsConfidence.value = "";
  invalidateFilterCache();
  renderAll();
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

function healthGeoText(item) {
  return normalizeSearchText([
    item.source_title,
    item.target_title,
    item.source_theme,
    item.target_theme,
    ...(item.shared_fields || []),
  ].join(" "));
}

function hasTerritorialFieldSignal(item, text = healthGeoText(item)) {
  const kinds = item.dimension_kinds || [];
  const scopes = item.local_scopes || [];
  return kinds.includes("territorial")
    || scopes.some((scope) => ["regiao", "uls", "hospital", "concelho", "entidade", "uf"].includes(scope))
    || /\b(regiao|ars|geografica|geografico|localizacao|concelho|distrito|freguesia|municipio|territorio|uls|hospital|instituicao|entidade|aces|usf|ucsp|unidade funcional)\b/.test(text);
}

function hasRegionalFieldSignal(item, text = healthGeoText(item)) {
  const scopes = item.local_scopes || [];
  return scopes.includes("regiao") || /\b(regiao|ars|area dos csp|ars uls)\b/.test(text);
}

function hasLocalEntitySignal(item, text = healthGeoText(item)) {
  const scopes = item.local_scopes || [];
  return scopes.some((scope) => ["uls", "hospital", "uf", "entidade"].includes(scope))
    || /\b(uls|unidade local de saude|hospital|centro hospitalar|aces|usf|ucsp|unidade funcional|instituicao|entidade)\b/.test(text);
}

function publicHealthScopeLabel(row) {
  if (!row) return "Âmbito por validar";
  if (row.geo.id === "nacional") return "Nacional · sem campo regional";
  if (row.geo.id === "agregar") {
    if (row.geo.resolution === "regional-field") return "Campo regional · valores por resolver";
    if (row.geo.resolution === "territorial-field") return "Campo territorial · valores por resolver";
    return "Entidade local · por corresponder";
  }
  if (row.geo.resolution === "mapped") return `${row.geo.label} · ${row.geo.matchedEntity}`;
  return `${row.geo.label} · região inferida`;
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
  const text = healthGeoText(item);
  const mapped = resolveUlsRegion(text);
  if (mapped) return mapped;
  const regional = PUBLIC_HEALTH_GEO.find((geo) => geo.match.test(text));
  if (regional) return {...regional, resolution: "direct", mappingConfidence: "media"};
  if (hasRegionalFieldSignal(item, text)) {
    return {...PUBLIC_HEALTH_AGGREGATION_GEO, resolution: "regional-field", mappingConfidence: "media"};
  }
  if (hasLocalEntitySignal(item, text)) {
    return {...PUBLIC_HEALTH_AGGREGATION_GEO, resolution: "pending", mappingConfidence: "baixa"};
  }
  if (hasTerritorialFieldSignal(item, text)) {
    return {...PUBLIC_HEALTH_AGGREGATION_GEO, resolution: "territorial-field", mappingConfidence: "baixa"};
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
  const key = activeFilterKey() + "|" + state.publicHealthSort;
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
    const trend = dimensionKinds.includes("temporal") ? 8 : 4;
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
  if (row.geo.id === "agregar") parts.push(publicHealthScopeLabel(row).toLowerCase());
  if (row.geo.id !== "nacional" && row.geo.id !== "agregar") parts.push(`área: ${row.geo.label}`);
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
    const message = document.createElement("span");
    message.textContent = "Sem oportunidades para os filtros atuais.";
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "ghost-button";
    clear.textContent = "Limpar filtros";
    clear.addEventListener("click", clearSemanticFilters);
    empty.append(message, clear);
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
    meta.textContent = `${viabilityLabel(model.likelihood?.level || "baixa")} · ${impactLabel(model.impact?.level || "baixo")} impacto · ${(item.risk_flags || []).length ? "validar risco" : "baixo risco"}`;
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
    renderAnalyticsStory(dataStoryStrip, [
      {
        label: "Pergunta ativa",
        value: "Que resultados existem no dataset?",
        detail: "Escolhe um dataset e corre a análise para ver medidas, tendência e distribuição.",
      },
      {
        label: "Leitura recomendada",
        value: "Sem amostra ativa",
        detail: "A página só deve interpretar dados reais carregados da API.",
        tone: "warning",
      },
      {
        label: "Próxima ação",
        value: "Analisar dados",
        detail: "Começa por dataset rico; usa dataset atual só quando queres auditar um caso específico.",
      },
    ]);
    dataAnalyticsMeta.textContent = "Escolhe um dataset para medir sinais reais.";
    dataStatRows.textContent = "-";
    dataStatCompletenessValue.textContent = "-";
    dataStatCounts.textContent = "-";
    clearElement(dataStatSuitability);
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

  const coverage = payload.sample?.coverage_ratio == null ? "" : ` · cobertura ${formatDecimal(payload.sample.coverage_ratio * 100, 1)}%`;
  const warningCount = (payload.quality_warnings || []).length;
  const fit = readinessInfo(payload);
  const leadInsight = (payload.insights || [])[0];
  const leadMetric = (payload.numeric_profiles || [])[0];
  const leadTrend = (payload.trends || [])[0];
  renderAnalyticsStory(dataStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "O que a amostra mostra?",
      detail: `${formatNumber(payload.sample_size)} registos lidos${coverage}; ${leadMetric ? `indicador líder: ${compactTitle(leadMetric.label || leadMetric.field, 62)}` : "sem medida numérica dominante"}.`,
    },
    {
      label: "Leitura recomendada",
      value: leadInsight ? compactTitle(leadInsight.value, 46) : (leadMetric ? compactTitle(leadMetric.label || leadMetric.field, 46) : "Sem achado forte"),
      detail: leadInsight?.detail || (leadTrend ? `Ler tendência de ${compactTitle(leadTrend.label || leadTrend.field, 58)}.` : "Usar dimensões/categorias; sem tendência temporal robusta."),
      tone: fit.band || "rever",
    },
    {
      label: "Travão",
      value: fit.label || "Rever",
      detail: firstWarning(payload.quality_warnings || [], readinessDetail(fit)),
      tone: warningCount ? "warning" : "ok",
    },
    {
      label: "Próxima ação",
      value: leadTrend ? "Explorar tendência" : "Abrir dados-chave",
      detail: leadTrend ? "Comparar períodos e confirmar se a amostra não está truncada." : "Ver medidas e dimensões com maior completude antes de cruzar.",
    },
  ]);
  dataAnalyticsMeta.textContent = `${payload.title} · n=${formatNumber(payload.sample_size)}${coverage} · fit ${fit.label || "Rever"}${payload.temporal_field ? ` · tempo: ${payload.temporal_field}` : ""}${warningCount ? ` · ${warningCount} aviso(s)` : ""}.`;

  dataStatRows.textContent = formatNumber(payload.sample_size);

  const numericProfiles = payload.numeric_profiles || [];
  const categoricalProfiles = payload.categorical_profiles || [];
  const allProfiles = [...numericProfiles, ...categoricalProfiles];
  const totalRecords = payload.sample_size || 1;
  const avgCompleteness = allProfiles.length > 0
    ? (allProfiles.reduce((acc, p) => acc + (1 - (p.missing || 0) / totalRecords), 0) / allProfiles.length)
    : 0;

  dataStatCompletenessValue.textContent = `${formatDecimal(avgCompleteness * 100, 1)}%`;
  if (dataStatCompletenessBar) {
    dataStatCompletenessBar.style.width = `${avgCompleteness * 100}%`;
  }

  dataStatCounts.textContent = `${numericProfiles.length} med. / ${categoricalProfiles.length} dim.`;

  clearElement(dataStatSuitability);
  const suitabilityBadge = document.createElement("div");
  suitabilityBadge.className = `suitability-badge is-${fit.band || "fragil"}`;
  suitabilityBadge.textContent = fit.label || "Frágil";
  dataStatSuitability.appendChild(suitabilityBadge);

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
  const warnings = payload.quality_warnings || [];

  function addCard({label, value, detail, tone = ""}) {
    const card = document.createElement("div");
    card.className = `data-insight-card ${tone}`.trim();
    const labelEl = document.createElement("span");
    labelEl.textContent = label;
    const valueEl = document.createElement("strong");
    valueEl.textContent = value;
    valueEl.title = value;
    const detailEl = document.createElement("small");
    detailEl.textContent = detail;
    card.append(labelEl, valueEl, detailEl);
    dataInsightCards.appendChild(card);
  }

  const fit = readinessInfo(payload);
  addCard({
    label: "Fit da amostra",
    value: `${fit.label || "Rever"} ${fit.score ?? 0}`,
    detail: readinessDetail(fit),
    tone: `quality-${fit.band || "fragil"}`,
  });

  if (!insights.length && !warnings.length) {
    const empty = document.createElement("div");
    empty.className = "data-insight-card";
    const label = document.createElement("span");
    label.textContent = "Sinais";
    const value = document.createElement("strong");
    value.textContent = "baixo sinal";
    const detail = document.createElement("small");
    detail.textContent = "A amostra ainda não mostra associações fortes. Validar denominadores antes de concluir.";
    empty.append(label, value, detail);
    dataInsightCards.appendChild(empty);
    return;
  }

  if (warnings.length) {
    addCard({
      label: "Atenção",
      value: `${warnings.length} aviso(s)`,
      detail: warnings.slice(0, 2).join(" · "),
      tone: "quality-rever",
    });
  }

  const remainingSlots = Math.max(1, 4 - dataInsightCards.children.length);
  insights.slice(0, remainingSlots).forEach((insight) => {
    addCard({
      label: insight.label,
      value: insight.value,
      detail: insight.detail,
    });
  });
}

function correlationDirectionLabel(value) {
  const magnitude = Math.abs(Number(value || 0));
  const direction = Number(value || 0) >= 0 ? "positiva" : "negativa";
  if (magnitude >= 0.9) return `${direction} muito forte`;
  if (magnitude >= 0.7) return `${direction} forte`;
  if (magnitude >= 0.5) return `${direction} moderada`;
  return `${direction} fraca`;
}

function createCorrelationMetric(label, value) {
  const metric = document.createElement("span");
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = label;
  metric.append(strong, small);
  return metric;
}

function renderDataCorrelations(payload) {
  clearElement(dataCorrelationList);
  const rows = payload.correlations || [];
  const exclusions = payload.correlation_exclusions || [];
  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    const title = document.createElement("strong");
    title.textContent = "Sem associações independentes fortes.";
    const detail = document.createElement("small");
    detail.textContent = exclusions.length
      ? `${formatNumber(exclusions.length)} par(es) derivado(s) foram ocultados: percentagens complementares, taxa vs total ou parte-total.`
      : "A amostra não tem pares numéricos suficientemente robustos depois das validações.";
    empty.append(title, detail);
    dataCorrelationList.appendChild(empty);
    return;
  }
  rows.slice(0, 10).forEach((row) => {
    const positive = Number(row.correlation || 0) >= 0;
    const item = document.createElement("article");
    item.className = positive ? "data-correlation-card is-positive" : "data-correlation-card is-negative";

    const head = document.createElement("div");
    head.className = "data-correlation-head";
    const direction = document.createElement("strong");
    direction.textContent = correlationDirectionLabel(row.correlation);
    const sample = document.createElement("span");
    sample.textContent = `${formatNumber(row.samples)} pares`;
    head.append(direction, sample);

    const pair = document.createElement("div");
    pair.className = "data-correlation-pair";
    [
      ["Medida A", row.label_a],
      ["Medida B", row.label_b],
    ].forEach(([labelText, valueText]) => {
      const block = document.createElement("div");
      const label = document.createElement("span");
      label.textContent = labelText;
      const value = document.createElement("strong");
      value.textContent = safeText(valueText);
      value.title = safeText(valueText);
      block.append(label, value);
      pair.appendChild(block);
    });

    const meter = document.createElement("div");
    meter.className = "data-correlation-meter";
    const center = document.createElement("i");
    center.className = "data-correlation-zero";
    const fill = document.createElement("b");
    fill.style.width = `${Math.max(3, Math.min(50, Math.abs(Number(row.correlation || 0)) * 50))}%`;
    if (positive) {
      fill.style.left = "50%";
    } else {
      fill.style.right = "50%";
    }
    meter.append(fill, center);

    const metrics = document.createElement("div");
    metrics.className = "data-correlation-metrics";
    metrics.append(
      createCorrelationMetric("Pearson", formatDecimal(row.correlation, 2)),
      createCorrelationMetric("Spearman", row.spearman == null ? "-" : formatDecimal(row.spearman, 2)),
      createCorrelationMetric("Amostra", formatNumber(row.samples)),
    );

    const note = document.createElement("small");
    note.className = "data-correlation-note";
    const warnings = row.warnings || [];
    note.textContent = warnings.length
      ? `Validar: ${warnings.slice(0, 2).join(" · ")}`
      : "Associação estatística; não implica causalidade.";

    item.append(head, pair, meter, metrics, note);
    dataCorrelationList.appendChild(item);
  });
  if (exclusions.length) {
    const note = document.createElement("div");
    note.className = "data-correlation-filter-note";
    const firstReason = safeText(exclusions[0].reason || "pares derivados");
    note.textContent = `${formatNumber(exclusions.length)} par(es) derivado(s) ocultado(s). Principal motivo: ${firstReason}.`;
    dataCorrelationList.appendChild(note);
  }
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
  const pcaWarnings = (pca.warnings || []).length ? ` · avisos: ${(pca.warnings || []).slice(0, 2).join(" · ")}` : "";
  dataPcaMeta.textContent = `PC1 explica ${pc1}% · PC2 explica ${pc2}% da variação padronizada${pcaWarnings}.`;

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
    item.className = row.semantic_role === "numeric_distribution" ? "data-profile-row data-profile-numeric" : "data-profile-row";
    const title = document.createElement("strong");
    title.textContent = row.label || row.field;
    const meta = document.createElement("small");
    const role = measureRoleLabel(row.measure_role);
    meta.textContent = `${formatNumber(row.count)} valores · ${formatNumber(row.missing)} em falta · ${role}`;
    const stats = document.createElement("div");
    stats.className = "data-profile-stats";
    [
      ["média", formatDecimal(row.avg, 2)],
      ["min", formatDecimal(row.min, 2)],
      ["max", formatDecimal(row.max, 2)],
      ["desvio", formatDecimal(row.stddev, 2)],
      ["CV", row.avg ? formatDecimal((row.stddev / row.avg) * 100, 1) + "%" : "-"],
    ].forEach(([label, value]) => {
      const chip = document.createElement("span");
      chip.textContent = `${label}: ${formatDisplayValue(value)}`;
      stats.appendChild(chip);
    });
    item.append(title, meta, stats);
    dataNumericProfiles.appendChild(item);
  });
}


function dimensionContext(row, payload) {
  const sampleSize = payload.sample_size || row.count || 0;
  const total = Math.max(1, (row.count || 0) + (row.missing || 0), sampleSize || 1);
  const coverage = Math.max(0, Math.min(1, (row.count || 0) / total));
  const uniqueRatio = (row.unique || 0) / Math.max(1, row.count || sampleSize || 1);
  const text = normalizeSearchText([row.field, row.label, row.semantic_role].filter(Boolean).join(" "));
  const baseWarning = coverage < 0.85
    ? `Cobertura ${formatDecimal(coverage * 100, 1)}%; validar nulos antes de segmentar.`
    : `Cobertura ${formatDecimal(coverage * 100, 1)}%; validar fonte e granularidade.`;

  if (row.semantic_role === "geolocation" || /geo|localizacao|localização|latitude|longitude|coordenad/.test(text)) {
    return {
      label: "Território",
      value: "Usar para mapa, não para ranking nominal",
      detail: "Converter coordenadas para região, ULS, concelho ou ponto validado antes de comparar resultados.",
      warning: baseWarning,
      tone: "warning",
    };
  }
  if (row.semantic_role === "numeric_distribution") {
    return {
      label: "Medida contínua",
      value: "Não tratar como categoria",
      detail: "Ler distribuição, outliers e tendência; não comparar valores únicos como grupos.",
      warning: baseWarning,
      tone: "warning",
    };
  }
  if (/periodo|período|ano|mes|mês|data|trimestre|semana|dia/.test(text)) {
    return {
      label: "Tempo",
      value: "Serve para tendência e sazonalidade",
      detail: "Comparar períodos equivalentes e confirmar se há meses/anos em falta antes de concluir evolução.",
      warning: baseWarning,
      tone: coverage >= 0.9 ? "ok" : "warning",
    };
  }
  if (/uls|hospital|instituicao|instituição|entidade|ars|aces|usf|ucsp|unidade/.test(text)) {
    return {
      label: "Entidade",
      value: "Serve para benchmarking cauteloso",
      detail: "Comparar apenas entidades com a mesma função, período e denominador operacional.",
      warning: baseWarning,
      tone: coverage >= 0.9 ? "ok" : "warning",
    };
  }
  if (/sexo|genero|género|idade|grupo etario|grupo etário|populacao|população|utente|inscrito/.test(text)) {
    return {
      label: "População",
      value: "Serve para estratificar resultados",
      detail: "Usar com denominador explícito e evitar conclusões sobre grupos pequenos ou incompletos.",
      warning: baseWarning,
      tone: coverage >= 0.9 ? "ok" : "warning",
    };
  }
  if ((row.unique || 0) <= 1) {
    return {
      label: "Sem variação",
      value: "Pouco útil para segmentar",
      detail: "A dimensão não separa grupos nesta amostra; usar só como metadado de contexto.",
      warning: baseWarning,
      tone: "danger",
    };
  }
  if (uniqueRatio > 0.85 && (row.unique || 0) > 20) {
    return {
      label: "Identificador provável",
      value: "Não usar como categoria analítica",
      detail: "Muitos valores únicos sugerem identificador, descrição livre ou granularidade demasiado fina.",
      warning: baseWarning,
      tone: "danger",
    };
  }
  return {
    label: "Categoria operacional",
    value: "Serve para comparar grupos",
    detail: "Ler a distribuição como segmentação do dataset, não como resultado por si só.",
    warning: baseWarning,
    tone: coverage >= 0.9 ? "ok" : "warning",
  };
}

function appendDimensionContext(container, context) {
  const panel = document.createElement("div");
  panel.className = `data-dimension-context is-${context.tone}`;
  [
    ["Papel", context.label],
    ["Uso", context.value],
    ["Validação", context.warning],
  ].forEach(([labelText, valueText]) => {
    const block = document.createElement("span");
    const label = document.createElement("b");
    label.textContent = labelText;
    const value = document.createElement("small");
    value.textContent = valueText;
    block.append(label, value);
    panel.appendChild(block);
  });
  const detail = document.createElement("p");
  detail.textContent = context.detail;
  panel.appendChild(detail);
  container.appendChild(panel);
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
    item.className = row.semantic_role === "geolocation" ? "data-profile-row data-profile-geo" : "data-profile-row";
    const title = document.createElement("strong");
    title.textContent = row.label || row.field;
    const meta = document.createElement("small");
    meta.textContent = row.semantic_role === "geolocation"
      ? `${formatNumber(row.unique)} ponto(s) · ${formatNumber(row.missing)} em falta`
      : `${formatNumber(row.unique)} categorias · ${formatNumber(row.missing)} em falta`;
    const context = dimensionContext(row, payload);
    const values = document.createElement("div");
    if (row.semantic_role === "geolocation") {
      values.className = "data-geo-summary";
      const bounds = row.bounds || {};
      const center = row.center || {};
      [
        ["Centro", `${formatDecimal(center.lat, 4)}, ${formatDecimal(center.lon, 4)}`],
        ["Latitude", `${formatDecimal(bounds.lat_min, 4)} a ${formatDecimal(bounds.lat_max, 4)}`],
        ["Longitude", `${formatDecimal(bounds.lon_min, 4)} a ${formatDecimal(bounds.lon_max, 4)}`],
      ].forEach(([label, value]) => {
        const chip = document.createElement("span");
        const strong = document.createElement("strong");
        strong.textContent = label;
        const small = document.createElement("small");
        small.textContent = value;
        chip.append(strong, small);
        values.appendChild(chip);
      });
      const note = document.createElement("p");
      note.textContent = "Coordenadas tratadas como dimensão espacial; não são categorias para ranking.";
      values.appendChild(note);
    } else if (row.semantic_role === "numeric_distribution") {
      values.className = "data-numeric-summary";
      const summary = row.numeric_summary || row;
      [
        ["min", formatDecimal(summary.min, 2)],
        ["mediana", formatDecimal(summary.median, 2)],
        ["max", formatDecimal(summary.max, 2)],
        ["média", formatDecimal(summary.avg, 2)],
      ].forEach(([label, value]) => {
        const chip = document.createElement("span");
        chip.textContent = `${label}: ${value}`;
        values.appendChild(chip);
      });
      const note = document.createElement("p");
      note.textContent = "Campo decimal mostrado como distribuição; não é uma categoria nominal.";
      values.appendChild(note);
    } else {
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
    }
    item.append(title, meta);
    appendDimensionContext(item, context);
    item.appendChild(values);
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

function predictiveOpportunityDiagnostics(trend, payload = null) {
  const points = trendPoints(trend);
  const eligibility = predictiveEligibility(trend, payload);
  const sampleSize = payload?.sample_size || 0;
  const recordsPerPeriod = points.length ? sampleSize / points.length : 0;
  const values = points.map((point) => point.value).filter((value) => Number.isFinite(value));
  const uniqueValues = new Set(values.map((value) => formatDecimal(value, 6))).size;
  const min = values.length ? Math.min(...values) : 0;
  const max = values.length ? Math.max(...values) : 0;
  const mean = values.length ? values.reduce((acc, value) => acc + value, 0) / values.length : 0;
  const relativeSpan = values.length ? Math.abs((max - min) / (Math.abs(mean) || 1)) : 0;
  const gaps = [];
  if (points.length < 2) gaps.push("sem pontos temporais comparáveis");
  if (points.length >= 2 && points.length < 4) gaps.push(`faltam ${4 - points.length} período(s) para projeção curta`);
  if (points.length > 50) gaps.push("série longa exige agregação ou sazonalidade");
  if (sampleSize && recordsPerPeriod < 8) gaps.push("baixo volume médio por período");
  if (values.length && uniqueValues < 3) gaps.push("valores quase constantes");
  if (values.length && relativeSpan < 0.015) gaps.push("variação relativa baixa");
  if (!gaps.length && eligibility.ok) gaps.push("apta para projeção exploratória curta");
  const score = Math.max(
    0,
    Math.min(
      100,
      Math.round(
        Math.min(34, points.length * 8)
        + Math.min(22, recordsPerPeriod || sampleSize / 6)
        + Math.min(22, relativeSpan * 180)
        + Math.min(12, uniqueValues * 3)
        + (eligibility.ok ? 10 : 0)
        - (points.length > 50 ? 16 : 0)
      )
    )
  );
  return {
    trend,
    eligibility,
    label: trend?.label || "Série temporal",
    points: points.length,
    score,
    recordsPerPeriod,
    relativeSpan,
    gaps,
    mode: eligibility.ok ? "projetar" : points.length >= 2 ? "rever tendência descritiva" : "enriquecer dados temporais",
  };
}

function predictiveNearMissCandidates(payload, limit = 5) {
  return (payload?.trends || [])
    .filter((trend) => (trend.points || []).length)
    .map((trend) => predictiveOpportunityDiagnostics(trend, payload))
    .sort((a, b) => (b.score - a.score) || (b.points - a.points))
    .slice(0, limit);
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

function predictiveFitBandLabel(band) {
  return {
    ready: "apta",
    usable: "usável",
    near: "quase",
    blocked: "sem base",
  }[band] || "por avaliar";
}

function predictiveFitBandTitle(row = {}) {
  const band = row.band || "unknown";
  const eligible = Number(row.eligible_count || 0);
  const near = Number(row.near_miss_count || 0);
  if (band === "ready") return `${formatNumber(eligible)} série(s) apta(s)`;
  if (band === "usable") return `${formatNumber(eligible)} série(s) usável(eis)`;
  if (band === "near") return `${formatNumber(near)} quase apta(s)`;
  if (band === "blocked") return "sem análise preditiva";
  return "por avaliar";
}

function predictiveFitDetail(row = {}) {
  const parts = [];
  if (row.best_indicator) parts.push(compactTitle(row.best_indicator, 70));
  if (row.best_points) parts.push(`${formatNumber(row.best_points)} períodos`);
  if (row.best_score) parts.push(`score ${formatDecimal(row.best_score, 0)}`);
  if (!parts.length && row.reason) parts.push(row.reason);
  const gaps = (row.gaps || []).filter(Boolean).slice(0, 2).join(" · ");
  return `${parts.join(" · ") || "sem série temporal avaliada"}${gaps ? ` · ${gaps}` : ""}`;
}

function predictiveFitRows() {
  return state.predictiveRecommendationPayload?.recommendations || [];
}

function activatePredictiveDataset(datasetId) {
  if (!datasetId || datasetId === state.selectedDataDataset) return;
  activePredictiveRecommendationRequest += 1;
  state.selectedDataDataset = datasetId;
  state.predictiveRecommendationPayload = null;
  state._predictiveRecommendationKey = "";
  if (dataDatasetSelect) dataDatasetSelect.value = datasetId;
  state.datasetMode = "manual";
  renderDatasetMode();
  loadDataAnalytics().then(() => setActiveTab("predictive")).catch(showDataError);
}

function renderPredictiveDatasetCandidates() {
  if (!predictiveDatasetCandidates) return;
  clearElement(predictiveDatasetCandidates);
  const evaluatedRows = predictiveFitRows();
  if (evaluatedRows.length) {
    evaluatedRows.slice(0, 10).forEach((row) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = `predictive-dataset-card is-${row.band || "unknown"} ${row.dataset_id === state.selectedDataDataset ? "is-active" : ""}`.trim();
      const title = document.createElement("strong");
      title.textContent = compactTitle(row.title || row.dataset_id, 70);
      title.title = row.title || row.dataset_id;
      const meta = document.createElement("small");
      meta.textContent = `${row.mega_theme || "Catálogo"} · ${predictiveFitBandTitle(row)} · ${predictiveFitDetail(row)}`;
      const action = document.createElement("span");
      action.textContent = row.dataset_id === state.selectedDataDataset ? "em análise" : predictiveFitBandLabel(row.band);
      button.append(title, meta, action);
      button.addEventListener("click", () => activatePredictiveDataset(row.dataset_id));
      predictiveDatasetCandidates.appendChild(button);
    });
    return;
  }
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
    button.addEventListener("click", () => activatePredictiveDataset(dataset.dataset_id));
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

async function loadPredictiveRecommendations() {
  if (!state.selectedDataDataset || !predictiveFitGuide) return;
  const key = `${state.selectedDataDataset}|${state.dataLimit}`;
  const requestId = ++activePredictiveRecommendationRequest;
  state._predictiveRecommendationKey = key;
  renderPredictiveFitGuide({loading: true});
  const payload = await fetchJson(
    `/api/predictive/recommendations?dataset_id=${encodeURIComponent(state.selectedDataDataset)}&limit=${state.dataLimit}&candidate_limit=8`,
    {timeoutMs: 52000},
  );
  if (requestId !== activePredictiveRecommendationRequest || state._predictiveRecommendationKey !== key) return;
  state.predictiveRecommendationPayload = payload;
  renderPredictiveFitGuide(payload);
  renderPredictiveDatasetCandidates();
}

function renderPredictiveFitGuide(payload = state.predictiveRecommendationPayload) {
  if (!predictiveFitGuide) return;
  clearElement(predictiveFitGuide);
  if (payload?.loading) {
    const note = document.createElement("div");
    note.className = "predictive-fit-note";
    note.textContent = "A avaliar datasets com séries temporais realmente utilizáveis...";
    predictiveFitGuide.appendChild(note);
    return;
  }
  const rows = payload?.recommendations || [];
  if (!rows.length) {
    const note = document.createElement("div");
    note.className = "predictive-fit-note is-muted";
    note.textContent = "Sem matriz de viabilidade carregada. A página avalia primeiro o dataset ativo e depois sugere alternativas temporais.";
    predictiveFitGuide.appendChild(note);
    return;
  }
  const active = payload.active || rows.find((row) => row.dataset_id === state.selectedDataDataset);
  const summary = document.createElement("div");
  summary.className = `predictive-fit-summary is-${active?.band || "unknown"}`;
  const strong = document.createElement("strong");
  strong.textContent = active
    ? `Dataset atual: ${predictiveFitBandTitle(active)}`
    : "Dataset atual ainda não avaliado";
  const small = document.createElement("small");
  small.textContent = active
    ? predictiveFitDetail(active)
    : "Escolhe um dataset ou usa uma alternativa apta abaixo.";
  summary.append(strong, small);

  const legend = document.createElement("div");
  legend.className = "predictive-fit-legend";
  [
    ["ready", "apta"],
    ["usable", "usável"],
    ["near", "quase"],
    ["blocked", "sem base"],
  ].forEach(([band, label]) => {
    const item = document.createElement("span");
    item.className = `is-${band}`;
    item.textContent = label;
    legend.appendChild(item);
  });

  const list = document.createElement("div");
  list.className = "predictive-fit-list";
  rows.slice(0, 6).forEach((row) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `predictive-fit-card is-${row.band || "unknown"} ${row.dataset_id === state.selectedDataDataset ? "is-active" : ""}`.trim();
    const title = document.createElement("strong");
    title.textContent = compactTitle(row.title || row.dataset_id, 74);
    title.title = row.title || row.dataset_id;
    const meta = document.createElement("small");
    meta.textContent = `${predictiveFitBandTitle(row)} · ${predictiveFitDetail(row)}`;
    const action = document.createElement("span");
    action.textContent = row.dataset_id === state.selectedDataDataset ? "atual" : predictiveFitBandLabel(row.band);
    button.append(title, meta, action);
    button.addEventListener("click", () => activatePredictiveDataset(row.dataset_id));
    list.appendChild(button);
  });

  predictiveFitGuide.append(summary, legend, list);
}

function renderPredictiveClosestOpportunities(payload, forecast, eligibleCandidates) {
  if (!predictiveClosestOpportunities) return;
  clearElement(predictiveClosestOpportunities);
  if (!payload) return;
  const nearMisses = predictiveNearMissCandidates(payload).filter((row) => !row.eligibility.ok);
  const shouldShowFallback = !forecast?.available || !eligibleCandidates.length;
  if (!shouldShowFallback && !nearMisses.length) return;

  const summary = document.createElement("div");
  summary.className = `predictive-opportunity-summary ${forecast?.available ? "is-secondary" : "is-warning"}`;
  const title = document.createElement("strong");
  title.textContent = forecast?.available ? "Oportunidades de robustez" : "Sem dataset fit: usar melhor oportunidade próxima";
  const detail = document.createElement("span");
  detail.textContent = forecast?.available
    ? "Mesmo com projeção ativa, valide denominadores, cobertura temporal e valores em falta antes de decidir."
    : "A página não força uma previsão. Mostra séries quase aptas ou alternativas do catálogo para orientar a próxima análise.";
  summary.append(title, detail);
  predictiveClosestOpportunities.appendChild(summary);

  const rows = nearMisses.slice(0, 3);
  if (!rows.length) {
    const catalogRows = predictiveDatasetCandidatesFromCatalog().slice(0, 3);
    if (!catalogRows.length) {
      const empty = document.createElement("div");
      empty.className = "empty-state compact";
      empty.textContent = "Sem séries temporais nem alternativas fortes no catálogo atual. Recolhe mais períodos ou escolhe outro âmbito.";
      predictiveClosestOpportunities.appendChild(empty);
      return;
    }
    const list = document.createElement("div");
    list.className = "predictive-opportunity-list";
    catalogRows.forEach(({dataset, score, reasons}) => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "predictive-opportunity-card";
      const strong = document.createElement("strong");
      strong.textContent = compactTitle(dataset.title || dataset.dataset_id, 76);
      strong.title = dataset.title || dataset.dataset_id;
      const small = document.createElement("small");
      small.textContent = `Dataset alternativo · ${reasons.slice(0, 3).join(" · ")} · score ${formatDecimal(score, 0)}`;
      const action = document.createElement("span");
      action.textContent = "analisar dataset";
      button.append(strong, small, action);
      button.addEventListener("click", () => activatePredictiveDataset(dataset.dataset_id));
      list.appendChild(button);
    });
    predictiveClosestOpportunities.appendChild(list);
    return;
  }

  const list = document.createElement("div");
  list.className = "predictive-opportunity-list";
  rows.forEach((row, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "predictive-opportunity-card";
    const strong = document.createElement("strong");
    strong.textContent = index === 0 ? `Melhor próxima: ${compactTitle(row.label, 64)}` : compactTitle(row.label, 78);
    strong.title = row.label;
    const small = document.createElement("small");
    small.textContent = `${row.mode} · ${row.points} período(s) · score ${formatDecimal(row.score, 0)} · ${row.gaps.slice(0, 2).join(" · ")}`;
    const action = document.createElement("span");
    action.textContent = "focar série";
    button.append(strong, small, action);
    button.addEventListener("click", () => {
      state.selectedPredictiveIndicator = row.label;
      renderPredictiveForecast(forecastFromTrend(row.trend, payload));
      renderPredictiveScenarios(forecastFromTrend(row.trend, payload));
      renderPredictiveRisk(payload, forecastFromTrend(row.trend, payload), row);
      renderPredictiveClosestOpportunities(payload, forecastFromTrend(row.trend, payload), eligibleCandidates);
    });
    list.appendChild(button);
  });
  predictiveClosestOpportunities.appendChild(list);
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
  [predictiveKpis, predictiveForecastChart, predictiveFitGuide, predictiveClosestOpportunities, predictiveDrivers, predictiveScenarios, predictiveRisk].forEach((node) => {
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

function renderPredictiveRisk(payload, forecast, opportunity = null) {
  clearElement(predictiveRisk);
  const risks = [];
  if (!forecast?.available) {
    risks.push(["Sem projeção linear", forecast?.reason || "Não há série temporal elegível."]);
    if (opportunity?.gaps?.length) {
      risks.push(["Próxima ação", opportunity.gaps.slice(0, 3).join(" · ")]);
    }
  }
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
    renderAnalyticsStory(predictiveStoryStrip, [
      {
        label: "Pergunta ativa",
        value: "Há série para projeção?",
        detail: "A projeção só aparece depois de analisar um dataset real com eixo temporal utilizável.",
      },
      {
        label: "Leitura recomendada",
        value: "Sem projeção ativa",
        detail: "Evitar previsões quando não há períodos, volume ou variação suficiente.",
        tone: "warning",
      },
      {
        label: "Próxima ação",
        value: "Escolher dataset",
        detail: "Volta a Dados reais e seleciona uma série com melhor cobertura temporal.",
      },
    ]);
    predictiveMeta.textContent = "Escolhe ou analisa um dataset em Dados reais para ativar a projeção.";
    renderPredictiveIndicatorFilter([]);
    renderPredictiveFitGuide(state.predictiveRecommendationPayload || null);
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
  renderAnalyticsStory(predictiveStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "O sinal continua a subir ou descer?",
      detail: trend ? `${compactTitle(trend.label || trend.field, 64)} · ${formatNumber(trend.points.length)} períodos.` : "Sem série temporal elegível.",
    },
    {
      label: "Leitura recomendada",
      value: forecast?.available ? `${slope >= 0 ? "+" : ""}${formatDecimal(slope, 2)} por período` : "Não projetar",
      detail: forecast?.available ? "Usar como alerta operacional curto, não como previsão oficial." : (forecast?.reason || "Sem base temporal suficiente."),
      tone: forecast?.available ? "ok" : "warning",
    },
    {
      label: "Travão",
      value: confidence,
      detail: firstWarning(payload.quality_warnings || [], "Validar sazonalidade, cobertura e estabilidade antes de extrapolar."),
      tone: confidence === "alta" ? "ok" : "warning",
    },
    {
      label: "Próxima ação",
      value: "Comparar cenários",
      detail: "Ver drivers e risco do modelo antes de comunicar tendência.",
    },
  ]);
  predictiveMeta.textContent = `${payload.title} · ${formatNumber(payload.sample_size)} registos · fiabilidade ${confidence}.`;
  addPredictiveKpi("Série", trend ? compactTitle(trend.label, 26) : "indisponível", trend ? `${trend.points.length} períodos` : "sem eixo temporal");
  addPredictiveKpi("Projeção", forecast?.available ? "ativa" : "não aplicável", forecast?.available ? "linear curta" : (forecast?.reason || "sem série elegível"));
  addPredictiveKpi("Elegíveis", formatNumber(candidates.length), "indicadores com volume suficiente");
  addPredictiveKpi("Tendência", slope === null ? "-" : `${slope >= 0 ? "+" : ""}${formatDecimal(slope, 2)}`, "variação média por período");
  const recommendationKey = `${state.selectedDataDataset}|${state.dataLimit}`;
  const recommendationsCurrent = state.predictiveRecommendationPayload?.active_dataset_id === state.selectedDataDataset;
  if (!recommendationsCurrent && state._predictiveRecommendationKey !== recommendationKey) {
    loadPredictiveRecommendations().catch(() => {
      renderPredictiveFitGuide({recommendations: [], active: null});
    });
  } else {
    renderPredictiveFitGuide(state.predictiveRecommendationPayload || null);
  }
  renderPredictiveForecast(forecast);
  renderPredictiveClosestOpportunities(payload, forecast, candidates);
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
  const kindRows = ["temporal", "territorial", "entidade", "clinico", "medida", "generico"].map((kind) => ({
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
    meta.textContent = `${publicHealthScopeLabel(row)} · score ${row.priority} · viabilidade ${formatDecimal(row.viability, 0)}`;
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
  const pendingCount = aggregateRows.length;
  const pendingRatio = rows.length ? pendingCount / rows.length : 0;
  const card = document.createElement("button");
  card.type = "button";
  card.className = `national-summary-card ${pendingRatio >= 0.7 ? "is-warning" : ""}`.trim();
  if (selected?.geo.id === "nacional" || selected?.geo.id === "agregar") card.classList.add("is-active");
  const label = document.createElement("span");
  label.textContent = aggregateRows.length ? "Território por resolver" : "Vista nacional";
  const value = document.createElement("strong");
  value.textContent = aggregateRows.length
    ? `${formatNumber(aggregateRows.length)} hipóteses com campo territorial`
    : `${formatNumber(nationalEntry?.count || 0)} hipóteses nacionais`;
  const detail = document.createElement("small");
  detail.textContent = aggregateRows.length
    ? `${formatNumber(mappedRows.length)} já mapeadas; restantes precisam de regra territorial.`
    : "Separado para não falsear região.";
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

function appendSvgText(parent, lines, x, y, attrs = {}) {
  lines.forEach((line, index) => {
    const {lineHeight = 13, ...textAttrs} = attrs;
    const text = svgNode("text", {...textAttrs, x, y: y + index * lineHeight});
    text.textContent = line;
    parent.appendChild(text);
  });
}

function healthLayerMetric(entry) {
  if (state.activePublicHealthLayer === "impact") return `impacto ${formatDecimal(entry.avgImpact, 1)}/3`;
  if (state.activePublicHealthLayer === "likelihood") return `viab. ${formatDecimal(entry.avgLikelihood, 1)}/3`;
  if (state.activePublicHealthLayer === "risk") return `risco ${formatDecimal(entry.avgRisk, 1)}/10`;
  if (state.activePublicHealthLayer === "entities") return `${formatNumber(entry.entities)} entidade(s)`;
  return `score ${formatDecimal(entry.avgPriority, 0)}`;
}

function polygonPath(points) {
  if (!points.length) return "";
  const [first, ...rest] = points;
  return `M ${first[0]} ${first[1]} ${rest.map((point) => `L ${point[0]} ${point[1]}`).join(" ")} Z`;
}

const TERRITORY_POLYGON_BLOCKS = {
  acores: {label: "Açores", labelX: 118, labelY: 128, points: [[54, 98], [150, 88], [166, 138], [72, 154]]},
  norte: {label: "Norte", labelX: 328, labelY: 80, points: [[268, 48], [378, 50], [398, 120], [248, 132], [238, 82]]},
  centro: {label: "Centro", labelX: 326, labelY: 178, points: [[248, 140], [398, 126], [408, 214], [238, 230]]},
  lisboa: {label: "Lisboa e V. Tejo", labelX: 318, labelY: 278, points: [[238, 238], [408, 222], [398, 310], [266, 328]]},
  alentejo: {label: "Alentejo", labelX: 326, labelY: 374, points: [[266, 338], [398, 320], [410, 414], [248, 432]]},
  algarve: {label: "Algarve", labelX: 330, labelY: 482, points: [[248, 442], [410, 424], [392, 506], [264, 512]]},
  madeira: {label: "Madeira", labelX: 118, labelY: 438, points: [[60, 404], [158, 392], [174, 444], [76, 458]]},
};

function renderPublicHealthMap(rows) {
  clearElement(publicHealthMap);
  const width = 620;
  const height = 500;
  publicHealthMap.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const layerTitle = PUBLIC_HEALTH_LAYERS.find(([key]) => key === state.activePublicHealthLayer)?.[1] || "Impacto";
  const title = svgNode("text", {x: 22, y: 30, class: "health-map-title"});
  title.textContent = `Regiões SNS · ${layerTitle}`;
  publicHealthMap.appendChild(title);
  const subtitle = svgNode("text", {x: 22, y: 50, class: "health-map-subtitle"});
  subtitle.textContent = "Mostra apenas regiões com regra validada; restantes ficam por resolver.";
  publicHealthMap.appendChild(subtitle);

  const selected = rows.find((row) => row.key === state.selectedPublicHealthKey);
  const geoRows = aggregateHealthGeo(rows);
  const entriesById = new Map(geoRows.map((entry) => [entry.id, entry]));
  const regionalRows = Object.keys(TERRITORY_POLYGON_BLOCKS).map((id) => entriesById.get(id) || {id, label: TERRITORY_POLYGON_BLOCKS[id].label, count: 0, avgImpact: 0, avgLikelihood: 0, avgRisk: 0, avgPriority: 0, entities: 0});
  const aggregateEntry = entriesById.get("agregar");
  const nationalEntry = entriesById.get("nacional");
  renderNationalSummary(rows, nationalEntry);

  publicHealthMap.appendChild(svgNode("rect", {x: 18, y: 68, width: 386, height: 392, rx: 16, class: "health-map-board"}));
  appendSvgText(publicHealthMap, ["Regiões SNS"], 36, 94, {class: "health-map-section-title"});
  appendSvgText(publicHealthMap, ["hipóteses com regra regional"], 126, 94, {class: "health-map-legend-text"});

  const maxCount = Math.max(1, ...regionalRows.map((entry) => entry.count || 0));
  regionalRows.forEach((entry, index) => {
    const value = mapLayerValue(entry);
    const level = entry.count ? Math.max(1, Math.min(5, Math.ceil(value * 5))) : 0;
    const active = selected?.geo.id === entry.id;
    const y = 112 + index * 46;
    const metric = entry.count ? healthLayerMetric(entry) : "sem regra regional";
    const countWidth = entry.count ? Math.max(8, Math.round((entry.count / maxCount) * 112)) : 0;
    const layerWidth = entry.count ? Math.max(8, Math.round(value * 112)) : 0;
    const group = svgNode("g", {
      class: `health-map-region health-map-row layer-${state.activePublicHealthLayer} level-${level} ${entry.count ? "" : "is-empty"} ${active ? "is-selected" : ""}`.trim(),
    });
    const titleNode = svgNode("title");
    titleNode.textContent = `${entry.label}: ${formatNumber(entry.count)} hipótese(s) · ${metric}`;
    group.appendChild(titleNode);
    group.appendChild(svgNode("rect", {x: 34, y, width: 354, height: 36, rx: 10}));

    const label = svgNode("text", {x: 48, y: y + 15, class: "health-map-row-label"});
    label.textContent = entry.label;
    group.appendChild(label);

    const metricText = svgNode("text", {x: 48, y: y + 29, class: "health-map-row-metric"});
    metricText.textContent = metric;
    group.appendChild(metricText);

    if (entry.count) {
      group.appendChild(svgNode("rect", {x: 220, y: y + 10, width: 112, height: 6, rx: 3, class: "health-map-bar-bg"}));
      group.appendChild(svgNode("rect", {x: 220, y: y + 10, width: layerWidth, height: 6, rx: 3, class: "health-map-bar-fill"}));
      group.appendChild(svgNode("rect", {x: 220, y: y + 23, width: 112, height: 6, rx: 3, class: "health-map-bar-bg"}));
      group.appendChild(svgNode("rect", {x: 220, y: y + 23, width: countWidth, height: 6, rx: 3, class: "health-map-count-bar"}));
    }

    const count = svgNode("text", {x: 374, y: y + 22, "text-anchor": "end", class: entry.count ? "health-map-count" : "health-map-row-state"});
    count.textContent = entry.count ? `${formatNumber(entry.count)} hipótese(s)` : "por mapear";
    group.appendChild(count);

    group.addEventListener("click", () => {
      const candidate = rows.find((row) => row.geo.id === entry.id);
      if (candidate) {
        state.selectedPublicHealthKey = candidate.key;
        renderPublicHealthMatrix();
      }
    });
    publicHealthMap.appendChild(group);
  });

  const statusRows = [
    {
      id: "agregar",
      label: "Território por resolver",
      value: `${formatNumber(aggregateEntry?.count || 0)} hipótese(s)`,
      detail: ["Campo territorial existe", "mas falta regra regional."],
      warning: true,
    },
    {
      id: "nacional",
      label: "Nacional",
      value: `${formatNumber(nationalEntry?.count || 0)} hipótese(s)`,
      detail: ["Sem leitura regional", "para evitar falso mapa."],
      warning: false,
    },
  ];
  statusRows.forEach((item, index) => {
    const y = 90 + index * 142;
    const active = selected?.geo.id === item.id;
    const group = svgNode("g", {class: `health-map-status ${item.warning ? "is-warning" : ""} ${active ? "is-selected" : ""}`.trim()});
    group.appendChild(svgNode("rect", {x: 424, y, width: 172, height: 118, rx: 14}));
    appendSvgText(group, [item.label], 442, y + 28, {class: "health-map-status-label"});
    appendSvgText(group, [item.value], 442, y + 56, {class: "health-map-status-value"});
    appendSvgText(group, item.detail, 442, y + 80, {class: "health-map-status-detail", lineHeight: 14});
    group.addEventListener("click", () => {
      const candidate = rows.find((row) => row.geo.id === item.id);
      if (candidate) {
        state.selectedPublicHealthKey = candidate.key;
        renderPublicHealthMatrix();
      }
    });
    publicHealthMap.appendChild(group);
  });

  const legend = svgNode("g", {class: "health-map-legend"});
  if (regionalRows.some((entry) => entry.count > 0)) {
    legend.appendChild(svgNode("rect", {x: 36, y: 476, width: 10, height: 10, rx: 2}));
    appendSvgText(legend, ["cor = layer ativo"], 52, 485, {class: "health-map-legend-text"});
    appendSvgText(legend, ["azul = volume de hipóteses"], 178, 485, {class: "health-map-legend-text"});
  } else {
    appendSvgText(legend, ["Sem regiões mapeadas nesta amostra."], 36, 485, {class: "health-map-legend-text"});
  }
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
  relation.textContent = `${compactTitle(row.item.source_title, 44)} / ${compactTitle(row.item.target_title, 44)}`;
  relation.title = `${row.item.source_title} / ${row.item.target_title}`;
  const scope = document.createElement("p");
  scope.className = "decision-scope";
  scope.textContent = row.geo.id === "nacional"
    ? "Âmbito nacional."
    : row.geo.id === "agregar"
      ? `${publicHealthScopeLabel(row)}: validar território antes de mapear.`
      : row.geo.resolution === "mapped"
        ? `${row.geo.label} · ${row.geo.matchedEntity}.`
        : `${row.geo.label} · match heurístico.`;
  const reason = document.createElement("p");
  reason.className = "meta";
  reason.textContent = shortSentence(row.reason, 110);
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
  validation.textContent = `Validar: ${compactTitle(keys, 72)}.`;
  const geoChecklist = document.createElement("div");
  geoChecklist.className = "geo-validation-note";
  const geoTitle = document.createElement("strong");
  geoTitle.textContent = row.geo.id === "agregar" ? "Como resolver este âmbito" : "Validação territorial";
  const geoText = document.createElement("span");
  geoText.textContent = row.geo.id === "agregar"
    ? "Confirmar valores territoriais e regra Região SNS."
    : row.geo.resolution === "mapped"
      ? "Confirmar entidade, região e período."
      : "Confirmar tipo de território antes de decidir.";
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
    ["agregar", "Por resolver"],
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
      ? "território por resolver"
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
    ? "Leitura nacional; não forçar região."
    : bucket.scope === "agregar"
      ? "Há sinal territorial, mas falta correspondência segura."
      : "Sinal regional inicial; validar período e granularidade.";
  const list = document.createElement("div");
  list.className = "hypothesis-detail-list";
  bucket.rows.slice(0, 6).forEach((row) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = row.key === state.selectedPublicHealthKey ? "is-active" : "";
    const strong = document.createElement("strong");
    strong.textContent = `${compactTitle(row.item.source_title, 34)} / ${compactTitle(row.item.target_title, 34)}`;
    const small = document.createElement("small");
    small.textContent = `${shortSentence(row.reason, 58)} · validar: ${compactTitle((row.item.risk_flags || []).join(", ") || "tipo, nulos, duplicados", 58)}`;
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

const CARE_OPS_TEMPLATES = [
  {
    key: "frailty-hospital-use",
    cohort: "Fragilidade com uso hospitalar recente",
    graIdra: "GRA/IDRA quando disponível",
    profile: "75+; multimorbilidade; polimedicação",
    utilization: "Urgência/internamento recente",
    primaryCare: "CSP em falta; faltas 12m",
    prevention: "Rastreios/social",
    plan: "PIC por rever",
    action: "Contacto + PIC.",
  },
  {
    key: "multimorbidity-low-csp",
    cohort: "Multimorbilidade sem continuidade CSP",
    graIdra: "GRA/IDRA para prioridade",
    profile: "65+; 2+ condições crónicas; 5+ medicamentos",
    utilization: "Urgência evitável/internamento",
    primaryCare: "Sem contacto CSP 12m",
    prevention: "Vacinas/rastreios em atraso",
    plan: "PIC ausente",
    action: "Consulta + revisão terapêutica.",
  },
  {
    key: "prevention-social-vulnerability",
    cohort: "Prevenção em atraso com vulnerabilidade social",
    graIdra: "IDRA social quando existir",
    profile: "Faixa elegível para rastreio; doença crónica ou fragilidade social",
    utilization: "Baixa consulta programada",
    primaryCare: "Faltas/contacto difícil",
    prevention: "Rastreios/social",
    plan: "Plano social por confirmar",
    action: "Recuperar rastreios + apoio social.",
  },
  {
    key: "post-discharge-care-gap",
    cohort: "Alta recente sem plano de continuidade",
    graIdra: "GRA/IDRA elevado/desconhecido",
    profile: "Alta hospitalar recente; idade avançada ou doença crónica complexa",
    utilization: "Internamento/reinternamento",
    primaryCare: "Sem contacto CSP pós-alta",
    prevention: "Medicação/apoio por rever",
    plan: "PIC desatualizado",
    action: "Contacto pós-alta + seguimento.",
  },
];

function careOpsItemText(item) {
  if (!item) return "";
  return normalizeSearchText([
    item.source_title,
    item.target_title,
    item.source_theme,
    item.target_theme,
    ...(item.shared_fields || []),
    ...(item.join_recipe?.suggested_keys || []),
    ...((item.public_health_model || {}).matched_areas || []),
  ].join(" "));
}

function inferCareInstitution(item) {
  const text = careOpsItemText(item);
  if (/usf|ucsp|aces|cuidados de saude primarios|csp/.test(text)) return "UF de inscrição: USF/UCSP";
  if (/uls|unidade local de saude/.test(text)) return "UF/ULS a confirmar";
  if (/hospital|centro hospitalar|internamento|urgencia|urgência/.test(text)) return "UF + hospital de referência";
  if (/ars|regiao|região|distrito|concelho|freguesia/.test(text)) return "UF + território agregado";
  return "UF de inscrição a definir";
}

function careActionBand(template) {
  if (/alta|hospitalar|alta recente|fragilidade/.test(normalizeSearchText(`${template.key} ${template.cohort}`))) return "intervir";
  if (/prevencao|rastreio|vulnerabilidade/.test(normalizeSearchText(`${template.key} ${template.cohort}`))) return "recuperar";
  return "programar";
}

function careViabilityBand(source) {
  if (!source) return "preparar";
  if ((source.viability || 0) >= 12 || source.decision === "Priorizar") return "boa";
  return "preparar";
}

function careOpsRows() {
  const sourceRows = publicHealthPriorityRows().slice(0, CARE_OPS_TEMPLATES.length);
  return CARE_OPS_TEMPLATES.map((template, index) => {
    const source = sourceRows[index] || null;
    const item = source?.item || null;
    const sourceTitle = item
      ? `Pista pública: ${compactTitle([item.source_title, item.target_title].filter(Boolean).join(" × "), 78)}`
      : "Sem fonte autorizada ligada";
    const geoLabel = source?.geo?.label || "freguesia/localização agregada";
    const quality = [
      "base UF obrigatória",
      "período fechado",
      item ? `pista pública: ${source.decision.toLowerCase()}` : "requer fonte clínica validada",
      "sem identificação nesta vista",
      ...((item?.risk_flags || []).slice(0, 2)),
    ].slice(0, 4);
    return {
      ...template,
      key: template.key,
      source,
      sourceTitle,
      institution: inferCareInstitution(item),
      location: geoLabel,
      priority: source?.priority || 0,
      actionBand: careActionBand(template),
      viabilityBand: careViabilityBand(source),
      quality,
    };
  });
}

function renderCareOpsKpis(rows) {
  if (!careOpsKpis) return;
  clearElement(careOpsKpis);
  const linked = rows.filter((row) => row.source).length;
  const kpis = [
    {label: "Regras de coorte", value: rows.length, note: "modelos de trabalho, não resultados"},
    {label: "Pistas no catálogo", value: `${linked}/${rows.length}`, note: "sinais públicos para orientar fontes"},
    {label: "Revisão", value: "15/30 dias", note: "ciclo operacional recomendado"},
    {label: "Dados pessoais", value: "fora desta vista", note: "só em ambiente clínico autorizado"},
  ];
  kpis.forEach((item) => {
    const card = document.createElement("div");
    const label = document.createElement("span");
    label.textContent = item.label;
    const value = document.createElement("strong");
    value.textContent = String(item.value);
    const note = document.createElement("small");
    note.textContent = item.note;
    card.append(label, value, note);
    careOpsKpis.appendChild(card);
  });
}

function careOpsMatrixKey(actionBand, viabilityBand) {
  return `${actionBand}|${viabilityBand}`;
}

function renderCareOpsActionMatrix(rows) {
  if (!careOpsMatrix) return;
  clearElement(careOpsMatrix);
  const actionBands = [
    ["intervir", "Intervenção rápida"],
    ["programar", "Consulta programada"],
    ["recuperar", "Recuperar prevenção"],
  ];
  const viabilityBands = [
    ["boa", "Dados aproveitáveis"],
    ["preparar", "Dados por preparar"],
  ];
  const buckets = new Map();
  rows.forEach((row) => {
    const key = careOpsMatrixKey(row.actionBand, row.viabilityBand);
    const bucket = buckets.get(key) || [];
    bucket.push(row);
    buckets.set(key, bucket);
  });

  const corner = document.createElement("div");
  corner.className = "care-ops-axis";
  corner.textContent = "Tipo / dados";
  careOpsMatrix.appendChild(corner);
  viabilityBands.forEach(([, label]) => {
    const axis = document.createElement("div");
    axis.className = "care-ops-axis";
    axis.textContent = label;
    careOpsMatrix.appendChild(axis);
  });

  actionBands.forEach(([actionKey, actionLabel]) => {
    const axis = document.createElement("div");
    axis.className = "care-ops-axis";
    axis.textContent = actionLabel;
    careOpsMatrix.appendChild(axis);
    viabilityBands.forEach(([viabilityKey, viabilityLabel]) => {
      const key = careOpsMatrixKey(actionKey, viabilityKey);
      const bucket = buckets.get(key) || [];
      const button = document.createElement("button");
      button.type = "button";
      button.className = `care-ops-cell action-${actionKey} viability-${viabilityKey}`.trim();
      if (!bucket.length) button.classList.add("is-empty");
      if (bucket.some((row) => row.key === state.selectedCareOpsKey)) button.classList.add("is-active");
      const strong = document.createElement("strong");
      strong.textContent = bucket.length ? `${bucket.length} regra(s)` : "0";
      const small = document.createElement("small");
      small.textContent = bucket[0]?.cohort || "sem regra nesta célula";
      button.append(strong, small);
      button.addEventListener("click", () => {
        if (!bucket.length) return;
        state.selectedCareOpsKey = bucket[0].key;
        renderCareOpsActionMatrix(rows);
        renderCareOpsTable(rows);
        renderCareOpsWorkflow(rows);
      });
      careOpsMatrix.appendChild(button);
    });
  });
}

function renderCareOpsTable(rows) {
  const tbody = careOpsTable?.querySelector("tbody");
  if (!tbody) return;
  clearElement(tbody);
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.className = row.key === state.selectedCareOpsKey ? "is-selected" : "";
    tr.addEventListener("click", () => {
      state.selectedCareOpsKey = row.key;
      renderCareOpsActionMatrix(rows);
      renderCareOpsTable(rows);
      renderCareOpsWorkflow(rows);
    });

    const addTextCell = (strongText, smallText = "") => {
      const td = document.createElement("td");
      const strong = document.createElement("strong");
      strong.textContent = strongText;
      td.appendChild(strong);
      if (smallText) {
        const small = document.createElement("small");
        small.textContent = smallText;
        td.appendChild(small);
      }
      tr.appendChild(td);
    };

    addTextCell(row.cohort, row.sourceTitle);
    addTextCell(row.institution, row.location);
    addTextCell(row.graIdra);
    addTextCell(row.profile);
    addTextCell(row.utilization);
    addTextCell(row.primaryCare);
    addTextCell(row.prevention);
    addTextCell(row.plan, row.action);

    const quality = document.createElement("td");
    row.quality.forEach((flag) => {
      const pill = document.createElement("span");
      pill.className = "care-ops-pill";
      pill.textContent = flag;
      quality.appendChild(pill);
    });
    tr.appendChild(quality);
    tbody.appendChild(tr);
  });
}

function renderCareOpsWorkflow(rows) {
  if (!careOpsWorkflow) return;
  clearElement(careOpsWorkflow);
  const selected = rows.find((row) => row.key === state.selectedCareOpsKey) || rows[0];
  if (!selected) return;
  const steps = [
    ["Definir coorte", "Aplicar a regra apenas a dados autorizados e confirmar a base populacional da UF."],
    ["Validar dados", "Rever denominador, período, duplicados, missing values, geografia e falsos positivos."],
    ["Intervir", selected.action],
    ["Auditar ciclo", "Registar motivo de consulta, ação tomada, resultado, PIC e próxima revisão."],
  ];
  const headline = document.createElement("div");
  headline.className = "care-ops-selected";
  const title = document.createElement("strong");
  title.textContent = selected.cohort;
  const meta = document.createElement("span");
  meta.textContent = `${selected.institution} · ${selected.location} · sem identificação nesta vista`;
  headline.append(title, meta);
  careOpsWorkflow.appendChild(headline);

  steps.forEach(([label, text], index) => {
    const step = document.createElement("div");
    step.className = "care-ops-step";
    const marker = document.createElement("b");
    marker.textContent = String(index + 1);
    const body = document.createElement("span");
    const strong = document.createElement("strong");
    strong.textContent = label;
    const copy = document.createElement("small");
    copy.textContent = text;
    body.append(strong, copy);
    step.append(marker, body);
    careOpsWorkflow.appendChild(step);
  });
}

function renderCareOpsGuardrails() {
  if (!careOpsGuardrails) return;
  clearElement(careOpsGuardrails);
  const items = [
    ["Sem simulação", "Não preencher com utentes fictícios nem contagens inventadas; a página fica como desenho até haver fonte autorizada."],
    ["Campos mínimos", "UF, localização agregada, GRA/IDRA, faixa etária, multimorbilidade, polimedicação, utilização, rastreios e PIC."],
    ["Base populacional", "A UF, ULS ou região precisa de inscritos ativos à data de referência para calcular taxas."],
    ["Janelas", "Urgências 6m, internamentos 12m, reinternamentos 30d, faltas 12m e rastreios por elegibilidade."],
    ["Granularidade", "Não misturar freguesia, ULS, hospital e região sem regra de agregação."],
    ["Qualidade", "Bloquear leitura com episódios duplicados, datas em falta, unidade incerta ou atualização vencida."],
    ["Acesso", "Perfis mínimos: equipa UF, coordenação local e auditoria; sem exportação aberta por defeito."],
    ["Registo", "Guardar quem consultou, quando, por que motivo, coorte vista e ação tomada."],
    ["Minimização", "Vista agregada por coorte; identificação só em ambiente clínico autorizado."],
    ["Equidade", "Auditar enviesamento por idade, vulnerabilidade social, território e acesso digital."],
    ["Responsabilidade", "Cada linha operacional deve ter dono, prazo, estado e resultado registado."],
  ];
  items.forEach(([label, text]) => {
    const card = document.createElement("div");
    const strong = document.createElement("strong");
    strong.textContent = label;
    const span = document.createElement("span");
    span.textContent = text;
    card.append(strong, span);
    careOpsGuardrails.appendChild(card);
  });
}

function renderCareOps() {
  if (!careOpsMeta) return;
  const rows = careOpsRows();
  if (!state.selectedCareOpsKey && rows.length) {
    state.selectedCareOpsKey = rows[0].key;
  }
  const linked = rows.filter((row) => row.source).length;
  careOpsMeta.textContent = `Blueprint operacional: ${rows.length} regras de coorte para desenhar a operação; ${linked} têm pista pública no catálogo. Não há dados individuais, contagens reais ou score clínico nesta página.`;
  const selected = rows.find((row) => row.key === state.selectedCareOpsKey) || rows[0];
  const readyRows = rows.filter((row) => row.viabilityBand === "boa").length;
  const actionLabel = {
    intervir: "Intervenção rápida",
    programar: "Consulta programada",
    recuperar: "Recuperar prevenção",
  }[selected?.actionBand] || "Ação por definir";
  const viabilityLabel = {
    boa: "dados aproveitáveis",
    preparar: "dados por preparar",
  }[selected?.viabilityBand] || "dados por validar";
  renderAnalyticsStory(careOpsStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "Que coorte pode virar ação?",
      detail: `${formatNumber(rows.length)} regras operacionais; ${formatNumber(linked)} ligadas a pistas públicas do catálogo.`,
    },
    {
      label: "Leitura recomendada",
      value: selected ? compactTitle(selected.cohort || "Coorte selecionada", 46) : "Sem coorte",
      detail: selected ? `${actionLabel} · ${viabilityLabel}.` : "Seleciona uma célula da matriz para ver o fluxo de intervenção.",
      tone: selected?.viabilityBand === "boa" ? "ok" : "warning",
    },
    {
      label: "Travão",
      value: "Sem dados individuais",
      detail: "Esta aba desenha operação e campos mínimos; não calcula risco clínico nem lista utentes.",
      tone: "warning",
    },
    {
      label: "Próxima ação",
      value: readyRows ? "Definir owner" : "Validar base",
      detail: readyRows ? "Atribuir dono, prazo e auditoria antes de produção." : "Confirmar denominador, período e permissões antes de operacionalizar.",
    },
  ]);
  renderCareOpsKpis(rows);
  renderCareOpsActionMatrix(rows);
  renderCareOpsTable(rows);
  renderCareOpsWorkflow(rows);
  renderCareOpsGuardrails();
}

function renderLocalPlanKpis(rows) {
  clearElement(localPlanKpis);
  const ready = rows.filter((row) => row.priorityBand === "Pronto para plano").length;
  const needsData = rows.filter((row) => row.priorityBand === "Necessita dados").length;
  const regional = rows.filter((row) => row.readiness === "regional").length;
  const surveillance = rows.filter((row) => row.surveillanceReadiness >= 55).length;
  [
    ["Pronto para discutir", formatNumber(ready)],
    ["Precisa de dados", formatNumber(needsData)],
    ["Com território", `${formatNumber(regional)}/${formatNumber(rows.length)}`],
    ["Com eixo temporal", formatNumber(surveillance)],
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
    ? "Travão: a maioria das hipóteses ainda não tem território classificável para leitura local."
    : "Há cobertura territorial suficiente para preparar discussão local exploratória.";
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
    ["pronta", "Base suficiente"],
    ["limitada", "Base por fechar"],
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
  corner.textContent = "Necessidade / dados";
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
  title.textContent = `${formatNumber(bucket.rows.length)} hipóteses para discutir`;
  const explanation = document.createElement("p");
  explanation.className = "decision-scope";
  explanation.textContent = bucket.capacityBand === "pronta"
    ? "Pode alimentar uma conversa local exploratória: há sinal suficiente para preparar validações com equipas locais."
    : "Há necessidade potencial, mas falta base analítica: fechar território, período, chave de join ou qualidade antes de usar no plano.";
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
    : "Vigilância só deve avançar quando houver campo temporal confirmado em dados reais; aqui é uma triagem.";
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
    ["alta", "Viabilidade alta"],
    ["media", "Viabilidade média"],
    ["baixa", "Viabilidade baixa"],
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
  corner.textContent = "Impacto / viabilidade";
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
      small.textContent = bucket ? `incerteza residual ${formatDecimal(bucket.residual / bucket.rows.length, 0)}` : "sem sinal";
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
    ["Perguntas para reunião local", [...new Set(questions)].slice(0, 5)],
    ["Validações antes de priorizar", validations],
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
  localPlanMeta.textContent = `Triagem exploratória: ${formatNumber(rows.length)} hipóteses filtradas; ${formatNumber(regional)} têm território; ${formatNumber(national)} ainda não servem para leitura local.`;
  const selectedBucket = state.selectedLocalPriorityCell
    ? rows.filter((row) => {
      const needBand = row.needScore >= 72 ? "alta" : row.needScore >= 48 ? "media" : "baixa";
      const capacityBand = row.analysisCapacity >= 58 ? "pronta" : "limitada";
      return localCellKey(needBand, capacityBand) === state.selectedLocalPriorityCell;
    })
    : [];
  const selected = selectedBucket[0] || rows[0];
  renderAnalyticsStory(localPlanStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "Que prioridade local merece plano?",
      detail: `${formatNumber(rows.length)} hipóteses; ${formatNumber(regional)} com território utilizável.`,
    },
    {
      label: "Leitura recomendada",
      value: selected ? compactTitle([selected.item.source_title, selected.item.target_title].filter(Boolean).join(" / "), 48) : "Sem prioridade",
      detail: selected ? `${selected.priorityBand} · necessidade ${formatDecimal(selected.needScore, 0)} · dados ${formatDecimal(selected.analysisCapacity, 0)}.` : "Sem hipóteses suficientes para plano local.",
      tone: selected?.priorityBand === "Pronto para plano" ? "ok" : "warning",
    },
    {
      label: "Travão",
      value: national ? "Território por resolver" : "Território validado",
      detail: national ? `${formatNumber(national)} sinais ainda não devem ser lidos como prioridade local.` : "Ainda assim, confirmar denominador, período e geografia antes de decidir.",
      tone: national ? "warning" : "ok",
    },
    {
      label: "Próxima ação",
      value: selected?.readiness === "regional" ? "Discutir intervenção" : "Validar território",
      detail: selected?.readiness === "regional" ? "Usar a matriz para priorizar owner, prazo e métrica de acompanhamento." : "Fechar regra territorial antes de passar para plano.",
    },
  ]);
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
  publicHealthMeta.textContent = `${formatNumber(correlations.length)} hipóteses filtradas · ${formatNumber(priorityRows.filter((row) => row.decision === "Priorizar").length)} com impacto+viabilidade altos · risco/incerteza como travão · layer: ${PUBLIC_HEALTH_LAYERS.find(([key]) => key === state.activePublicHealthLayer)?.[1] || "Impacto"}.`;
  const selectedPriority = priorityRows.find((row) => row.key === state.selectedPublicHealthKey) || priorityRows[0];
  const mappedCount = priorityRows.filter((row) => row.geo.resolution === "mapped").length;
  const unresolvedCount = priorityRows.filter((row) => row.geo.id === "agregar").length;
  renderAnalyticsStory(publicHealthStoryStrip, [
    {
      label: "Pergunta ativa",
      value: "Que sinais merecem acompanhamento?",
      detail: `${formatNumber(priorityRows.length)} hipóteses; ${formatNumber(priorityRows.filter((row) => row.decision === "Priorizar").length)} para priorizar por impacto e viabilidade.`,
    },
    {
      label: "Leitura recomendada",
      value: selectedPriority ? compactTitle(selectedPriority.decision, 40) : "Sem hipótese",
      detail: selectedPriority ? `${compactTitle(selectedPriority.item.source_title, 48)} / ${compactTitle(selectedPriority.item.target_title, 48)}.` : "Alarga filtros ou baixa score mínimo.",
      tone: selectedPriority?.decision === "Priorizar" ? "ok" : "warning",
    },
    {
      label: "Travão",
      value: mappedCount ? `${formatNumber(mappedCount)} mapeadas` : "Território por validar",
      detail: unresolvedCount ? `${formatNumber(unresolvedCount)} hipóteses têm campo territorial mas falta regra regional.` : "Território regional disponível para os sinais visíveis.",
      tone: unresolvedCount ? "warning" : "ok",
    },
    {
      label: "Próxima ação",
      value: "Acompanhar drivers",
      detail: "Abrir a hipótese líder e confirmar fonte, período, entidade e denominador.",
    },
  ]);
  renderPublicHealthCockpit(priorityRows);

  const likelihoodLevels = ["alta", "media", "baixa"];
  const impactLevels = ["baixo", "medio", "alto"];
  const maxCount = Math.max(...[...cells.values()].map((cell) => cell.count), 1);

  const corner = document.createElement("div");
  corner.className = "public-health-axis public-health-corner";
  corner.textContent = "Viabilidade ↓ / impacto →";
  publicHealthMatrix.appendChild(corner);
  impactLevels.forEach((impact) => {
    const axis = document.createElement("div");
    axis.className = "public-health-axis";
    axis.textContent = `Impacto ${impactLabel(impact).toLowerCase()}`;
    publicHealthMatrix.appendChild(axis);
  });

  likelihoodLevels.forEach((likelihood) => {
    const axis = document.createElement("div");
    axis.className = "public-health-axis";
    axis.textContent = viabilityLabel(likelihood);
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
  titleLabel.textContent = `${viabilityLabel(likelihood)} × ${impactLabel(impact)} impacto`;
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

  if (!rows.length) {
    const emptyRow = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 7;
    const empty = document.createElement("div");
    empty.className = "empty-state";
    const message = document.createElement("span");
    message.textContent = "Sem hipóteses para estes filtros.";
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "ghost-button";
    clear.textContent = "Limpar filtros";
    clear.addEventListener("click", clearSemanticFilters);
    empty.append(message, clear);
    cell.appendChild(empty);
    emptyRow.appendChild(cell);
    tbody.appendChild(emptyRow);
    return;
  }

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
    publicHealthChip.textContent = `${viabilityLabel(publicHealthModel.likelihood?.level || "baixa")} × ${impactLabel(publicHealthModel.impact?.level || "baixo")} impacto`;
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
    risks.textContent = (item.risk_flags || []).join(", ") || "sem flags detetadas";

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
  if (state.activeTab === "care") {
    renderSummary();
    renderCareOps();
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
  if (["data", "finprod", "predictive", "semantic", "health", "care", "local", "tables"].includes(initialTab)) {
    state.activeTab = initialTab;
  }
  setupTabAccessibility();
  renderDatasetMode();

  analyticsTabs.forEach((button) => {
    button.addEventListener("click", () => setActiveTab(button.dataset.analyticsTab));
    button.addEventListener("keydown", (event) => {
      const currentIndex = analyticsTabs.indexOf(button);
      let nextIndex = null;
      if (event.key === "ArrowRight") nextIndex = (currentIndex + 1) % analyticsTabs.length;
      if (event.key === "ArrowLeft") nextIndex = (currentIndex - 1 + analyticsTabs.length) % analyticsTabs.length;
      if (event.key === "Home") nextIndex = 0;
      if (event.key === "End") nextIndex = analyticsTabs.length - 1;
      if (nextIndex === null) return;
      event.preventDefault();
      const nextButton = analyticsTabs[nextIndex];
      setActiveTab(nextButton.dataset.analyticsTab);
      nextButton.focus();
    });
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
    activeFinProdRequest += 1;
    activeFinProdRecommendationRequest += 1;
    state.selectedFinancialDataset = finProdFinancialDataset.value;
    state.finProdPayload = null;
    state.finProdRecommendationPayload = null;
    renderFinProdAnalytics();
    loadFinProdRecommendations().catch(() => {
      renderFinProdRecommendations({recommendations: [], warning: "Não foi possível calcular compatibilidade para este financeiro."});
    });
  });
  finProdProductionDataset?.addEventListener("change", () => {
    activeFinProdRequest += 1;
    state.selectedProductionDataset = finProdProductionDataset.value;
    state.finProdPayload = null;
    renderFinProdAnalytics();
    renderFinProdRecommendations(state.finProdRecommendationPayload || {recommendations: []});
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

  analyticsLocalScope?.addEventListener("change", () => {
    state.localScope = analyticsLocalScope.value;
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
    state.predictiveRecommendationPayload = null;
    state._predictiveRecommendationKey = "";
    activePredictiveRecommendationRequest += 1;
    renderDataAnalytics();
    loadDataAnalytics().catch(showDataError);
  });

  dataSampleLimit.addEventListener("input", () => {
    state.dataLimit = Number(dataSampleLimit.value);
    dataSampleLimitValue.textContent = state.dataLimit;
    state.predictiveRecommendationPayload = null;
    state._predictiveRecommendationKey = "";
    activePredictiveRecommendationRequest += 1;
    debounceLoadDataAnalytics();
  });

  dataRefreshButton.addEventListener("click", () => {
    loadDataAnalytics().catch(showDataError);
  });

  dataRichMode?.addEventListener("click", () => {
    state.datasetMode = "rich";
    state.dataPayload = null;
    state.predictiveRecommendationPayload = null;
    state._predictiveRecommendationKey = "";
    activePredictiveRecommendationRequest += 1;
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
      methodology_version: state.payload?.methodology?.version || "semantic-links-v1",
      generated_at: state.payload?.generated_at,
      exported_at: new Date().toISOString(),
      active_tab: state.activeTab,
      filters: {
        min_score: state.minScore,
        search: state.search,
        dimension: state.kind,
        local_scope: state.localScope,
        confidence: state.confidence,
        low_risk_only: state.lowRiskOnly,
      },
      context: {
        selected_data_dataset: state.selectedDataDataset,
        selected_financial_dataset: state.selectedFinancialDataset,
        selected_production_dataset: state.selectedProductionDataset,
        sample_limit: state.dataLimit,
        data_quality_warnings: state.dataPayload?.quality_warnings || [],
        finprod_warnings: state.finProdPayload?.diagnostics?.warnings || [],
      },
      link_hypotheses: state.filteredCorrelations,
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
