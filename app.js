const state = {
  datasets: [],
  links: [],
  opportunities: [],
  selectedDataset: null,
  activeTheme: "",
  localScope: "",
  institutionType: "",
  dimensionType: "",
  minScore: 4,
  filterText: "",
  themes: [],
  recentData: null,
  datasetById: new Map(),
  degreeById: new Map(),
  analysisAt: null,
  _viewState: null,
  _viewStamp: 0,
  _selectionStamp: 0,
  _graphState: null,
  _titleCache: new Map(),
  _searchTextCache: new Map(),
  _datasetMetaCache: new Map(),
  _recentCache: new Map(),
  datasetRenderLimit: 160,
};

const TITLE_CACHE_SIZE = 700;
const SEARCH_TEXT_CACHE_SIZE = 700;
const DATASET_METADATA_CACHE_SIZE = 40;
const RECENT_DATA_CACHE_SIZE = 24;
const META_DATA_CACHE_TTL_MS = 12 * 60 * 60 * 1000;
const RECENT_DATA_CACHE_TTL_MS = 2.5 * 60 * 1000;
const DATASET_RENDER_STEP = 160;

const THEME_COLORS = {
  "Acesso & Produção": "#2f6fdb",
  "Recursos Humanos": "#0a7a58",
  "Finanças & Compras": "#a95a06",
  "Qualidade & Resultados": "#6b2fa3",
  "Saúde Pública & Emergência": "#d1434e",
  "Rede & Território": "#2d8ab7",
  Outros: "#6d7787",
};

const FALLBACK_ANALYSIS = {
  datasets: [],
  links: [],
  opportunities: [],
  themes: [],
};

function getThemeColor(theme) {
  return THEME_COLORS[theme] || THEME_COLORS.Outros;
}

const graphEl = document.getElementById("linkGraph");
const datasetSummary = document.getElementById("datasetSummary");
const datasetList = document.getElementById("datasetList");
const linkTree = document.getElementById("linkTree");
const selectedModel = document.getElementById("selectedModel");
const opportunitiesSection = document.getElementById("opportunitiesSection");
const minScoreInput = document.getElementById("minScore");
const minScoreValue = document.getElementById("minScoreValue");
const datasetFilter = document.getElementById("datasetFilter");
const syncState = document.getElementById("syncState");
const graphScope = document.getElementById("graphScope");
const datasetCountLabel = document.getElementById("datasetCountLabel");
const statDatasets = document.getElementById("statDatasets");
const statLinks = document.getElementById("statLinks");
const statFields = document.getElementById("statFields");
const statFocus = document.getElementById("statFocus");
const themeCards = document.getElementById("themeCards");
const catalogCockpit = document.getElementById("catalogCockpit");
const themeFilter = document.getElementById("themeFilter");
const localScopeFilter = document.getElementById("localScopeFilter");
const institutionFilter = document.getElementById("institutionFilter");
const dimensionFilter = document.getElementById("dimensionFilter");
const recentDataMeta = document.getElementById("recentDataMeta");
const recentDataTable = document.getElementById("recentDataTable");
const recentDataEmpty = document.getElementById("recentDataEmpty");
const reloadRecordsButton = document.getElementById("reloadRecordsButton");
const quickAnalysisMeta = document.getElementById("quickAnalysisMeta");
const quickAnalysisKpis = document.getElementById("quickAnalysisKpis");
const quickKeyVariables = document.getElementById("quickKeyVariables");
const quickTrendChart = document.getElementById("quickTrendChart");
const quickDistributionChart = document.getElementById("quickDistributionChart");
const quickAnomalyList = document.getElementById("quickAnomalyList");

const tooltip = document.createElement("div");
tooltip.className = "graph-tooltip";
document.body.appendChild(tooltip);

let analysisLoadTimer = null;
let filterTimer = null;
let resizeTimer = null;

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

function normalizeSearch(value) {
  return safeText(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function datasetFacets(dataset) {
  return dataset?.facets || {};
}

function hasFacetValue(dataset, group, value) {
  if (!value) return true;
  const values = datasetFacets(dataset)[group] || [];
  if (value === "none") return !values.length;
  return values.includes(value);
}

async function fetchJson(url, {timeoutMs = 18000} = {}) {
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

function clearElement(element) {
  element.replaceChildren();
}

function setBoundedCache(map, key, value, maxSize) {
  if (map.has(key)) {
    map.delete(key);
  }
  map.set(key, value);
  while (map.size > maxSize) {
    const oldest = map.keys().next().value;
    map.delete(oldest);
  }
}

function createShowMoreButton(text, onClick) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "show-more-button";
  button.textContent = text;
  button.addEventListener("click", onClick);
  return button;
}

function createDisclosure(summaryText, nodes = [], className = "inline-disclosure") {
  const details = document.createElement("details");
  details.className = className;
  const summary = document.createElement("summary");
  summary.textContent = summaryText;
  details.appendChild(summary);
  nodes.forEach((node) => details.appendChild(node));
  return details;
}

function createChip(text, className = "tag") {
  const chip = document.createElement("span");
  chip.className = className;
  chip.textContent = safeText(text);
  return chip;
}

function svgNode(tag, attributes = {}) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  return node;
}

function invalidateViewState() {
  state._viewState = null;
  state._viewStamp += 1;
  state._graphState = null;
}

function touchSelectionState(nextDataset) {
  if (state.selectedDataset === nextDataset) {
    return false;
  }
  state.selectedDataset = nextDataset;
  state._selectionStamp += 1;
  state._graphState = null;
  return true;
}

function getViewState() {
  if (state._viewState) {
    return state._viewState;
  }

  const value = normalizeSearch(state.filterText).trim();
  const datasets = [];
  for (const item of state.datasets) {
    if (state.activeTheme && item.mega_theme !== state.activeTheme) {
      continue;
    }
    if (!hasFacetValue(item, "local_scopes", state.localScope)) continue;
    if (!hasFacetValue(item, "institution_types", state.institutionType)) continue;
    if (!hasFacetValue(item, "dimension_types", state.dimensionType)) continue;
    if (value) {
      if (!matchesExpandedSearch(item, value)) {
        continue;
      }
    }
    datasets.push(item);
  }

  datasets.sort((a, b) => {
    const degreeDelta = (state.degreeById.get(b.dataset_id) || 0) - (state.degreeById.get(a.dataset_id) || 0);
    if (degreeDelta !== 0) return degreeDelta;
    return displayTitle(a).localeCompare(displayTitle(b));
  });

  const visibleDatasetIds = new Set(datasets.map((item) => item.dataset_id));
  const links = state.links.filter((edge) => visibleDatasetIds.has(edge.source) && visibleDatasetIds.has(edge.target));
  state._viewState = {datasets, links, visibleDatasetIds};
  return state._viewState;
}

function debounceLoadAnalysis() {
  if (analysisLoadTimer) clearTimeout(analysisLoadTimer);
  analysisLoadTimer = setTimeout(() => {
    loadAnalysis().catch(showError);
  }, 180);
}

function debounceRender() {
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    if (state.datasets.length) {
      renderAll();
    }
  }, 120);
}

function fallbackAnalysisPayload(error) {
  const payload = structuredClone(FALLBACK_ANALYSIS);
  payload.links = payload.links.filter((link) => link.score >= state.minScore);
  payload.total = payload.datasets.length;
  payload.link_count = payload.links.length;
  payload.generated_at = Math.floor(Date.now() / 1000);
  payload.fallback = true;
  payload.error_message = error?.message || "API indisponível";
  payload.error_kind = "client_fetch_error";
  payload.warning = `API indisponível: ${payload.error_message}. Sem dados fictícios carregados.`;
  payload.empty_reason = "api_unavailable";
  return payload;
}

function fallbackStatusText(payload) {
  const warning = payload?.warning || payload?.error_message || "falha desconhecida";
  if (/pedido|parâmetro|parametro|configura/i.test(warning)) return "API indisponível · rever configuração";
  if (/indispon|sem resposta|fetch|timeout|network/i.test(warning)) return "API indisponível · sem dados fictícios";
  return "API indisponível · ver diagnóstico";
}

async function loadAnalysis() {
  syncState.textContent = "A sincronizar…";
  const url = `/api/analysis?min_score=${state.minScore}`;
  let payload;
  try {
    payload = await fetchJson(url, {timeoutMs: 24000});
  } catch (error) {
    payload = fallbackAnalysisPayload(error);
  }
  state.datasets = payload.datasets || [];
  state.links = payload.links || [];
  state.opportunities = payload.opportunities || [];
  state.themes = payload.themes || [];
  state.analysisAt = payload.generated_at;
  state.datasetById = new Map(state.datasets.map((item) => [item.dataset_id, item]));
  state.degreeById = buildDegreeMap(state.links);
  state._titleCache = new Map();
  state._searchTextCache = new Map();
  const validDatasetIds = new Set(state.datasets.map((dataset) => dataset.dataset_id));
  for (const key of state._datasetMetaCache.keys()) {
    if (!validDatasetIds.has(key)) {
      state._datasetMetaCache.delete(key);
    }
  }
  for (const key of state._recentCache.keys()) {
    if (!validDatasetIds.has(key)) {
      state._recentCache.delete(key);
    }
  }
  state._graphState = null;
  state._viewStamp = 0;
  state._selectionStamp = 0;
  state.datasetRenderLimit = DATASET_RENDER_STEP;
  invalidateViewState();
  if (!state.selectedDataset || !state.datasetById.has(state.selectedDataset)) {
    touchSelectionState(filteredDatasets()[0]?.dataset_id || null);
    state.recentData = null;
  }
  syncState.textContent = payload.fallback
    ? fallbackStatusText(payload)
    : `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
  syncState.title = payload.fallback ? (payload.warning || payload.error_message || "") : "";
  renderAll();
  if (state.selectedDataset) {
    loadRecentRecords(state.selectedDataset).catch(showRecordsError);
  }
}

function buildDegreeMap(links) {
  const map = new Map();
  links.forEach((link) => {
    map.set(link.source, (map.get(link.source) || 0) + 1);
    map.set(link.target, (map.get(link.target) || 0) + 1);
  });
  return map;
}

function displayTitle(dataset) {
  const datasetId = dataset?.dataset_id || "";
  if (state._titleCache.has(datasetId)) {
    return state._titleCache.get(datasetId);
  }

  const value = dataset?.title ? safeText(dataset.title) : datasetId;
  const title = value
    .replace(/-/g, " ")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  setBoundedCache(state._titleCache, datasetId, title, TITLE_CACHE_SIZE);
  return title;
}

function getDatasetSearchText(dataset) {
  const datasetId = dataset?.dataset_id || "";
  if (state._searchTextCache.has(datasetId)) {
    return state._searchTextCache.get(datasetId);
  }
  const facets = datasetFacets(dataset);
  const text = normalizeSearch([
    datasetId,
    displayTitle(dataset),
    dataset?.mega_theme,
    ...(dataset?.fields || []),
    ...(facets.tags || []),
    ...(facets.labels || []),
    ...(facets.local_scopes || []),
    ...(facets.institution_types || []),
    ...(facets.dimension_types || []),
    ...(dataset?.quality_flags || []),
  ].join(" "));
  setBoundedCache(state._searchTextCache, datasetId, text, SEARCH_TEXT_CACHE_SIZE);
  return text;
}

const SEARCH_SYNONYM_GROUPS = [
  ["icpc", "problema", "problemas", "diagnostico", "diagnosticos", "morbilidade"],
  ["utente", "utentes", "inscrito", "inscritos", "populacao", "populacao inscrita"],
  ["uf", "unidade funcional", "usf", "ucsp", "aces", "csp", "cuidados primarios"],
  ["uls", "unidade local de saude"],
  ["regiao", "regiao ars", "ars", "norte", "centro", "lisboa", "alentejo", "algarve"],
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

function matchesExpandedSearch(dataset, rawSearch) {
  const tokens = normalizeSearch(rawSearch).split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;
  const text = getDatasetSearchText(dataset);
  return tokens.every((token) => searchAlternatives(token).some((candidate) => text.includes(candidate)));
}

function compactTitle(value, max = 54) {
  if (!value) return "";
  return value.length > max ? `${value.slice(0, max - 1)}...` : value;
}

function readiness(dataset) {
  return dataset?.analysis_readiness || {score: 0, band: "fragil", label: "Frágil", gaps: ["validar"]};
}

function readinessChip(dataset, compact = false) {
  const info = readiness(dataset);
  const chip = document.createElement("span");
  chip.className = `quality-chip quality-${info.band || "fragil"}`;
  const gaps = (info.gaps || []).slice(0, 2).join(", ");
  chip.textContent = compact ? `${info.label || "Rever"} ${info.score ?? 0}` : `${info.label || "Rever"} · ${info.score ?? 0}`;
  chip.title = gaps ? `Validar: ${gaps}` : "Validar fonte e denominadores";
  return chip;
}

function ensureSelectionInFilter() {
  const visible = filteredDatasets();
  if (!state.selectedDataset || visible.some((item) => item.dataset_id === state.selectedDataset)) {
    return false;
  }

  const fallback = visible[0]?.dataset_id || null;
  if (fallback === state.selectedDataset) return false;

  touchSelectionState(fallback);
  state.recentData = null;
  if (fallback) {
    loadRecentRecords(fallback).catch(showRecordsError);
  }
  return true;
}

function filteredDatasets() {
  return getViewState().datasets;
}

function visibleLinks() {
  return getViewState().links;
}

function renderThemeOptions() {
  const current = themeFilter.value;
  clearElement(themeFilter);
  const allOption = document.createElement("option");
  allOption.value = "";
  allOption.textContent = "Todos";
  themeFilter.appendChild(allOption);
  state.themes.forEach((theme) => {
    const option = document.createElement("option");
    option.value = theme.theme;
    option.textContent = `${theme.theme} (${theme.dataset_count})`;
    themeFilter.appendChild(option);
  });
  themeFilter.value = current || state.activeTheme;
}

function renderThemeCards() {
  clearElement(themeCards);
  const allCard = {
    theme: "Todos",
    dataset_count: state.datasets.length,
    link_score: state.links.reduce((acc, link) => acc + link.score, 0),
    description: "Visão global do catálogo e das ligações mais fortes para arrancar uma exploração transversal.",
    value: "",
  };
  [allCard, ...state.themes].forEach((theme) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "theme-card";
    if ((theme.value ?? theme.theme) === state.activeTheme || (!state.activeTheme && theme.theme === "Todos")) {
      button.classList.add("active");
    }
    const title = document.createElement("span");
    title.textContent = safeText(theme.theme);

    const count = document.createElement("strong");
    count.textContent = `${formatNumber(theme.dataset_count)} datasets`;

    const description = document.createElement("small");
    description.textContent = safeText(theme.description || "Datasets com classificação automática.");

    button.append(title, count, description);
    button.onclick = () => setActiveTheme(theme.value ?? theme.theme);
    themeCards.appendChild(button);
  });
}

function countDatasetsByFacet(datasets, group, value) {
  return datasets.filter((dataset) => (datasetFacets(dataset)[group] || []).includes(value)).length;
}

function hasClinicalSignal(dataset) {
  return matchesExpandedSearch(dataset, "icpc") || matchesExpandedSearch(dataset, "fragilidade") || matchesExpandedSearch(dataset, "rastreios");
}

function setQuickCatalogFilter({search = "", localScope = "", institutionType = "", dimensionType = ""} = {}) {
  state.filterText = search;
  state.localScope = localScope;
  state.institutionType = institutionType;
  state.dimensionType = dimensionType;
  datasetFilter.value = search;
  if (localScopeFilter) localScopeFilter.value = localScope;
  if (institutionFilter) institutionFilter.value = institutionType;
  if (dimensionFilter) dimensionFilter.value = dimensionType;
  state.datasetRenderLimit = DATASET_RENDER_STEP;
  invalidateViewState();
  ensureSelectionInFilter();
  renderAll();
}

function renderCatalogCockpit() {
  if (!catalogCockpit) return;
  clearElement(catalogCockpit);
  const visible = filteredDatasets();
  const ready = visible.filter((dataset) => ["pronto", "rever"].includes(readiness(dataset).band)).length;
  const local = visible.filter((dataset) => (datasetFacets(dataset).local_scopes || []).length).length;
  const uf = countDatasetsByFacet(visible, "local_scopes", "uf");
  const uls = countDatasetsByFacet(visible, "local_scopes", "uls");
  const clinical = visible.filter(hasClinicalSignal).length;
  const fallback = state.datasets.length === 0;
  const cards = fallback
    ? [
        {
          label: "API",
          value: "sem catálogo",
          detail: "Sem dados fictícios. Atualiza quando a API SNS responder.",
          action: "Atualizar",
          onClick: () => loadAnalysis().catch(showError),
        },
      ]
    : [
        {
          label: "Prontos para triagem",
          value: `${formatNumber(ready)}/${formatNumber(visible.length)}`,
          detail: "Com metadados suficientes para abrir amostra e validações.",
          action: "Ver prontos",
          onClick: () => setQuickCatalogFilter({dimensionType: ""}),
        },
        {
          label: "Localização",
          value: `${formatNumber(local)} datasets`,
          detail: `ULS ${formatNumber(uls)} · UF/CSP ${formatNumber(uf)} · região/concelho/hospital quando existir.`,
          action: "Focar ULS/UF",
          onClick: () => setQuickCatalogFilter({localScope: uf ? "uf" : "uls"}),
        },
        {
          label: "Clínico/CSP",
          value: `${formatNumber(clinical)} sinais`,
          detail: "ICPC, problemas, utentes, fragilidade, rastreios ou continuidade CSP nos metadados.",
          action: "Pesquisar clínico",
          onClick: () => setQuickCatalogFilter({search: "icpc problemas utentes"}),
        },
        {
          label: "Ação seguinte",
          value: visible.length ? "analisar" : "sem resultados",
          detail: visible.length ? "Seleciona dataset, abre Analytics ou limpa filtros se a lista ficou estreita." : "Os filtros atuais não devolvem datasets.",
          action: visible.length ? "Limpar filtros" : "Limpar filtros",
          onClick: clearCatalogFilters,
        },
      ];
  cards.forEach((card) => {
    const article = document.createElement("article");
    article.className = "cockpit-card";
    const label = document.createElement("span");
    label.textContent = card.label;
    const value = document.createElement("strong");
    value.textContent = card.value;
    const detail = document.createElement("small");
    detail.textContent = card.detail;
    const button = document.createElement("button");
    button.type = "button";
    button.className = "ghost-button";
    button.textContent = card.action;
    button.addEventListener("click", card.onClick);
    article.append(label, value, button, createDisclosure("detalhe", [detail], "compact-disclosure"));
    catalogCockpit.appendChild(article);
  });
}

function renderSummary() {
  const visible = filteredDatasets();
  const totalFields = visible.reduce((acc, item) => acc + (item.field_count || 0), 0);
  const avg = visible.length ? (totalFields / visible.length).toFixed(1) : 0;
  const links = visibleLinks();

  statDatasets.textContent = `${visible.length}/${state.datasets.length}`;
  statLinks.textContent = links.length.toLocaleString("pt-PT");
  statFields.textContent = avg;
  statFocus.textContent = state.selectedDataset
    ? compactTitle(displayTitle(state.datasetById.get(state.selectedDataset)), 24)
    : (state.activeTheme || "Catálogo");

  datasetCountLabel.textContent = `${visible.length} visíveis`;
  const fallbackNote = state.datasets.length ? "" : "Sem catálogo carregado: verifica a API e atualiza. ";
  datasetSummary.textContent = fallbackNote + (state.selectedDataset
    ? "Ordenado por relevância face ao dataset em foco."
    : "Ordenado por número de ligações encontradas.");
}

function renderDatasetList() {
  const visible = filteredDatasets();
  const selected = state.selectedDataset;
  clearElement(datasetList);

  if (!visible.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    const title = document.createElement("strong");
    title.textContent = state.datasets.length ? "Sem datasets para estes filtros" : "Catálogo indisponível";
    const detail = document.createElement("span");
    detail.textContent = state.datasets.length
      ? "Ajusta pesquisa, âmbito local ou dimensão."
      : "A API não carregou dados; não há fallback fictício.";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "ghost-button";
    button.textContent = state.datasets.length ? "Limpar filtros" : "Atualizar";
    button.addEventListener("click", state.datasets.length ? clearCatalogFilters : () => loadAnalysis().catch(showError));
    empty.append(title, detail, button);
    datasetList.appendChild(empty);
    return;
  }

  const selectedIndex = selected ? visible.findIndex((dataset) => dataset.dataset_id === selected) : -1;
  if (selectedIndex >= state.datasetRenderLimit) {
    state.datasetRenderLimit = Math.min(visible.length, selectedIndex + 1);
  }

  const visibleRows = visible.slice(0, state.datasetRenderLimit);
  const fragment = document.createDocumentFragment();
  visibleRows.forEach((dataset) => {
    const item = document.createElement("div");
    item.className = "dataset-item";
    item.tabIndex = 0;
    item.setAttribute("role", "button");
    if (selected === dataset.dataset_id) item.classList.add("active");
    item.style.setProperty("--item-theme", getThemeColor(dataset.mega_theme || "Outros"));

    const title = document.createElement("div");
    title.className = "dataset-title";
    title.textContent = compactTitle(displayTitle(dataset), 72);

    const meta = document.createElement("div");
    meta.className = "dataset-meta-row";
    const degree = state.degreeById.get(dataset.dataset_id) || 0;
    const fieldsCount = document.createElement("span");
    fieldsCount.textContent = `${dataset.field_count || 0} campos`;
    const linksCount = document.createElement("span");
    linksCount.textContent = `${degree} ligações`;
    meta.append(fieldsCount, linksCount);
    meta.appendChild(readinessChip(dataset, true));

    const conceptRow = document.createElement("div");
    conceptRow.className = "dataset-meta-row concept-row";
    const themeChip = createChip(dataset.mega_theme || "Outros", "tag theme-tag");
    conceptRow.appendChild(themeChip);
    (datasetFacets(dataset).labels || []).slice(0, 3).forEach((label) => {
      const chip = createChip(label, "tag facet-tag");
      chip.textContent = label;
      chip.title = "Faceta inferida a partir dos metadados";
      conceptRow.appendChild(chip);
    });
    const fieldWrap = document.createElement("div");
    fieldWrap.className = "dataset-field-wrap";
    (dataset.fields || []).slice(0, 10).forEach((field) => fieldWrap.appendChild(createChip(field)));
    const fieldDisclosure = createDisclosure(
      `${Math.min((dataset.fields || []).length, 10)} campos`,
      [fieldWrap],
      "compact-disclosure dataset-fields-disclosure",
    );

    item.appendChild(title);
    item.appendChild(meta);
    item.appendChild(conceptRow);
    item.appendChild(fieldDisclosure);
    item.addEventListener("click", (event) => {
      if (event.target.closest("details")) return;
      selectDataset(dataset.dataset_id);
    });
    item.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectDataset(dataset.dataset_id);
      }
    });
    fragment.appendChild(item);
  });
  datasetList.appendChild(fragment);

  if (visibleRows.length < visible.length) {
    datasetList.appendChild(createShowMoreButton(
      `Mostrar mais ${Math.min(DATASET_RENDER_STEP, visible.length - visibleRows.length)} datasets`,
      () => {
        state.datasetRenderLimit = Math.min(visible.length, state.datasetRenderLimit + DATASET_RENDER_STEP);
        renderDatasetList();
      },
    ));
  }

  if (!visible.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    const message = document.createElement("span");
    message.textContent = "Sem resultados para estes filtros.";
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "ghost-button";
    clear.textContent = "Limpar filtros";
    clear.addEventListener("click", clearCatalogFilters);
    empty.append(message, clear);
    datasetList.appendChild(empty);
  }
}

function graphData() {
  const visible = filteredDatasets();
  const links = visibleLinks();

  if (state.selectedDataset) {
    const focusLinks = links
      .filter((link) => link.source === state.selectedDataset || link.target === state.selectedDataset)
      .slice(0, 45);
    const neighborIds = focusLinks
      .flatMap((link) => [link.source, link.target]);
    const ids = new Set([state.selectedDataset, ...neighborIds]);
    return {
      scope: `${ids.size} datasets ligados ao foco`,
      nodes: visible.filter((dataset) => ids.has(dataset.dataset_id)),
      links: focusLinks,
    };
  }

  const topLinks = links.slice(0, 260);
  const candidateIds = new Set(topLinks.flatMap((link) => [link.source, link.target]));
  const ids = new Set(
    visible
      .filter((dataset) => candidateIds.has(dataset.dataset_id))
      .slice(0, 70)
      .map((dataset) => dataset.dataset_id),
  );
  return {
    scope: "Top ligações do catálogo",
    nodes: visible.filter((dataset) => ids.has(dataset.dataset_id)),
    links: topLinks.filter((link) => ids.has(link.source) && ids.has(link.target)),
  };
}

function renderGraph() {
  const container = document.getElementById("graphContainer");
  const width = container.clientWidth;
  const height = container.clientHeight;
  const signature = `${state._viewStamp}|${state._selectionStamp}|${width}|${height}`;
  if (state._graphState?.signature === signature) {
    return;
  }

  state._graphState = {signature};
  const data = graphData();
  const nodeMap = new Map(data.nodes.map((dataset) => [dataset.dataset_id, {
    dataset_id: dataset.dataset_id,
    title: displayTitle(dataset),
    degree: state.degreeById.get(dataset.dataset_id) || 0,
  }]));
  const linkData = data.links
    .filter((link) => nodeMap.has(link.source) && nodeMap.has(link.target))
    .map((link) => ({...link}));
  const nodes = Array.from(nodeMap.values());

  graphScope.textContent = data.scope;
  const svg = d3.select(graphEl);
  svg.selectAll("*").remove();
  svg.attr("viewBox", `0 0 ${width} ${height}`).attr("preserveAspectRatio", "xMidYMid meet");

  if (!nodes.length) {
    svg.append("text")
      .attr("x", width / 2)
      .attr("y", height / 2)
      .attr("text-anchor", "middle")
      .attr("fill", "#657489")
      .text("Sem ligações para os filtros atuais.");
    return;
  }

  const rootSvg = svg.append("g");
  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(linkData).id((d) => d.dataset_id).distance((d) => 95 + Math.max(0, 8 - d.score) * 10).strength(0.32))
    .force("charge", d3.forceManyBody().strength(-430))
    .force("center", d3.forceCenter(width / 2, height / 2))
    .force("collision", d3.forceCollide((d) => 18 + Math.min(12, d.degree)));

  const link = rootSvg.append("g")
    .selectAll("line")
    .data(linkData)
    .join("line")
    .attr("class", "link")
    .attr("stroke-width", (d) => Math.max(1.2, Math.min(5, d.score / 2)));

  const node = rootSvg.append("g")
    .selectAll("g")
    .data(nodes)
    .join("g")
    .attr("class", (d) => d.dataset_id === state.selectedDataset ? "node active" : "node")
    .on("click", (event, d) => {
      event.stopPropagation();
      selectDataset(d.dataset_id);
    })
    .on("mouseenter", (event, d) => {
      highlightConnections(d.dataset_id);
      tooltip.textContent = `${d.title} · ${d.degree} ligações no score atual`;
      tooltip.style.opacity = 1;
      moveTooltip(event);
    })
    .on("mousemove", moveTooltip)
    .on("mouseleave", () => {
      tooltip.style.opacity = 0;
      clearHighlights();
    });

  node.append("circle")
    .attr("class", "node-circle")
    .attr("r", (d) => 7 + Math.min(11, Math.sqrt(d.degree || 1) * 2));

  node.append("text")
    .attr("class", "node-text")
    .attr("x", (d) => 12 + Math.min(10, Math.sqrt(d.degree || 1)))
    .attr("y", 4)
    .text((d) => shouldLabelNode(d) ? compactTitle(d.title, 28) : "");

  simulation.on("tick", () => {
    nodes.forEach((nodeItem) => {
      nodeItem.x = Math.max(24, Math.min(width - 24, nodeItem.x));
      nodeItem.y = Math.max(24, Math.min(height - 24, nodeItem.y));
    });
    link
      .attr("x1", (d) => d.source.x)
      .attr("y1", (d) => d.source.y)
      .attr("x2", (d) => d.target.x)
      .attr("y2", (d) => d.target.y);
    node.attr("transform", (d) => `translate(${d.x},${d.y})`);
  });

  d3.select(graphEl).call(d3.zoom().scaleExtent([0.5, 4]).on("zoom", (event) => {
    rootSvg.attr("transform", event.transform);
  }));

  function shouldLabelNode(d) {
    return d.dataset_id === state.selectedDataset || d.degree >= 12 || nodes.length < 26;
  }

  function highlightConnections(datasetId) {
    node.select("circle").attr("fill", (d) => {
      if (d.dataset_id === datasetId) return "#c86d1d";
      const isNeighbor = linkData.some((l) => {
        const src = l.source.dataset_id || l.source;
        const dst = l.target.dataset_id || l.target;
        return (src === datasetId && dst === d.dataset_id) || (dst === datasetId && src === d.dataset_id);
      });
      return isNeighbor ? "#1b5fa7" : "#9eb0c5";
    });
    link.attr("class", (l) => {
      const src = l.source.dataset_id || l.source;
      const dst = l.target.dataset_id || l.target;
      return src === datasetId || dst === datasetId ? "link link-highlight" : "link";
    });
  }

  function clearHighlights() {
    node.select("circle").attr("fill", null);
    link.attr("class", "link");
  }
}

function moveTooltip(event) {
  tooltip.style.left = `${event.clientX + 12}px`;
  tooltip.style.top = `${event.clientY + 12}px`;
}

function hideTooltip() {
  tooltip.style.opacity = 0;
}

function buildTreeForSelected(datasetId) {
  if (!datasetId) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Escolhe um dataset para ver ligações candidatas.";
    clearElement(linkTree);
    linkTree.appendChild(empty);
    return;
  }

  const links = visibleLinks()
    .filter((item) => item.source === datasetId || item.target === datasetId)
    .slice(0, 60);

  if (!links.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem ligações com o score atual. Reduz a força da ligação.";
    clearElement(linkTree);
    linkTree.appendChild(empty);
    return;
  }

  const byField = new Map();
  links.forEach((edge) => {
    const otherId = edge.source === datasetId ? edge.target : edge.source;
    const other = state.datasetById.get(otherId);
    const otherTitle = displayTitle(other || {dataset_id: otherId});
    (edge.shared_fields || []).slice(0, 6).forEach((field) => {
      if (!byField.has(field)) byField.set(field, []);
      byField.get(field).push({dataset_id: otherId, dataset_title: otherTitle, score: edge.score});
    });
  });

  clearElement(linkTree);
  Array.from(byField.entries())
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, 12)
    .forEach(([field, datasets]) => {
      const group = document.createElement("details");
      group.className = "relation-group";
      group.open = linkTree.childElementCount < 3;
      const heading = document.createElement("summary");
      const fieldLabel = document.createElement("span");
      fieldLabel.className = "tag";
      fieldLabel.textContent = safeText(field);
      const count = document.createElement("span");
      count.textContent = ` ${datasets.length} dataset(s)`;
      heading.append(fieldLabel, count);
      group.appendChild(heading);

      datasets
        .sort((a, b) => b.score - a.score || a.dataset_title.localeCompare(b.dataset_title))
        .slice(0, 6)
        .forEach((entry) => {
          const row = document.createElement("div");
          row.className = "relation-row";

          const title = document.createElement("div");
          title.className = "relation-title";
          title.textContent = entry.dataset_title;

          const action = document.createElement("button");
          action.type = "button";
          action.className = "relation-button";
          action.textContent = `score ${entry.score}`;
          action.onclick = () => selectDataset(entry.dataset_id);

          row.appendChild(title);
          row.appendChild(action);
          group.appendChild(row);
        });

      linkTree.appendChild(group);
    });
}

function renderOpportunities() {
  const selected = state.selectedDataset;
  const byTheme = state.activeTheme
    ? state.opportunities.filter((opp) => opp.dataset_ids.some((id) => state.datasetById.get(id)?.mega_theme === state.activeTheme))
    : state.opportunities;
  const baseList = selected
    ? state.opportunities.filter((opp) => opp.dataset_ids.includes(selected)).slice(0, 12)
    : byTheme.slice(0, 10);

  clearElement(opportunitiesSection);
  const title = document.createElement("h2");
  title.textContent = selected ? "Campos com maior potencial" : "Oportunidades globais";
  opportunitiesSection.appendChild(title);

  if (!baseList.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem oportunidades para este filtro.";
    opportunitiesSection.appendChild(empty);
    return;
  }

  baseList.forEach((opp) => {
    const box = document.createElement("div");
    box.className = "opportunity";
    const shown = opp.dataset_ids
      .slice(0, 4)
      .map((id) => compactTitle(displayTitle(state.datasetById.get(id) || {dataset_id: id}), 34))
      .join(", ");
    const more = opp.dataset_count > 4 ? ` +${opp.dataset_count - 4}` : "";
      const key = document.createElement("strong");
      key.textContent = safeText(opp.key);
      const extra = document.createElement("small");
      extra.textContent = `${opp.dataset_count} datasets · ${shown}${more}`;
    box.append(key, createDisclosure("datasets", [extra], "compact-disclosure"));
    opportunitiesSection.appendChild(box);
  });
}

function renderSelectedInfo() {
  if (!state.selectedDataset) {
    const info = document.createElement("div");
    info.className = "meta";
    info.textContent = "Escolhe um dataset para abrir cruzamentos possíveis.";
    clearElement(selectedModel);
    selectedModel.appendChild(info);
    return;
  }
  const dataset = state.datasetById.get(state.selectedDataset);
  const fields = (dataset?.fields || []).slice(0, 8);
  clearElement(selectedModel);

  const title = document.createElement("div");
  title.className = "selected-title";
  title.textContent = displayTitle(dataset);

  const metaRow = document.createElement("div");
  metaRow.className = "dataset-meta-row";
  const themeTag = createChip(dataset?.mega_theme || "Outros", "tag theme-tag");
  const fieldCount = document.createElement("span");
  fieldCount.textContent = `${dataset?.field_count || 0} campos`;
  const degree = document.createElement("span");
  degree.textContent = `${state.degreeById.get(state.selectedDataset) || 0} ligações`;
  metaRow.append(themeTag, fieldCount, degree, readinessChip(dataset));

  const fieldRow = document.createElement("div");
  fieldRow.className = "dataset-field-wrap";
  fields.forEach((field) => {
    fieldRow.appendChild(createChip(field));
  });

  const gaps = document.createElement("div");
  gaps.className = "quality-note";
  const info = readiness(dataset);
  gaps.textContent = `Fit analítico: ${info.label || "Rever"} · validar ${(info.gaps || ["fonte"]).slice(0, 3).join(", ")}.`;

  selectedModel.append(title, metaRow, gaps, createDisclosure("ver campos", [fieldRow], "compact-disclosure"));
}

async function selectDataset(datasetId) {
  const changed = touchSelectionState(datasetId);
  if (!changed && state.recentData) {
    renderAll();
    return;
  }
  state.recentData = null;
  renderAll();
  const cachedMeta = state._datasetMetaCache.get(datasetId);
  if (cachedMeta && Date.now() - cachedMeta.timestamp < META_DATA_CACHE_TTL_MS) {
    const payload = cachedMeta.value;
    if (payload?.metas?.default?.description && state.selectedDataset === datasetId) {
      const container = document.createElement("div");
      container.className = "meta description";
      container.textContent = stripHtml(payload.metas.default.description);
      selectedModel.appendChild(container);
    }
  }

  loadRecentRecords(datasetId).catch(showRecordsError);

  try {
    const cacheEntry = state._datasetMetaCache.get(datasetId);
    if (cacheEntry && Date.now() - cacheEntry.timestamp < META_DATA_CACHE_TTL_MS) {
      return;
    }
    const payload = await fetchJson(`/api/dataset/${encodeURIComponent(datasetId)}`);
    if (payload?.metas?.default?.description && state.selectedDataset === datasetId) {
      setBoundedCache(
        state._datasetMetaCache,
        datasetId,
        {timestamp: Date.now(), value: payload},
        DATASET_METADATA_CACHE_SIZE,
      );
      const container = document.createElement("div");
      container.className = "meta description";
      container.textContent = stripHtml(payload.metas.default.description);
      selectedModel.appendChild(container);
    }
  } catch {
    // Metadata enrichment is optional. The main analysis is already rendered.
  }
}

async function loadRecentRecords(datasetId) {
  if (!datasetId) return;

  const cached = state._recentCache.get(datasetId);
  if (cached && Date.now() - cached.timestamp < RECENT_DATA_CACHE_TTL_MS) {
    if (state.selectedDataset !== datasetId) {
      return;
    }
    state.recentData = cached.value;
    renderRecentTable();
    return;
  }

  recentDataMeta.textContent = "A carregar registos recentes…";
  const payload = await fetchJson(`/api/recent/${encodeURIComponent(datasetId)}?limit=60`);
  if (state.selectedDataset !== datasetId) {
    return;
  }

  setBoundedCache(
    state._recentCache,
    datasetId,
    {timestamp: Date.now(), value: payload},
    RECENT_DATA_CACHE_SIZE,
  );
  state.recentData = payload;
  renderRecentTable();
}

function parseNumericValue(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (value === null || value === undefined || value === "") return null;
  const text = String(value)
    .replace(/\s/g, "")
    .replace(/%$/, "")
    .replace(/\./g, "")
    .replace(",", ".");
  const number = Number(text);
  return Number.isFinite(number) ? number : null;
}

function detectColumnRole(column, data) {
  const name = safeText(column?.name || column?.label).toLowerCase();
  if (data?.temporal_field && column?.name === data.temporal_field) return "tempo";
  if (/(^ano$|period|data|tempo|m[eê]s|mes|trimestre|semana|evolu)/i.test(name)) return "tempo";
  if (/(regi|ars|uls|hospital|concelho|distrito|local|territ|geograf|entidade|unidade|norte|centro|alentejo|algarve|lisboa)/i.test(name)) return "local";
  return "";
}

function isExcludedMeasureColumn(column, data) {
  const name = safeText(column?.name || column?.label).toLowerCase();
  if (detectColumnRole(column, data)) return true;
  return /(telefone|telemovel|telemóvel|fax|email|e-mail|codigo|código|cod_|^cod$|postal|nif|nipc|niss|id_|^id$|url|link|latitude|longitude)/i.test(name);
}

function quantile(sortedValues, q) {
  if (!sortedValues.length) return null;
  const position = (sortedValues.length - 1) * q;
  const base = Math.floor(position);
  const rest = position - base;
  if (sortedValues[base + 1] === undefined) {
    return sortedValues[base];
  }
  return sortedValues[base] + rest * (sortedValues[base + 1] - sortedValues[base]);
}

function robustStats(values) {
  const nums = values.map((value) => Number(value)).filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!nums.length) return null;
  const median = quantile(nums, 0.5);
  const q1 = quantile(nums, 0.25);
  const q3 = quantile(nums, 0.75);
  const iqr = Math.max(0, q3 - q1);
  const deviations = nums.map((value) => Math.abs(value - median)).sort((a, b) => a - b);
  const mad = quantile(deviations, 0.5) || 0;
  const sum = nums.reduce((acc, value) => acc + value, 0);
  const avg = sum / nums.length;
  const variance = nums.reduce((acc, value) => acc + ((value - avg) ** 2), 0) / nums.length;
  const stddev = Math.sqrt(variance);
  const robustScale = mad > 0 ? mad / 0.6745 : iqr > 0 ? iqr / 1.349 : stddev;
  return {
    count: nums.length,
    sum,
    avg,
    median,
    q1,
    q3,
    iqr,
    mad,
    robustScale,
    stddev,
    min: nums[0],
    max: nums[nums.length - 1],
  };
}

function numericProfileForColumn(rows, column) {
  const values = rows
    .map((row, index) => ({index, value: parseNumericValue(row[column.name])}))
    .filter((item) => item.value !== null);
  if (values.length < Math.max(3, Math.ceil(rows.length * 0.25))) return null;
  const nums = values.map((item) => item.value);
  const stats = robustStats(nums);
  const uniqueCount = new Set(nums.map((value) => String(value))).size;
  return {
    column,
    values,
    count: stats.count,
    sum: stats.sum,
    avg: stats.avg,
    median: stats.median,
    q1: stats.q1,
    q3: stats.q3,
    iqr: stats.iqr,
    mad: stats.mad,
    robustScale: stats.robustScale,
    min: stats.min,
    max: stats.max,
    stddev: stats.stddev,
    uniqueCount,
    missing: rows.length - nums.length,
  };
}

function groupValuesByContext(profile, rows, column) {
  const groups = new Map();
  if (!column) return groups;
  profile.values.forEach((item) => {
    const key = safeText(rows[item.index]?.[column.name]).trim();
    if (!key) return;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(item.value);
  });
  return groups;
}

function baselineForItem(profile, rows, item, timeColumn, localColumn) {
  const record = rows[item.index] || {};
  const candidates = [];
  const period = timeColumn ? safeText(record[timeColumn.name]).trim() : "";
  const place = localColumn ? safeText(record[localColumn.name]).trim() : "";

  if (profile.periodGroups?.has(period)) {
    candidates.push({kind: "período", label: period, values: profile.periodGroups.get(period)});
  }
  if (profile.localGroups?.has(place)) {
    candidates.push({kind: "local", label: place, values: profile.localGroups.get(place)});
  }
  candidates.push({kind: "amostra", label: "amostra global", values: profile.values.map((entry) => entry.value)});

  for (const candidate of candidates) {
    if ((candidate.values || []).length < 8) continue;
    const stats = robustStats(candidate.values);
    if (!stats || !stats.robustScale) continue;
    return {...candidate, stats};
  }
  return null;
}

function anomalyScore(itemValue, stats) {
  if (!stats || !stats.robustScale) return null;
  const modifiedZ = 0.6745 * (itemValue - stats.median) / (stats.mad || stats.robustScale * 0.6745);
  const robustZ = (itemValue - stats.median) / stats.robustScale;
  const iqrHigh = stats.q3 + 1.5 * stats.iqr;
  const iqrLow = stats.q1 - 1.5 * stats.iqr;
  const beyondIqr = stats.iqr > 0 && (itemValue > iqrHigh || itemValue < iqrLow);
  const score = Math.max(Math.abs(modifiedZ), Math.abs(robustZ));
  return {score, modifiedZ, robustZ, beyondIqr, iqrHigh, iqrLow};
}

function buildQuickAnalysis(data, columns) {
  const rows = data?.records || [];
  const roles = columns.map((column) => ({column, role: detectColumnRole(column, data)}));
  const timeColumn = roles.find((item) => item.role === "tempo")?.column || null;
  const localColumn = roles.find((item) => item.role === "local")?.column || null;
  const numericProfiles = columns
    .filter((column) => !isExcludedMeasureColumn(column, data))
    .map((column) => numericProfileForColumn(rows, column))
    .filter(Boolean)
    .sort((a, b) => (b.stddev * b.count) - (a.stddev * a.count));
  numericProfiles.forEach((profile) => {
    profile.periodGroups = groupValuesByContext(profile, rows, timeColumn);
    profile.localGroups = groupValuesByContext(profile, rows, localColumn);
  });
  const primary = numericProfiles[0] || null;

  const anomalies = [];
  numericProfiles.slice(0, 6).forEach((profile) => {
    if (profile.count < 8 || profile.uniqueCount < 4) return;
    profile.values.forEach((item) => {
      const baseline = baselineForItem(profile, rows, item, timeColumn, localColumn);
      const score = anomalyScore(item.value, baseline?.stats);
      if (score && (Math.abs(score.modifiedZ) >= 3.5 || (score.beyondIqr && score.score >= 2.8))) {
        const record = rows[item.index] || {};
        const period = timeColumn ? safeText(record[timeColumn.name]) : "";
        const place = localColumn ? safeText(record[localColumn.name]) : "";
        anomalies.push({
          field: profile.column.label || profile.column.name,
          value: item.value,
          expected: baseline.stats.median,
          baselineKind: baseline.kind,
          baselineLabel: baseline.label,
          baselineCount: baseline.stats.count,
          delta: item.value - baseline.stats.median,
          row: item.index + 1,
          z: score.score,
          modifiedZ: score.modifiedZ,
          iqrFlag: score.beyondIqr,
          direction: item.value >= baseline.stats.median ? "acima" : "abaixo",
          severity: Math.abs(score.modifiedZ) >= 5 || score.score >= 5 ? "alta" : Math.abs(score.modifiedZ) >= 3.5 ? "média" : "ligeira",
          period,
          place,
        });
      }
    });
  });
  anomalies.sort((a, b) => b.z - a.z);

  const timeBuckets = new Map();
  if (primary) {
    primary.values.forEach((item) => {
      const record = rows[item.index] || {};
      const period = timeColumn ? safeText(record[timeColumn.name] || "sem período") : `#${item.index + 1}`;
      timeBuckets.set(period, (timeBuckets.get(period) || 0) + item.value);
    });
  }
  const running = [...timeBuckets.entries()]
    .sort(([a], [b]) => safeText(a).localeCompare(safeText(b), "pt-PT", {numeric: true}))
    .reduce((acc, [period, value]) => {
      const total = (acc.at(-1)?.total || 0) + value;
      acc.push({period, value, total});
      return acc;
    }, []);

  return {
    rows,
    timeColumn,
    localColumn,
    numericProfiles,
    primary,
    anomalies: anomalies.slice(0, 4),
    anomalyTotal: anomalies.length,
    running,
  };
}

function renderQuickEmpty(message) {
  if (!quickAnalysisMeta) return;
  quickAnalysisMeta.textContent = message;
  [quickAnalysisKpis, quickKeyVariables, quickTrendChart, quickDistributionChart, quickAnomalyList].forEach((node) => {
    if (node) clearElement(node);
  });
}

function addQuickKpi(label, value, detail) {
  const card = document.createElement("div");
  const span = document.createElement("span");
  span.textContent = label;
  const strong = document.createElement("strong");
  strong.textContent = value;
  const small = document.createElement("small");
  small.textContent = detail;
  card.append(span, strong, small);
  quickAnalysisKpis.appendChild(card);
}

function renderQuickVariables(analysis) {
  clearElement(quickKeyVariables);
  const items = [
    ["Tempo", analysis.timeColumn?.label || analysis.timeColumn?.name || "não identificado", "eixo para tendência"],
    ["Local", analysis.localColumn?.label || analysis.localColumn?.name || "não identificado", "território ou entidade"],
    ["Medida principal", analysis.primary?.column?.label || analysis.primary?.column?.name || "sem medida contínua", "maior variação útil"],
  ];
  analysis.numericProfiles.slice(1, 3).forEach((profile) => {
    items.push(["Medida", profile.column.label || profile.column.name, `${formatNumber(Math.round(profile.sum))} total`]);
  });
  if (analysis.numericProfiles.length > 3) {
    items.push(["Outras medidas", `${analysis.numericProfiles.length - 3}`, "disponíveis na amostra"]);
  }
  const fragment = document.createDocumentFragment();
  items.forEach(([label, value, detail]) => {
    const item = document.createElement("div");
    item.className = "quick-key-item";
    const top = document.createElement("span");
    top.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = compactTitle(value, 46);
    strong.title = value;
    const small = document.createElement("small");
    small.textContent = detail;
    item.append(top, strong, small);
    fragment.appendChild(item);
  });
  quickKeyVariables.appendChild(fragment);
}

function renderQuickTrend(analysis) {
  clearElement(quickTrendChart);
  const width = Math.max(360, quickTrendChart.closest(".quick-chart-wrap")?.clientWidth || 360);
  const height = 210;
  quickTrendChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const points = analysis.running;
  if (!points.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem medida contínua para acumular.";
    quickTrendChart.appendChild(empty);
    return;
  }
  const left = 46;
  const right = width - 22;
  const top = 28;
  const bottom = height - 38;
  const max = Math.max(...points.map((point) => point.total), 1);
  const xStep = points.length > 1 ? (right - left) / (points.length - 1) : 0;
  const coords = points.map((point, index) => ({
    x: left + index * xStep,
    y: bottom - (point.total / max) * (bottom - top),
    point,
  }));
  const axis = svgNode("g", {class: "quick-chart-axis"});
  axis.append(svgNode("line", {x1: left, y1: bottom, x2: right, y2: bottom}), svgNode("line", {x1: left, y1: top, x2: left, y2: bottom}));
  quickTrendChart.appendChild(axis);
  const path = svgNode("path", {class: "quick-trend-line", d: coords.map((coord, index) => `${index ? "L" : "M"} ${coord.x} ${coord.y}`).join(" ")});
  quickTrendChart.appendChild(path);
  coords.forEach((coord, index) => {
    if (points.length > 8 && index % Math.ceil(points.length / 6)) return;
    const group = svgNode("g", {class: "quick-trend-point", transform: `translate(${coord.x}, ${coord.y})`});
    const title = svgNode("title");
    title.textContent = `${coord.point.period}: ${formatNumber(Math.round(coord.point.total))}`;
    group.append(title, svgNode("circle", {r: 3.5}));
    const label = svgNode("text", {y: 18, "text-anchor": coord.x > width - 80 ? "end" : "middle"});
    label.textContent = compactTitle(coord.point.period, 12);
    group.appendChild(label);
    quickTrendChart.appendChild(group);
  });
}

function renderQuickDistribution(analysis) {
  clearElement(quickDistributionChart);
  const width = Math.max(360, quickDistributionChart.closest(".quick-chart-wrap")?.clientWidth || 360);
  const height = 210;
  quickDistributionChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const profile = analysis.primary;
  if (!profile || !profile.values.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem distribuição numérica.";
    quickDistributionChart.appendChild(empty);
    return;
  }
  const binCount = 6;
  const span = profile.max - profile.min || 1;
  const bins = Array.from({length: binCount}, (_, index) => ({
    start: profile.min + (span / binCount) * index,
    end: profile.min + (span / binCount) * (index + 1),
    count: 0,
  }));
  profile.values.forEach((item) => {
    const index = Math.min(binCount - 1, Math.floor(((item.value - profile.min) / span) * binCount));
    bins[index].count += 1;
  });
  const maxCount = Math.max(...bins.map((bin) => bin.count), 1);
  const left = 34;
  const right = width - 18;
  const bottom = height - 34;
  const top = 24;
  const gap = 8;
  const barWidth = ((right - left) - gap * (binCount - 1)) / binCount;
  bins.forEach((bin, index) => {
    const h = (bin.count / maxCount) * (bottom - top);
    const x = left + index * (barWidth + gap);
    const y = bottom - h;
    const rect = svgNode("rect", {class: "quick-distribution-bar", x, y, width: barWidth, height: Math.max(2, h), rx: 4});
    const title = svgNode("title");
    title.textContent = `${formatNumber(Math.round(bin.start))} a ${formatNumber(Math.round(bin.end))}: ${formatNumber(bin.count)} registos`;
    rect.appendChild(title);
    quickDistributionChart.appendChild(rect);
    if (index === 0 || index === bins.length - 1) {
      const label = svgNode("text", {x: x + barWidth / 2, y: bottom + 18, "text-anchor": "middle"});
      label.textContent = formatNumber(Math.round(bin.start));
      quickDistributionChart.appendChild(label);
    }
  });
}

function renderQuickAnomalies(analysis) {
  clearElement(quickAnomalyList);
  if (!analysis.anomalies.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Nada salta à vista nesta amostra. Boa notícia para triagem rápida, mas confirma na tabela se o dataset for crítico.";
    quickAnomalyList.appendChild(empty);
    return;
  }
  const intro = document.createElement("div");
  intro.className = "quick-anomaly-note";
  intro.textContent = `${formatNumber(analysis.anomalyTotal || analysis.anomalies.length)} sinais robustos fora do padrão. A comparação usa mediana/MAD e, quando possível, grupos por período ou local; confirma unidade, cobertura e definição antes de concluir.`;
  quickAnomalyList.appendChild(intro);
  const fragment = document.createDocumentFragment();
  analysis.anomalies.forEach((anomaly) => {
    const item = document.createElement("div");
    item.className = `quick-anomaly-item severity-${anomaly.severity}`;
    const badge = document.createElement("span");
    badge.className = "quick-anomaly-badge";
    badge.textContent = anomaly.severity === "alta" ? "Priorizar validação" : anomaly.severity === "média" ? "Ver contexto" : "Sinal leve";
    const strong = document.createElement("strong");
    strong.textContent = compactTitle(anomaly.field, 38);
    strong.title = anomaly.field;
    const summary = document.createElement("p");
    summary.textContent = `${formatNumber(Math.round(Math.abs(anomaly.delta)))} ${anomaly.direction} da mediana de referência.`;
    const facts = document.createElement("div");
    facts.className = "quick-anomaly-facts";
    [
      ["Valor", formatNumber(Math.round(anomaly.value))],
      ["Mediana ref.", formatNumber(Math.round(anomaly.expected))],
      ["Linha", `#${anomaly.row}`],
      ["Desvio", `${anomaly.z.toFixed(1)} robusto`],
      ["Base", `${anomaly.baselineKind} · n=${formatNumber(anomaly.baselineCount)}`],
    ].forEach(([label, value]) => {
      const fact = document.createElement("span");
      fact.textContent = `${label}: ${value}`;
      facts.appendChild(fact);
    });
    const context = document.createElement("small");
    const contextParts = [
      anomaly.period && `período ${compactTitle(anomaly.period, 18)}`,
      anomaly.place && `local ${compactTitle(anomaly.place, 18)}`,
      anomaly.baselineLabel && `comparado com ${compactTitle(anomaly.baselineLabel, 24)}`,
    ].filter(Boolean);
    context.textContent = contextParts.length ? contextParts.join(" · ") : "Sem eixo temporal/local detetado para contextualizar.";
    const action = document.createElement("em");
    action.textContent = anomaly.severity === "alta"
      ? "Validar fonte, duplicados e mudança de definição antes de interpretar."
      : "Confirmar se é efeito real, falta de registos, codificação ou grupo pouco comparável.";
    item.append(badge, strong, summary, facts, context, action);
    fragment.appendChild(item);
  });
  quickAnomalyList.appendChild(fragment);
}

function renderQuickAnalysis(data, columns) {
  if (!quickAnalysisMeta) return;
  const analysis = buildQuickAnalysis(data, columns);
  if (!analysis.rows.length) {
    renderQuickEmpty("Sem registos suficientes para análise rápida.");
    return;
  }
  clearElement(quickAnalysisKpis);
  quickAnalysisMeta.textContent = `${formatNumber(analysis.rows.length)} linhas analisadas na janela atual. Heurística exploratória, não validação estatística final.`;
  addQuickKpi("Medidas contínuas", formatNumber(analysis.numericProfiles.length), analysis.primary ? compactTitle(analysis.primary.column.label || analysis.primary.column.name, 28) : "sem medida principal");
  addQuickKpi("Running total", analysis.running.length ? formatNumber(Math.round(analysis.running.at(-1).total)) : "-", analysis.timeColumn ? `por ${analysis.timeColumn.label || analysis.timeColumn.name}` : "ordem da amostra");
  addQuickKpi("Anomalias", formatNumber(analysis.anomalyTotal || analysis.anomalies.length), "fora do padrão da amostra");
  addQuickKpi("Local", analysis.localColumn ? "sim" : "não", analysis.localColumn?.label || analysis.localColumn?.name || "campo não detetado");
  renderQuickVariables(analysis);
  renderQuickTrend(analysis);
  renderQuickDistribution(analysis);
  renderQuickAnomalies(analysis);
}

function renderRecentTable() {
  const data = state.recentData;
  const selected = state.selectedDataset;
  const tableWrap = recentDataTable.closest(".table-wrap");
  const tableFrame = tableWrap?.closest(".table-strip-frame");
  const selectedDataset = selected ? state.datasetById.get(selected) : null;
  const selectedTheme = getThemeColor(selectedDataset?.mega_theme || "Outros");
  const columns = (data?.columns?.length ? data.columns : ((selectedDataset?.fields || [])
    .slice(0, 12)
    .map((field) => ({name: field, label: field, type: "string"}))));

  if (selected && tableWrap && tableFrame) {
    tableWrap.style.setProperty("--table-theme-color", selectedTheme);
    tableFrame.classList.add("with-theme");
    tableFrame.style.setProperty("--table-theme-color", selectedTheme);
  } else {
    if (tableWrap) {
      tableWrap.style.removeProperty("--table-theme-color");
    }
    if (tableFrame) {
      tableFrame.classList.remove("with-theme");
      tableFrame.style.removeProperty("--table-theme-color");
    }
    if (!selected || !tableWrap || !tableFrame) {
      recentDataTable.hidden = true;
      recentDataEmpty.hidden = false;
      recentDataEmpty.textContent = "Escolhe um dataset.";
      recentDataMeta.textContent = "Amostra recente da API.";
      renderQuickEmpty("Escolhe um dataset para ativar a leitura rápida.");
      return;
    }
  }

  const thead = recentDataTable.querySelector("thead");
  const tbody = recentDataTable.querySelector("tbody");
  clearElement(thead);
  clearElement(tbody);

  if (!selected) {
    recentDataTable.hidden = true;
    recentDataEmpty.hidden = false;
    recentDataEmpty.textContent = "Escolhe um dataset.";
    recentDataMeta.textContent = "Amostra recente da API.";
    renderQuickEmpty("Escolhe um dataset para ativar a leitura rápida.");
    return;
  }

  if (!data || !columns.length) {
    recentDataTable.hidden = true;
    recentDataEmpty.hidden = false;
    recentDataEmpty.textContent = "Sem registos recentes disponíveis para este dataset.";
    recentDataMeta.textContent = "Sem dados recentes para este dataset com o filtro atual.";
    renderQuickEmpty("Sem amostra para analisar.");
    return;
  }

  recentDataTable.hidden = false;
  recentDataEmpty.hidden = true;
  const rows = data?.records || [];
  const temporal = data?.temporal_field ? `campo temporal: ${data.temporal_field}` : "campo temporal não identificado";
  recentDataMeta.textContent = `${rows.length} registos · desde ${data?.min_year || "N/A"} quando identificável · ${temporal}`;

  if (!rows.length) {
    const emptyRow = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = columns.length || 1;
    td.textContent = "Sem registos recentes para este dataset.";
    emptyRow.appendChild(td);
    tbody.appendChild(emptyRow);
  }

  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column.label || column.name;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  rows.forEach((record) => {
    const row = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      td.textContent = formatCell(record[column.name]);
      row.appendChild(td);
    });
    tbody.appendChild(row);
  });
  renderQuickAnalysis(data, columns);
}

function showRecordsError(error) {
  recentDataTable.hidden = true;
  recentDataEmpty.hidden = false;
  recentDataEmpty.textContent = error.message;
  recentDataMeta.textContent = "Falha ao consultar registos recentes.";
  renderQuickEmpty("Falha ao carregar a amostra para análise rápida.");
}

function stripHtml(value) {
  return safeText(value).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function renderAll() {
  renderThemeOptions();
  renderThemeCards();
  renderCatalogCockpit();
  renderSummary();
  renderDatasetList();
  renderGraph();
  renderSelectedInfo();
  buildTreeForSelected(state.selectedDataset);
  renderOpportunities();
  renderRecentTable();
}

function setActiveTheme(theme) {
  if (state.activeTheme === theme) {
    return;
  }
  state.activeTheme = theme || "";
  state.datasetRenderLimit = DATASET_RENDER_STEP;
  invalidateViewState();
  themeFilter.value = state.activeTheme;
  ensureSelectionInFilter();
  renderAll();
}

function clearCatalogFilters() {
  state.activeTheme = "";
  state.filterText = "";
  state.localScope = "";
  state.institutionType = "";
  state.dimensionType = "";
  datasetFilter.value = "";
  themeFilter.value = "";
  if (localScopeFilter) localScopeFilter.value = "";
  if (institutionFilter) institutionFilter.value = "";
  if (dimensionFilter) dimensionFilter.value = "";
  state.datasetRenderLimit = DATASET_RENDER_STEP;
  invalidateViewState();
  ensureSelectionInFilter();
  renderAll();
}

function formatCell(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number") return new Intl.NumberFormat("pt-PT").format(value);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatNumber(value) {
  return new Intl.NumberFormat("pt-PT").format(value || 0);
}

function setupEvents() {
  minScoreInput.value = state.minScore;
  minScoreValue.textContent = state.minScore;

  minScoreInput.addEventListener("input", () => {
    state.minScore = Number(minScoreInput.value);
    minScoreValue.textContent = state.minScore;
    debounceLoadAnalysis();
  });

  datasetFilter.addEventListener("input", () => {
    const nextFilter = datasetFilter.value;
    clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      state.filterText = nextFilter;
      state.datasetRenderLimit = DATASET_RENDER_STEP;
      invalidateViewState();
      ensureSelectionInFilter();
      renderAll();
    }, 140);
  });

  themeFilter.addEventListener("change", () => {
    setActiveTheme(themeFilter.value);
  });

  [localScopeFilter, institutionFilter, dimensionFilter].forEach((element) => {
    element?.addEventListener("change", () => {
      state.localScope = localScopeFilter?.value || "";
      state.institutionType = institutionFilter?.value || "";
      state.dimensionType = dimensionFilter?.value || "";
      state.datasetRenderLimit = DATASET_RENDER_STEP;
      invalidateViewState();
      ensureSelectionInFilter();
      renderAll();
    });
  });

  document.getElementById("refreshButton").addEventListener("click", () => {
    loadAnalysis().catch(showError);
  });

  document.getElementById("clearSelectionButton").addEventListener("click", () => {
    touchSelectionState(null);
    state.recentData = null;
    renderAll();
  });

  reloadRecordsButton.addEventListener("click", () => {
    if (state.selectedDataset) loadRecentRecords(state.selectedDataset).catch(showRecordsError);
  });

  window.addEventListener("scroll", hideTooltip, {passive: true});

  window.addEventListener("resize", () => {
    hideTooltip();
    debounceRender();
  });
}

function showError(error) {
  syncState.textContent = "Erro";
  datasetSummary.textContent = error.message;
}

setupEvents();
loadAnalysis().catch(showError);
