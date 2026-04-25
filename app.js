const state = {
  datasets: [],
  links: [],
  opportunities: [],
  selectedDataset: null,
  activeTheme: "",
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
const themeFilter = document.getElementById("themeFilter");
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

function safeText(value) {
  return value == null ? "" : String(value);
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

  const value = state.filterText.trim().toLowerCase();
  const datasets = [];
  for (const item of state.datasets) {
    if (state.activeTheme && item.mega_theme !== state.activeTheme) {
      continue;
    }
    if (value) {
      if (!getDatasetSearchText(item).includes(value)) {
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

async function loadAnalysis() {
  syncState.textContent = "A sincronizar…";
  const url = `/api/analysis?min_score=${state.minScore}`;
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao carregar análise");
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
  syncState.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
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

  const value = dataset?.title || datasetId;
  const title = value
    .replace(/-/g, " ")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
  setBoundedCache(state._titleCache, datasetId, title, TITLE_CACHE_SIZE);
  return title;
}

function getDatasetSearchText(dataset) {
  const datasetId = dataset?.dataset_id || "";
  if (state._searchTextCache.has(datasetId)) {
    return state._searchTextCache.get(datasetId);
  }
  const text = `${datasetId} ${displayTitle(dataset)} ${(dataset?.fields || []).join(" ")}`.toLowerCase();
  setBoundedCache(state._searchTextCache, datasetId, text, SEARCH_TEXT_CACHE_SIZE);
  return text;
}

function compactTitle(value, max = 54) {
  if (!value) return "";
  return value.length > max ? `${value.slice(0, max - 1)}...` : value;
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
    description: "Visão global do catálogo e das relações mais fortes para arrancar uma exploração transversal.",
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
  datasetSummary.textContent = state.selectedDataset
    ? "Ordenado por relevância face ao dataset em foco."
    : "Ordenado por número de ligações encontradas.";
}

function renderDatasetList() {
  const visible = filteredDatasets();
  const selected = state.selectedDataset;
  clearElement(datasetList);

  const selectedIndex = selected ? visible.findIndex((dataset) => dataset.dataset_id === selected) : -1;
  if (selectedIndex >= state.datasetRenderLimit) {
    state.datasetRenderLimit = Math.min(visible.length, selectedIndex + 1);
  }

  const visibleRows = visible.slice(0, state.datasetRenderLimit);
  const fragment = document.createDocumentFragment();
  visibleRows.forEach((dataset) => {
    const item = document.createElement("button");
    item.type = "button";
    item.className = "dataset-item";
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

    const fieldRow = document.createElement("div");
    fieldRow.className = "dataset-meta-row";
    const themeChip = document.createElement("span");
    themeChip.className = "tag theme-tag";
    themeChip.textContent = dataset.mega_theme || "Outros";
    fieldRow.appendChild(themeChip);
    (dataset.fields || []).slice(0, 3).forEach((field) => {
      const chip = document.createElement("span");
      chip.className = "tag";
      chip.textContent = field;
      fieldRow.appendChild(chip);
    });

    item.appendChild(title);
    item.appendChild(meta);
    item.appendChild(fieldRow);
    item.onclick = () => selectDataset(dataset.dataset_id);
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
    empty.textContent = "Sem resultados para esta pesquisa.";
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
    scope: "Top relações do catálogo",
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
      const group = document.createElement("section");
      group.className = "relation-group";
      const heading = document.createElement("h3");
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
    box.append(key, extra);
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
  const themeTag = document.createElement("span");
  themeTag.className = "tag theme-tag";
  themeTag.textContent = safeText(dataset?.mega_theme || "Outros");
  const fieldCount = document.createElement("span");
  fieldCount.textContent = `${dataset?.field_count || 0} campos`;
  const degree = document.createElement("span");
  degree.textContent = `${state.degreeById.get(state.selectedDataset) || 0} ligações`;
  metaRow.append(themeTag, fieldCount, degree);

  const fieldRow = document.createElement("div");
  fieldRow.className = "dataset-meta-row";
  fields.forEach((field) => {
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = safeText(field);
    fieldRow.appendChild(tag);
  });

  selectedModel.append(title, metaRow, fieldRow);
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
    const response = await fetch(`/api/dataset/${encodeURIComponent(datasetId)}`);
    const payload = await response.json();
    if (response.ok && payload?.metas?.default?.description && state.selectedDataset === datasetId) {
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
    } else if (!response.ok) {
      throw new Error(payload.error || "Não foi possível carregar a descrição do dataset.");
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
  const response = await fetch(`/api/recent/${encodeURIComponent(datasetId)}?limit=60`);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao consultar registos recentes");
  }
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

function numericProfileForColumn(rows, column) {
  const values = rows
    .map((row, index) => ({index, value: parseNumericValue(row[column.name])}))
    .filter((item) => item.value !== null);
  if (values.length < Math.max(3, Math.ceil(rows.length * 0.25))) return null;
  const nums = values.map((item) => item.value);
  const sum = nums.reduce((acc, value) => acc + value, 0);
  const avg = sum / nums.length;
  const variance = nums.reduce((acc, value) => acc + ((value - avg) ** 2), 0) / nums.length;
  return {
    column,
    values,
    count: nums.length,
    sum,
    avg,
    min: Math.min(...nums),
    max: Math.max(...nums),
    stddev: Math.sqrt(variance),
    missing: rows.length - nums.length,
  };
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
  const primary = numericProfiles[0] || null;

  const anomalies = [];
  numericProfiles.slice(0, 6).forEach((profile) => {
    if (!profile.stddev) return;
    profile.values.forEach((item) => {
      const z = Math.abs((item.value - profile.avg) / profile.stddev);
      if (z >= 2) {
        const record = rows[item.index] || {};
        const period = timeColumn ? safeText(record[timeColumn.name]) : "";
        const place = localColumn ? safeText(record[localColumn.name]) : "";
        anomalies.push({
          field: profile.column.label || profile.column.name,
          value: item.value,
          expected: profile.avg,
          delta: item.value - profile.avg,
          row: item.index + 1,
          z,
          direction: item.value >= profile.avg ? "acima" : "abaixo",
          severity: z >= 3 ? "alta" : z >= 2.5 ? "média" : "ligeira",
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
  intro.textContent = `${formatNumber(analysis.anomalyTotal || analysis.anomalies.length)} sinais fora do padrão. Mostro os mais fortes; confirma unidade, período e entidade antes de concluir.`;
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
    summary.textContent = `${formatNumber(Math.round(Math.abs(anomaly.delta)))} ${anomaly.direction} do padrão da amostra.`;
    const facts = document.createElement("div");
    facts.className = "quick-anomaly-facts";
    [
      ["Valor", formatNumber(Math.round(anomaly.value))],
      ["Esperado", formatNumber(Math.round(anomaly.expected))],
      ["Linha", `#${anomaly.row}`],
      ["Desvio", `${anomaly.z.toFixed(1)} sigma`],
    ].forEach(([label, value]) => {
      const fact = document.createElement("span");
      fact.textContent = `${label}: ${value}`;
      facts.appendChild(fact);
    });
    const context = document.createElement("small");
    const contextParts = [anomaly.period && `período ${compactTitle(anomaly.period, 18)}`, anomaly.place && `local ${compactTitle(anomaly.place, 18)}`].filter(Boolean);
    context.textContent = contextParts.length ? contextParts.join(" · ") : "Sem eixo temporal/local detetado para contextualizar.";
    const action = document.createElement("em");
    action.textContent = anomaly.severity === "alta"
      ? "Validar na fonte e procurar mudança de definição."
      : "Confirmar se é efeito real, falta de registos ou codificação.";
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
