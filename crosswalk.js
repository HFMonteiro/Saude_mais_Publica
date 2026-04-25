const state = {
  datasets: [],
  links: [],
  opportunities: [],
  themes: [],
  analysisAt: null,
  activeTheme: "",
  minScore: 4,
  filterText: "",
  selectedField: "",
  selectedDataset: null,
  selectedPairKey: null,
  selectedPairIsAuto: true,
  datasetById: new Map(),
  degreeById: new Map(),
  linkByPair: new Map(),
  pairRows: [],
  _viewState: null,
  _viewStamp: 0,
  _pairRowsCache: null,
  _adjacencyCache: null,
  _pathThemeCache: new Map(),
  _titleCache: new Map(),
  _searchTextCache: new Map(),
  _themeOptionsSignature: "",
  _fieldOptionsSignature: "",
  fieldRenderLimit: 80,
  pairRenderLimit: 120,
  pathRenderLimit: 40,
};

const FIELD_RENDER_STEP = 80;
const PAIR_RENDER_STEP = 120;
const PATH_RENDER_STEP = 40;
const TITLE_CACHE_SIZE = 700;
const SEARCH_TEXT_CACHE_SIZE = 700;
const PATH_THEME_CACHE_SIZE = 120;

const THEME_COLORS = {
  "Acesso & Produção": "#2f6fdb",
  "Recursos Humanos": "#0a7a58",
  "Finanças & Compras": "#a95a06",
  "Qualidade & Resultados": "#6b2fa3",
  "Saúde Pública & Emergência": "#d1434e",
  "Rede & Território": "#2d8ab7",
  Outros: "#6d7787",
};

const datasetFilter = document.getElementById("crossDatasetFilter");
const themeFilter = document.getElementById("crossThemeFilter");
const fieldFilter = document.getElementById("crossFieldFilter");
const minScore = document.getElementById("crossMinScore");
const minScoreValue = document.getElementById("crossMinScoreValue");
const statusEl = document.getElementById("crossStatus");
const reloadButton = document.getElementById("crossReloadButton");
const exportButton = document.getElementById("crossExportButton");
const clearFieldButton = document.getElementById("crossClearField");
const clearDatasetButton = document.getElementById("crossClearDataset");
const fieldListEl = document.getElementById("crossFieldList");
const pairTable = document.getElementById("pairTable");
const pairTitle = document.getElementById("pairTitle");
const pairDescription = document.getElementById("pairDescription");
const pairEmpty = document.getElementById("pairsEmpty");
const crossDetail = document.getElementById("crossDetail");
const pathMetaText = document.getElementById("crossPathMetaText");
const crossPaths = document.getElementById("crossPaths");
const recommendedPath = document.getElementById("recommendedPath");
const crossStatFields = document.getElementById("crossStatFields");
const crossStatPairs = document.getElementById("crossStatPairs");
const crossStatThemes = document.getElementById("crossStatThemes");
const crossStatFocus = document.getElementById("crossStatFocus");
const semanticGraph = document.getElementById("semanticModelGraph");
const semanticGraphMeta = document.getElementById("semanticGraphMeta");
const semanticGraphLegend = document.getElementById("semanticGraphLegend");

let analysisLoadTimer = null;
let filterTimer = null;
let resizeTimer = null;
const SVG_NS = "http://www.w3.org/2000/svg";

function clearElement(element) {
  element.replaceChildren();
}

function safeText(value) {
  return value == null ? "" : String(value);
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

function invalidateViewState() {
  state._viewState = null;
  state._viewStamp += 1;
  state._pairRowsCache = null;
  state._adjacencyCache = null;
  state._pathThemeCache = new Map();
  state._fieldOptionsSignature = "";
}

function resetRenderWindows({fields = true, pairs = true, paths = true} = {}) {
  if (fields) state.fieldRenderLimit = FIELD_RENDER_STEP;
  if (pairs) state.pairRenderLimit = PAIR_RENDER_STEP;
  if (paths) state.pathRenderLimit = PATH_RENDER_STEP;
}

function getViewState() {
  if (state._viewState) {
    return state._viewState;
  }

  const text = state.filterText.trim().toLowerCase();
  const datasets = [];
  for (const dataset of state.datasets) {
    if (state.activeTheme && dataset.mega_theme !== state.activeTheme) continue;
    if (text) {
      if (!getDatasetSearchText(dataset).includes(text)) continue;
    }
    datasets.push(dataset);
  }

  datasets.sort((a, b) => (state.degreeById.get(b.dataset_id) || 0) - (state.degreeById.get(a.dataset_id) || 0));
  const visibleDatasetIds = new Set(datasets.map((item) => item.dataset_id));
  const links = [];
  const fieldOptions = new Map();

  for (const link of state.links) {
    if (!visibleDatasetIds.has(link.source) || !visibleDatasetIds.has(link.target)) continue;
    links.push(link);

    const shared = link.shared_fields || [];
    for (const field of shared) {
      const current = fieldOptions.get(field);
      if (!current) {
        const datasetSet = new Set();
        datasetSet.add(link.source);
        datasetSet.add(link.target);
        fieldOptions.set(field, datasetSet);
      } else {
        current.add(link.source);
        current.add(link.target);
      }
    }
  }

  const fieldRows = [];
  let maxFieldCount = 0;
  fieldOptions.forEach((datasetIds, field) => {
    const ids = Array.from(datasetIds);
    if (ids.length >= 2) {
      const datasetCount = ids.length;
      if (datasetCount > maxFieldCount) maxFieldCount = datasetCount;
      fieldRows.push({field, datasetIds: ids, datasetCount});
    }
  });

  fieldRows.sort((a, b) => b.datasetCount - a.datasetCount || a.field.localeCompare(b.field));
  state._viewState = {datasets, visibleDatasetIds, links, fieldRows, maxFieldCount};
  return state._viewState;
}

function debounceLoadAnalysis() {
  if (analysisLoadTimer) clearTimeout(analysisLoadTimer);
  analysisLoadTimer = setTimeout(() => loadAnalysis().catch(showError), 180);
}

function showError(message) {
  statusEl.textContent = message?.message || message || "Erro";
}

function getThemeColor(theme) {
  return THEME_COLORS[theme] || THEME_COLORS.Outros;
}

function formatCell(value) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number") return new Intl.NumberFormat("pt-PT").format(value);
  if (typeof value === "string" && value.length > 120) return `${value.slice(0, 120)}…`;
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function formatNumber(value) {
  return new Intl.NumberFormat("pt-PT").format(value || 0);
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

function compactTitle(value, max = 46) {
  if (!value) return "";
  return value.length > max ? `${value.slice(0, max - 1)}…` : value;
}

function classifySemanticField(field) {
  const normalized = safeText(field).toLowerCase();
  if (/(tempo|periodo|ano|data|trimestre|semana|mes|dia)/.test(normalized)) return "temporal";
  if (/(regiao|ars|uls|localizacao|geografica|concelho|distrito|postal|hospital)/.test(normalized)) return "territorial";
  if (/(entidade|instituicao|unidade|servico|grupo|fornecedor|utente|doente)/.test(normalized)) return "entidade";
  if (/(valor|total|taxa|numero|contagem|volume|quantidade|dias|encargos|custo|pvp)/.test(normalized)) return "medida";
  return "generico";
}

function semanticClassLabel(kind) {
  return {
    temporal: "Temporal",
    territorial: "Território",
    entidade: "Entidade",
    medida: "Medida",
    generico: "Genérico",
  }[kind] || "Genérico";
}

function fieldRisk(kind) {
  if (kind === "generico" || kind === "medida") return "validar granularidade";
  if (kind === "temporal" || kind === "territorial" || kind === "entidade") return "boa chave candidata";
  return "validar";
}

function pairKey(a, b) {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function buildPairRowsFromLinks(selectedDataset, selectedField, links, visibleDatasets) {
  if (selectedField) {
    const map = new Map();
    links.forEach((link) => {
      const a = link.source;
      const b = link.target;
      if (!visibleDatasets.has(a) || !visibleDatasets.has(b)) return;
      if (link.shared_fields?.includes(selectedField)) {
        map.set(pairKey(a, b), {
          source: a,
          target: b,
          score: link.score || 0,
          shared_fields: link.shared_fields || [],
        });
      }
    });

    return Array.from(map.values())
      .filter((link) => !selectedDataset || link.source === selectedDataset || link.target === selectedDataset)
      .sort((a, b) => b.score - a.score || a.source.localeCompare(b.source) || a.target.localeCompare(b.target))
      .slice(0, 240);
  }

  if (selectedDataset) {
    return links
      .filter((link) => link.source === selectedDataset || link.target === selectedDataset)
      .slice(0, 240);
  }

  return links.slice(0, 260);
}

function getPairRows(selectedDataset, selectedField, links, visibleDatasets) {
  const cacheKey = `${state._viewStamp}|${selectedDataset || ""}|${selectedField || ""}|${links.length}|${visibleDatasets.size || 0}`;
  if (state._pairRowsCache?.key === cacheKey) {
    return state._pairRowsCache.rows;
  }

  const rows = buildPairRowsFromLinks(selectedDataset, selectedField, links, visibleDatasets);
  state._pairRowsCache = {key: cacheKey, rows};
  return rows;
}

async function loadAnalysis() {
  statusEl.textContent = "A carregar…";
  try {
  const response = await fetch(`/api/analysis?min_score=${state.minScore}`);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Erro a carregar análise");

    state.datasets = payload.datasets || [];
    state.links = payload.links || [];
    state.opportunities = payload.opportunities || [];
    state.themes = payload.themes || [];
    state.analysisAt = payload.generated_at || Date.now();

  state.datasetById = new Map(state.datasets.map((dataset) => [dataset.dataset_id, dataset]));
  state.degreeById = buildDegreeMap(state.links);
  state.linkByPair = new Map();
  state.links.forEach((link) => {
    state.linkByPair.set(pairKey(link.source, link.target), link);
  });
  state._titleCache = new Map();
  state._searchTextCache = new Map();
  state._pathThemeCache = new Map();
  state._pairRowsCache = null;
  state._adjacencyCache = null;
  state._themeOptionsSignature = "";
  state._fieldOptionsSignature = "";
  state._viewStamp = 0;
  resetRenderWindows();
  invalidateViewState();

  if (state.selectedDataset && !state.datasetById.has(state.selectedDataset)) {
    setSelectedDataset(null);
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
    }
  const availableFields = getAvailableFieldOptions();
  if (state.selectedField && !availableFields.has(state.selectedField)) {
    state.selectedField = "";
  }
    statusEl.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
    renderAll();
  } catch (error) {
    statusEl.textContent = "Erro";
    pairDescription.textContent = error.message;
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

function ensureSelectionInFilter() {
  const visible = filteredDatasets();
  const visibleIds = getViewState().visibleDatasetIds;
  if (state.selectedDataset && !visibleIds.has(state.selectedDataset)) {
    setSelectedDataset(visible[0]?.dataset_id || null);
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
  }
  if (state.selectedPairKey) {
    const [a, b] = state.selectedPairKey.split("|");
    if (!visibleIds.has(a) || !visibleIds.has(b)) {
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
    }
  }
}

function filteredDatasets() {
  return getViewState().datasets;
}

function getAvailableFieldOptions() {
  const {rows} = getFilteredFieldRows();
  const mapped = new Map();
  rows.forEach((row) => {
    mapped.set(row.field, row.datasetIds);
  });
  return mapped;
}

function getFilteredFieldRows() {
  const view = getViewState();
  return {rows: view.fieldRows.slice(), maxCount: view.maxFieldCount};
}

function getVisibleLinks() {
  return getViewState().links;
}

function setSelectedDataset(datasetId) {
  if (state.selectedDataset === datasetId) {
    return;
  }
  state.selectedDataset = datasetId;
  state._pairRowsCache = null;
  state.pairRenderLimit = PAIR_RENDER_STEP;
  state.pathRenderLimit = PATH_RENDER_STEP;
  if (!datasetId) {
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
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

function renderThemeOptions() {
  const previous = themeFilter.value;
  const signature = `${state.activeTheme}|${state.themes.map((theme) => `${theme.theme}:${theme.dataset_count}`).join("|")}`;
  if (state._themeOptionsSignature === signature && themeFilter.options.length) {
    themeFilter.value = state.activeTheme || previous || "";
    return;
  }

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
  themeFilter.value = state.activeTheme || previous || "";
  state._themeOptionsSignature = signature;
}

function renderFieldOptions() {
  const previous = fieldFilter.value;
  const {rows} = getFilteredFieldRows();
  const signature = `${state.selectedField}|${state._viewStamp}|${rows.length}|${rows.slice(0, 40).map((row) => row.field).join("|")}`;
  if (state._fieldOptionsSignature === signature && fieldFilter.options.length) {
    fieldFilter.value = state.selectedField || previous || "";
    return;
  }

  clearElement(fieldFilter);
  const allFields = document.createElement("option");
  allFields.value = "";
  allFields.textContent = "Todos os campos";
  fieldFilter.appendChild(allFields);
  rows.slice(0, 500).forEach((row) => {
    const option = document.createElement("option");
    option.value = row.field;
    option.textContent = `${row.field} (${row.datasetCount})`;
    fieldFilter.appendChild(option);
  });

  if (previous && rows.some((row) => row.field === previous)) {
    fieldFilter.value = previous;
  } else {
    fieldFilter.value = state.selectedField || "";
  }
  state._fieldOptionsSignature = signature;
}

function renderSummary() {
  const visible = filteredDatasets();
  const {rows} = getFilteredFieldRows();
  const linkCount = getVisibleLinks().length;

  crossStatFields.textContent = rows.length.toLocaleString("pt-PT");
  crossStatPairs.textContent = linkCount.toLocaleString("pt-PT");
  crossStatThemes.textContent = `${visible.length}/${state.datasets.length}`;
  crossStatFocus.textContent = state.selectedDataset
    ? compactTitle(displayTitle(state.datasetById.get(state.selectedDataset)), 26)
    : (state.activeTheme || "Catálogo");

  let subtitle = "";
  if (state.selectedField) {
    subtitle = `Filtrados ao campo “${state.selectedField}”.`;
  } else if (state.selectedDataset) {
    subtitle = `Cruzamentos em torno de “${compactTitle(displayTitle(state.datasetById.get(state.selectedDataset)), 30)}”.`;
  } else {
    subtitle = "Filtrados por tema e pontuação mínima da ligação.";
  }

  pairDescription.textContent = subtitle;
}

function renderFieldList() {
  const {rows, maxCount} = getFilteredFieldRows();
  clearElement(fieldListEl);

  if (!rows.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem campos para este filtro.";
    fieldListEl.appendChild(empty);
    return;
  }

  const selectedIndex = state.selectedField ? rows.findIndex((row) => row.field === state.selectedField) : -1;
  if (selectedIndex >= state.fieldRenderLimit) {
    state.fieldRenderLimit = Math.min(rows.length, selectedIndex + 1);
  }

  const visibleRows = rows.slice(0, state.fieldRenderLimit);
  const fragment = document.createDocumentFragment();
  visibleRows.forEach((row) => {
    const kind = classifySemanticField(row.field);
    const item = document.createElement("button");
    item.type = "button";
    item.className = `field-item semantic-field-${kind}`;
    if (state.selectedField === row.field) item.classList.add("active");

    const head = document.createElement("div");
    head.className = "field-item-head";
    const left = document.createElement("strong");
    left.textContent = row.field;
    const right = document.createElement("span");
    right.className = "meta";
    right.textContent = `${row.datasetCount} datasets`;
    head.append(left, right);

    const meta = document.createElement("div");
    meta.className = "field-meta";
    meta.textContent = `Cobertura por tema: ${formatUniqueThemes(row.datasetIds).join(", ")}`;

    const badges = document.createElement("div");
    badges.className = "field-badge-row";
    const kindBadge = document.createElement("span");
    kindBadge.className = "field-kind";
    kindBadge.textContent = semanticClassLabel(kind);
    const riskBadge = document.createElement("span");
    riskBadge.className = "field-risk";
    riskBadge.textContent = fieldRisk(kind);
    badges.append(kindBadge, riskBadge);

    const track = document.createElement("div");
    track.className = "field-progress-track";
    const bar = document.createElement("div");
    bar.className = "field-progress-bar";
    const ratio = maxCount ? (row.datasetCount / maxCount) * 100 : 0;
    bar.style.width = `${ratio}%`;
    track.appendChild(bar);

    item.append(head, badges, meta, track);
    item.onclick = () => {
      const next = state.selectedField === row.field ? "" : row.field;
      state.selectedField = next;
      if (fieldFilter.value !== next) {
        fieldFilter.value = next;
      }
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
      state.pairRenderLimit = PAIR_RENDER_STEP;
      state.pathRenderLimit = PATH_RENDER_STEP;
      renderAll();
    };
    fragment.appendChild(item);
  });
  fieldListEl.appendChild(fragment);

  if (visibleRows.length < rows.length) {
    fieldListEl.appendChild(createShowMoreButton(
      `Mostrar mais ${Math.min(FIELD_RENDER_STEP, rows.length - visibleRows.length)} campos`,
      () => {
        state.fieldRenderLimit = Math.min(rows.length, state.fieldRenderLimit + FIELD_RENDER_STEP);
        renderFieldList();
      },
    ));
  }
}

function formatUniqueThemes(datasetIds) {
  const cacheKey = `themes|${datasetIds.slice().sort().join("|")}`;
  const cached = state._pathThemeCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  const themeSet = new Set();
  datasetIds.forEach((datasetId) => {
    const dataset = state.datasetById.get(datasetId);
    themeSet.add(dataset?.mega_theme || "Outros");
  });
  const value = Array.from(themeSet).sort().slice(0, 3);
  setBoundedCache(state._pathThemeCache, cacheKey, value, PATH_THEME_CACHE_SIZE);
  return value;
}

function renderPairTable() {
  const view = getViewState();
  const links = view.links;
  const visibleIds = view.visibleDatasetIds;
  const rows = getPairRows(state.selectedDataset, state.selectedField, links, visibleIds);
  state.pairRows = rows;
  const thead = pairTable.querySelector("thead");
  const tbody = pairTable.querySelector("tbody");
  clearElement(thead);
  clearElement(tbody);

  if (!rows.length) {
    pairEmpty.hidden = false;
    pairTable.hidden = true;
    pairTitle.textContent = "Sem pares de cruzamento";
    pairDescription.textContent = state.selectedField
      ? `Campo "${state.selectedField}" não encontra pares visíveis para os filtros atuais.`
      : "Sem ligações neste filtro.";
    clearElement(crossPaths);
    const pathEmpty = document.createElement("div");
    pathEmpty.className = "empty-state";
    pathEmpty.textContent = "Sem caminhos para mostrar.";
    crossPaths.appendChild(pathEmpty);
    renderCrossDetail(null);
    return;
  }

  pairEmpty.hidden = true;
  pairTable.hidden = false;

  if (state.selectedField) {
    pairTitle.textContent = `Pares com “${state.selectedField}”`;
  } else if (state.selectedDataset) {
    pairTitle.textContent = `Pares relacionados com ${compactTitle(displayTitle(state.datasetById.get(state.selectedDataset)), 36)}`;
  } else {
    pairTitle.textContent = "Top pares de cruzamento";
  }

  const rowNode = document.createElement("tr");
  ["Dataset A", "Dataset B", "Score", "Áreas", "Chaves"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    rowNode.appendChild(th);
  });
  thead.appendChild(rowNode);

  const topPairs = rows;
  const sourceFocus = state.selectedDataset;
  if (!state.selectedPairKey && topPairs.length) {
    const best = topPairs[0];
    state.selectedPairKey = pairKey(best.source, best.target);
    state.selectedPairIsAuto = true;
  }

  if (state.selectedPairKey) {
    const selectedIndex = topPairs.findIndex((pair) => pairKey(pair.source, pair.target) === state.selectedPairKey);
    if (selectedIndex === -1) {
      state.selectedPairKey = pairKey(topPairs[0].source, topPairs[0].target);
      state.selectedPairIsAuto = true;
    } else if (selectedIndex >= state.pairRenderLimit) {
      state.pairRenderLimit = Math.min(topPairs.length, selectedIndex + 1);
    }
  }

  const visiblePairs = topPairs.slice(0, state.pairRenderLimit);
  const fragment = document.createDocumentFragment();
  visiblePairs.forEach((row) => {
    const line = document.createElement("tr");
    const datasetA = row.source;
    const datasetB = row.target;
    line.className = "pair-row";
    const key = pairKey(datasetA, datasetB);
    line.dataset.key = key;
    if (state.selectedPairKey === key) line.classList.add("active");

    const a = state.datasetById.get(datasetA);
    const b = state.datasetById.get(datasetB);
    const datasetATitle = compactTitle(displayTitle(a), 40);
    const datasetBTitle = compactTitle(displayTitle(b), 40);
    const shared = row.shared_fields || [];
    const themeA = a?.mega_theme || "Outros";
    const themeB = b?.mega_theme || "Outros";
    const themes = themeA === themeB ? [themeA] : [themeA, themeB];

    const rowElements = [
      datasetATitle,
      datasetBTitle,
      row.score?.toString() || "0",
      themes.join(" · "),
      shared,
    ];

    rowElements.forEach((value, index) => {
      const cell = document.createElement("td");
      if (index === 4) {
        const fieldList = document.createElement("div");
        fieldList.className = "field-pill-list";
        if (Array.isArray(value) && value.length) {
          value.slice(0, 5).forEach((field) => {
            const pill = document.createElement("span");
            pill.className = "field-pill";
            pill.textContent = formatCell(field);
            fieldList.appendChild(pill);
          });
        } else {
          const emptyPill = document.createElement("span");
          emptyPill.className = "field-pill";
          emptyPill.textContent = "Sem campos";
          fieldList.appendChild(emptyPill);
        }
        cell.appendChild(fieldList);
      } else {
        if (index === 0 || index === 1) {
          const icon = document.createElement("button");
          icon.type = "button";
          icon.className = "ghost-button pair-focus-button";
          icon.textContent = "foco";
          icon.onclick = (event) => {
            event.stopPropagation();
            setSelectedDataset(index === 0 ? datasetA : datasetB);
            state.selectedPairKey = null;
            state.selectedPairIsAuto = true;
            renderAll();
          };
          const wrapper = document.createElement("div");
          wrapper.className = "pair-focus-cell";
          const label = document.createElement("span");
          label.textContent = value;
          wrapper.append(label, icon);
          cell.appendChild(wrapper);
        } else {
          cell.textContent = value;
        }
      }
      line.appendChild(cell);
    });

    line.addEventListener("click", () => {
      state.selectedPairKey = key;
      state.selectedPairIsAuto = false;
      setSelectedDataset(sourceFocus);
      renderPairSelection(key, line);
    });
    fragment.appendChild(line);
  });
  tbody.appendChild(fragment);

  if (state.selectedPairKey) {
    const selectedRow = tbody.querySelector(`tr[data-key="${CSS.escape(state.selectedPairKey)}"]`);
    if (selectedRow) {
      selectedRow.classList.add("active");
    }
  }
  if (visiblePairs.length < topPairs.length) {
    const moreRow = document.createElement("tr");
    moreRow.className = "pair-more-row";
    const moreCell = document.createElement("td");
    moreCell.colSpan = 5;
    moreCell.appendChild(createShowMoreButton(
      `Mostrar mais ${Math.min(PAIR_RENDER_STEP, topPairs.length - visiblePairs.length)} pares`,
      () => {
        state.pairRenderLimit = Math.min(topPairs.length, state.pairRenderLimit + PAIR_RENDER_STEP);
        renderPairTable();
      },
    ));
    moreRow.appendChild(moreCell);
    tbody.appendChild(moreRow);
  }
  renderCrossDetail();
  renderPaths();
}

function renderPairSelection(key, rowElement) {
  if (key === state.selectedPairKey && rowElement) {
    renderCrossDetail();
    renderPaths();
    renderSemanticGraph();
    return;
  }
  pairTable.querySelectorAll("tr.pair-row").forEach((row) => row.classList.remove("active"));
  if (rowElement) {
    rowElement.classList.add("active");
  } else {
    const row = pairTable.querySelector(`tr[data-key="${CSS.escape(key)}"]`);
    if (row) row.classList.add("active");
  }
  renderCrossDetail();
  renderPaths();
  renderSemanticGraph();
}

function semanticGraphData() {
  const view = getViewState();
  const links = view.links;
  const visibleIds = view.visibleDatasetIds;
  let edges = [];
  let mode = "top";

  if (state.selectedPairKey && !state.selectedPairIsAuto) {
    const edge = state.linkByPair.get(state.selectedPairKey);
    if (edge && visibleIds.has(edge.source) && visibleIds.has(edge.target)) {
      edges = [edge];
      mode = "par selecionado";
    }
  } else if (state.selectedDataset) {
    edges = links
      .filter((link) => link.source === state.selectedDataset || link.target === state.selectedDataset)
      .slice(0, 8);
    mode = "dataset em foco";
  } else if (state.selectedField) {
    edges = links
      .filter((link) => (link.shared_fields || []).includes(state.selectedField))
      .slice(0, 10);
    mode = "campo em foco";
  } else {
    edges = links.slice(0, 8);
  }

  const datasetIds = new Set();
  const fieldNames = new Set();
  edges.forEach((edge) => {
    datasetIds.add(edge.source);
    datasetIds.add(edge.target);
    (edge.shared_fields || []).slice(0, mode === "par selecionado" ? 8 : 4).forEach((field) => fieldNames.add(field));
  });

  const datasets = Array.from(datasetIds)
    .map((id) => state.datasetById.get(id) || {dataset_id: id, title: id, mega_theme: "Outros"})
    .slice(0, 12);
  const fields = Array.from(fieldNames).slice(0, 12);

  return {mode, datasets, fields, edges};
}

function svgNode(tag, attributes = {}) {
  const node = document.createElementNS(SVG_NS, tag);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  return node;
}

function renderSemanticGraph() {
  if (!semanticGraph) return;
  const {mode, datasets, fields, edges} = semanticGraphData();
  semanticGraph.replaceChildren();

  const width = Math.max(720, semanticGraph.closest(".semantic-graph-wrap")?.clientWidth || 720);
  const height = Math.max(300, Math.min(420, 220 + fields.length * 12 + datasets.length * 8));
  semanticGraph.setAttribute("viewBox", `0 0 ${width} ${height}`);
  semanticGraph.setAttribute("preserveAspectRatio", "xMidYMid meet");

  semanticGraphMeta.textContent = `${mode}: ${datasets.length} dataset(s), ${fields.length} campo(s) de ligação, ${edges.length} relação(ões).`;
  renderSemanticLegend(fields);

  if (!datasets.length || !fields.length) {
    const empty = svgNode("text", {x: width / 2, y: height / 2, "text-anchor": "middle", fill: "#657489"});
    empty.textContent = "Sem relações semânticas para os filtros atuais.";
    semanticGraph.appendChild(empty);
    return;
  }

  const sideInset = Math.max(188, Math.min(220, width * 0.24));
  const leftX = sideInset;
  const rightX = width - sideInset;
  const fieldX = width / 2;
  const datasetLabelMax = width < 820 ? 16 : 18;
  const datasetYStep = height / (datasets.length + 1);
  const fieldYStep = height / (fields.length + 1);
  const datasetPositions = new Map();
  const fieldPositions = new Map();
  const leftCount = Math.ceil(datasets.length / 2);

  datasets.forEach((dataset, index) => {
    const side = index < leftCount ? "left" : "right";
    const localIndex = side === "left" ? index : index - leftCount;
    const totalOnSide = side === "left" ? leftCount : datasets.length - leftCount;
    const y = height * ((localIndex + 1) / (totalOnSide + 1));
    datasetPositions.set(dataset.dataset_id, {x: side === "left" ? leftX : rightX, y, side});
  });

  fields.forEach((field, index) => {
    fieldPositions.set(field, {
      x: fieldX,
      y: fieldYStep * (index + 1),
      kind: classifySemanticField(field),
    });
  });

  const edgeLayer = svgNode("g", {"class": "semantic-edge-layer"});
  const fieldLayer = svgNode("g", {"class": "semantic-field-layer"});
  const datasetLayer = svgNode("g", {"class": "semantic-dataset-layer"});
  semanticGraph.append(edgeLayer, fieldLayer, datasetLayer);

  const drawn = new Set();
  edges.forEach((edge) => {
    const sources = [edge.source, edge.target];
    (edge.shared_fields || []).slice(0, 8).forEach((field) => {
      const fieldPos = fieldPositions.get(field);
      if (!fieldPos) return;
      sources.forEach((datasetId) => {
        const datasetPos = datasetPositions.get(datasetId);
        if (!datasetPos) return;
        const drawKey = `${datasetId}|${field}`;
        if (drawn.has(drawKey)) return;
        drawn.add(drawKey);
        const path = svgNode("path", {
          d: `M ${datasetPos.x} ${datasetPos.y} C ${(datasetPos.x + fieldPos.x) / 2} ${datasetPos.y}, ${(datasetPos.x + fieldPos.x) / 2} ${fieldPos.y}, ${fieldPos.x} ${fieldPos.y}`,
          class: `semantic-edge semantic-edge-${fieldPos.kind}`,
          "stroke-width": String(Math.max(1.4, Math.min(5, (edge.score || 1) / 4))),
        });
        edgeLayer.appendChild(path);
      });
    });
  });

  fields.forEach((field) => {
    const pos = fieldPositions.get(field);
    const group = svgNode("g", {class: `semantic-field semantic-field-${pos.kind}`, transform: `translate(${pos.x}, ${pos.y})`});
    const text = compactTitle(field, 26);
    const widthBox = Math.max(92, Math.min(220, text.length * 7 + 28));
    group.appendChild(svgNode("rect", {x: -widthBox / 2, y: -15, width: widthBox, height: 30, rx: 6}));
    const label = svgNode("text", {"text-anchor": "middle", y: 4});
    label.textContent = text;
    group.appendChild(label);
    group.addEventListener("click", () => {
      state.selectedField = field;
      if (fieldFilter) fieldFilter.value = field;
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
      resetRenderWindows({fields: false});
      renderAll();
    });
    fieldLayer.appendChild(group);
  });

  datasets.forEach((dataset) => {
    const pos = datasetPositions.get(dataset.dataset_id);
    const group = svgNode("g", {class: "semantic-dataset", transform: `translate(${pos.x}, ${pos.y})`});
    group.style.setProperty("--node-theme", getThemeColor(dataset.mega_theme || "Outros"));
    group.appendChild(svgNode("circle", {r: 13}));
    const title = svgNode("text", {
      x: pos.side === "left" ? -18 : 18,
      y: 4,
      "text-anchor": pos.side === "left" ? "end" : "start",
    });
    title.textContent = compactTitle(displayTitle(dataset), datasetLabelMax);
    group.appendChild(title);
    group.addEventListener("click", () => {
      setSelectedDataset(dataset.dataset_id);
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
      resetRenderWindows({fields: false});
      renderAll();
    });
    datasetLayer.appendChild(group);
  });
}

function renderSemanticLegend(fields) {
  if (!semanticGraphLegend) return;
  clearElement(semanticGraphLegend);
  const kinds = Array.from(new Set(fields.map(classifySemanticField)));
  (kinds.length ? kinds : ["temporal", "territorial", "entidade", "medida", "generico"]).forEach((kind) => {
    const item = document.createElement("span");
    item.className = `semantic-legend-item semantic-legend-${kind}`;
    item.textContent = `${semanticClassLabel(kind)} · ${fieldRisk(kind)}`;
    semanticGraphLegend.appendChild(item);
  });
}

function renderCrossDetail() {
  if (!state.selectedPairKey) {
    clearElement(crossDetail);
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Abre uma linha para ver o detalhe.";
    crossDetail.appendChild(empty);
    return;
  }
  const [a, b] = state.selectedPairKey.split("|");
  const edge = state.linkByPair.get(state.selectedPairKey);
  if (!edge) {
    clearElement(crossDetail);
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Ligação não encontrada na seleção atual.";
    crossDetail.appendChild(empty);
    return;
  }

  const datasetA = state.datasetById.get(a);
  const datasetB = state.datasetById.get(b);
  const fields = edge.shared_fields || [];

  clearElement(crossDetail);
  const connection = document.createElement("div");
  connection.className = "cross-detail-item";

  const title = document.createElement("h3");
  title.textContent = "Conexão direta";
  const titlesRow = document.createElement("div");
  titlesRow.className = "cross-detail-title";
  titlesRow.textContent = `${compactTitle(displayTitle(datasetA), 80)} ↔ ${compactTitle(displayTitle(datasetB), 80)}`;

  const meta = document.createElement("div");
  meta.className = "meta";
  const themeA = document.createElement("span");
  themeA.className = "tag theme-tag";
  themeA.textContent = safeText(datasetA?.mega_theme || "Outros");
  const themeB = document.createElement("span");
  themeB.className = "tag theme-tag";
  themeB.textContent = safeText(datasetB?.mega_theme || "Outros");
  meta.append(themeA, themeB);

  const score = document.createElement("p");
  const scoreValue = document.createElement("span");
  scoreValue.className = "pair-score";
  scoreValue.textContent = safeText(edge.score || 0);
  score.textContent = "Força da ligação: ";
  score.append(scoreValue, document.createTextNode("."));

  const sharedCount = document.createElement("p");
  sharedCount.textContent = `Registo da ligação para este contexto: ${fields.length} campo(s) partilhado(s).`;
  connection.append(title, titlesRow, meta, score, sharedCount);

  const sharedSection = document.createElement("div");
  sharedSection.className = "cross-detail-item";
  const sharedTitle = document.createElement("h3");
  sharedTitle.textContent = "Campos partilhados";
  const sharedList = document.createElement("div");
  sharedList.className = "field-pill-list";
  if (fields.length) {
    fields.forEach((field) => {
      const pill = document.createElement("span");
      pill.className = "field-pill";
      pill.textContent = safeText(field);
      sharedList.appendChild(pill);
    });
  } else {
    const none = document.createElement("span");
    none.className = "field-pill";
    none.textContent = "Nenhum";
    sharedList.appendChild(none);
  }
  sharedSection.append(sharedTitle, sharedList);

  const recipe = buildJoinRecipe(fields, datasetA, datasetB);
  const suggestion = document.createElement("div");
  suggestion.className = "cross-detail-item join-recipe";
  const suggestionTitle = document.createElement("h3");
  suggestionTitle.textContent = "Receita de join";
  const recipeGrid = document.createElement("div");
  recipeGrid.className = "join-recipe-grid";
  [
    ["Chave sugerida", recipe.keys],
    ["Tipo recomendado", recipe.joinType],
    ["Validações", recipe.validations],
    ["Risco", recipe.risk],
  ].forEach(([label, value]) => {
    const block = document.createElement("div");
    const blockLabel = document.createElement("span");
    blockLabel.textContent = label;
    const blockValue = document.createElement("strong");
    blockValue.textContent = value;
    block.append(blockLabel, blockValue);
    recipeGrid.appendChild(block);
  });
  const suggestionMeta = document.createElement("p");
  suggestionMeta.className = "meta";
  suggestionMeta.textContent = `Registo de análise: ${new Date((state.analysisAt || Date.now()) * 1000).toLocaleString("pt-PT")}`;
  suggestion.append(suggestionTitle, recipeGrid, suggestionMeta);

  crossDetail.append(connection, sharedSection, suggestion);
}

function buildJoinRecipe(fields, datasetA, datasetB) {
  const kinds = fields.map(classifySemanticField);
  const preferred = fields
    .filter((field) => {
      const kind = classifySemanticField(field);
      return kind === "entidade" || kind === "territorial" || kind === "temporal";
    })
    .slice(0, 4);
  const hasMeasure = kinds.includes("medida");
  const hasGeneric = kinds.includes("generico");
  const hasEntityOrTerritory = kinds.includes("entidade") || kinds.includes("territorial");
  const crossTheme = datasetA?.mega_theme && datasetB?.mega_theme && datasetA.mega_theme !== datasetB.mega_theme;

  return {
    keys: (preferred.length ? preferred : fields.slice(0, 3)).join(", ") || "validar chave manualmente",
    joinType: hasEntityOrTerritory ? "left join com reconciliação de entidades" : "inner join exploratório",
    validations: "tipo, nulos, duplicados e cardinalidade 1:N",
    risk: hasMeasure || hasGeneric || crossTheme ? "médio: granularidade a confirmar" : "baixo: chave candidata estável",
  };
}

function renderPaths() {
  if (!state.selectedPairKey && !state.selectedDataset) {
    pathMetaText.textContent = "Escolhe um foco para ver caminhos de 2 saltos.";
    renderRecommendedPath([]);
    clearElement(crossPaths);
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Escolhe um foco para calcular caminhos.";
    crossPaths.appendChild(empty);
    return;
  }

  const anchors = [];
  const view = getViewState();
  const visibleLinks = view.links;
  const visibleIds = view.visibleDatasetIds;
  const adjacency = getAdjacency(visibleLinks);
  let title = "";

  if (state.selectedPairKey) {
    const [a, b] = state.selectedPairKey.split("|");
    if (!visibleIds.has(a) || !visibleIds.has(b)) {
      pathMetaText.textContent = "O par selecionado não está no filtro atual.";
      renderRecommendedPath([]);
      clearElement(crossPaths);
      const empty = document.createElement("div");
      empty.className = "empty-state";
      empty.textContent = "Sem foco válido para cálculo de cadeias.";
      crossPaths.appendChild(empty);
      return;
    }
    title = `Cadeias de 2 saltos entre ${compactTitle(displayTitle(state.datasetById.get(a)), 38)} e ${compactTitle(displayTitle(state.datasetById.get(b)), 38)}.`;
    const byMiddle = new Map();
    (adjacency[a]?.edges || []).forEach((edgeToA) => {
      const middle = edgeToA.other;
      if (middle === b) return;
      const linkFromMiddleToB = adjacency[middle]?.index?.get(b);
      if (!linkFromMiddleToB) return;
      byMiddle.set(middle, {
        path: [a, middle, b],
        score: (edgeToA.score || 0) + (linkFromMiddleToB.score || 0),
        connectors: [edgeToA.fields || [], linkFromMiddleToB.fields || []],
      });
    });

    anchors.push(...Array.from(byMiddle.values()));
  } else if (state.selectedDataset) {
    const seed = state.selectedDataset;
    title = `Caminhos de 2 saltos a partir de ${compactTitle(displayTitle(state.datasetById.get(seed)), 52)}.`;
    const twoStep = [];
    (adjacency[seed]?.edges || []).forEach((edge1) => {
      const middle = edge1.other;
      (adjacency[middle]?.edges || []).forEach((edge2) => {
        const end = edge2.other;
        if (end === seed) return;
        twoStep.push({
          path: [seed, middle, end],
          score: (edge1.score || 0) + (edge2.score || 0),
          connectors: [edge1.fields || [], edge2.fields || []],
        });
      });
    });
    anchors.push(...twoStep);
  }

  anchors.sort((p1, p2) => p2.score - p1.score);
  const topPaths = anchors.slice(0, state.pathRenderLimit);
  renderRecommendedPath(anchors);

  pathMetaText.textContent = `${title} A mostrar ${topPaths.length}/${anchors.length} resultados com filtros ativos.`;
  if (!topPaths.length) {
    clearElement(crossPaths);
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem caminhos de 2 saltos para o foco atual.";
    crossPaths.appendChild(empty);
    return;
  }

  clearElement(crossPaths);
  const fragment = document.createDocumentFragment();
  topPaths.forEach((path) => {
    const item = document.createElement("div");
    item.className = "cross-path-item";
    const [first, middle, last] = path.path;
    const firstTitle = compactTitle(displayTitle(state.datasetById.get(first)), 42);
    const middleTitle = compactTitle(displayTitle(state.datasetById.get(middle)), 42);
    const lastTitle = compactTitle(displayTitle(state.datasetById.get(last)), 42);
    const pathFieldsA = (path.connectors[0] || []).slice(0, 3).join(", ") || "—";
    const pathFieldsB = (path.connectors[1] || []).slice(0, 3).join(", ") || "—";

    const header = document.createElement("div");
    header.className = "meta";
    const strength = document.createElement("strong");
    strength.textContent = safeText(path.score);
    header.append(strength, document.createTextNode(" pontos potenciais"));

    const route = document.createElement("div");
    route.textContent = `${firstTitle} → ${middleTitle} → ${lastTitle}`;

    const pathKeys = document.createElement("p");
    pathKeys.textContent = `Chaves: ${pathFieldsA} / ${pathFieldsB}`;

    item.append(header, route, pathKeys);
    item.addEventListener("click", () => {
        const candidatePair = pairKey(first, last);
        if (state.linkByPair.has(candidatePair)) {
          if (state.selectedPairKey !== candidatePair) {
            setSelectedDataset(null);
            state.selectedPairKey = candidatePair;
            state.selectedPairIsAuto = false;
            renderAll();
          }
        } else {
          setSelectedDataset(last);
          state.selectedPairKey = null;
          state.selectedPairIsAuto = true;
          renderAll();
        }
    });
    fragment.appendChild(item);
  });
  crossPaths.appendChild(fragment);

  if (topPaths.length < anchors.length) {
    crossPaths.appendChild(createShowMoreButton(
      `Mostrar mais ${Math.min(PATH_RENDER_STEP, anchors.length - topPaths.length)} caminhos`,
      () => {
        state.pathRenderLimit = Math.min(anchors.length, state.pathRenderLimit + PATH_RENDER_STEP);
        renderPaths();
      },
    ));
  }
}

function renderRecommendedPath(paths) {
  if (!recommendedPath) return;
  clearElement(recommendedPath);
  const title = document.createElement("strong");
  title.textContent = "Caminho recomendado";

  if (state.selectedPairKey) {
    const edge = state.linkByPair.get(state.selectedPairKey);
    const direct = document.createElement("p");
    direct.className = "meta";
    direct.textContent = edge
      ? `Melhor par direto selecionado: score ${edge.score || 0}, ${(edge.shared_fields || []).slice(0, 3).join(", ") || "chave a validar"}.`
      : "Par direto indisponível nos filtros atuais.";
    recommendedPath.append(title, direct);
  } else {
    recommendedPath.appendChild(title);
  }

  const best = (paths || [])[0];
  if (!best) {
    const empty = document.createElement("p");
    empty.className = "meta";
    empty.textContent = state.selectedDataset || state.selectedPairKey
      ? "Sem caminho de 2 saltos forte para este foco."
      : "Escolhe dataset, campo ou par para recomendar caminho.";
    recommendedPath.appendChild(empty);
    return;
  }

  const route = document.createElement("div");
  route.className = "recommended-route";
  route.textContent = best.path.map((id) => compactTitle(displayTitle(state.datasetById.get(id)), 24)).join(" → ");
  const reason = document.createElement("p");
  reason.className = "meta";
  const keys = best.connectors.flat().slice(0, 4).join(", ") || "chaves a validar";
  reason.textContent = `Melhor 2 saltos: score ${best.score}. Razão: maior força acumulada e chaves comuns (${keys}).`;
  recommendedPath.append(route, reason);
}

function buildAdjacency() {
  const links = arguments.length ? arguments[0] : getVisibleLinks();
  const adjacency = {};
  links.forEach((link) => {
    const linkFields = link.shared_fields || [];
    const add = (source, target) => {
      const node = adjacency[source] || {edges: [], index: new Map()};
      node.edges.push({
        other: target,
        score: link.score || 0,
        fields: linkFields,
      });
      if (!node.index.has(target)) {
        node.index.set(target, {score: link.score || 0, fields: linkFields});
      }
      adjacency[source] = node;
    };
    add(link.source, link.target);
    add(link.target, link.source);
  });
  return adjacency;
}

function getAdjacency(links = getVisibleLinks()) {
  const cacheKey = `${state._viewStamp}|${links.length}`;
  if (state._adjacencyCache?.key === cacheKey) {
    return state._adjacencyCache.adjacency;
  }
  const adjacency = buildAdjacency(links);
  state._adjacencyCache = {key: cacheKey, adjacency};
  return adjacency;
}

function renderAll() {
  ensureSelectionInFilter();
  renderThemeOptions();
  renderFieldOptions();
  renderSummary();
  renderFieldList();
  renderPairTable();
  renderSemanticGraph();
}

function setupEvents() {
  minScore.value = state.minScore;
  minScoreValue.textContent = state.minScore;

  minScore.addEventListener("input", () => {
    state.minScore = Number(minScore.value);
    minScoreValue.textContent = state.minScore;
    resetRenderWindows();
    debounceLoadAnalysis();
  });

  datasetFilter.addEventListener("input", () => {
    const next = datasetFilter.value;
    clearTimeout(filterTimer);
    filterTimer = setTimeout(() => {
      state.filterText = next;
      invalidateViewState();
      state.selectedPairKey = null;
      state.selectedPairIsAuto = true;
      resetRenderWindows();
      renderAll();
    }, 140);
  });

  themeFilter.addEventListener("change", () => {
    const nextTheme = themeFilter.value || "";
    if (state.activeTheme === nextTheme) {
      return;
    }
    state.activeTheme = nextTheme;
    invalidateViewState();
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
    setSelectedDataset(null);
    resetRenderWindows();
    renderAll();
  });

  fieldFilter.addEventListener("change", () => {
    state.selectedField = fieldFilter.value || "";
    invalidateViewState();
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
    resetRenderWindows({fields: false});
    renderAll();
  });

  clearFieldButton.addEventListener("click", () => {
    state.selectedField = "";
    if (fieldFilter) fieldFilter.value = "";
    invalidateViewState();
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
    resetRenderWindows({fields: false});
    renderAll();
  });

  clearDatasetButton.addEventListener("click", () => {
    setSelectedDataset(null);
    invalidateViewState();
    state.selectedPairKey = null;
    state.selectedPairIsAuto = true;
    resetRenderWindows({fields: false});
    renderAll();
  });

  reloadButton.addEventListener("click", () => {
    loadAnalysis().catch(showError);
  });

  exportButton.addEventListener("click", () => {
    const payload = {
      generated_at: state.analysisAt,
      filter: {
        theme: state.activeTheme || "Todos",
        min_score: state.minScore,
        dataset_search: state.filterText || "",
        focused_field: state.selectedField || "",
        focused_dataset: state.selectedDataset || "",
      },
      pairs: state.pairRows.map((pair) => ({
        source: pair.source,
        target: pair.target,
        score: pair.score,
        shared_fields: pair.shared_fields || [],
      })),
    };

    const blob = new Blob([JSON.stringify(payload, null, 2)], {type: "application/json"});
    const anchor = document.createElement("a");
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = `crosswalk-${state.activeTheme || "catalogo"}-${stamp}.json`;
    anchor.click();
    URL.revokeObjectURL(anchor.href);
  });

  window.addEventListener("resize", () => {
    if (!state.datasets.length) return;
    if (resizeTimer) clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => renderAll(), 120);
  });
}

setupEvents();
loadAnalysis();
