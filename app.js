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
};
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

const tooltip = document.createElement("div");
tooltip.style.position = "fixed";
tooltip.style.pointerEvents = "none";
tooltip.style.padding = "8px 10px";
tooltip.style.background = "#102033";
tooltip.style.borderRadius = "6px";
tooltip.style.color = "#fff";
tooltip.style.fontSize = "12px";
tooltip.style.opacity = "0";
tooltip.style.transition = "opacity 0.18s ease";
tooltip.style.maxWidth = "340px";
tooltip.style.zIndex = "10";
document.body.appendChild(tooltip);

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
  if (!state.selectedDataset || !state.datasetById.has(state.selectedDataset)) {
    state.selectedDataset = filteredDatasets()[0]?.dataset_id || null;
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
  const value = dataset?.title || dataset?.dataset_id || "";
  return value
    .replace(/-/g, " ")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
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

  state.selectedDataset = fallback;
  state.recentData = null;
  if (fallback) {
    loadRecentRecords(fallback).catch(showRecordsError);
  }
  return true;
}

function filteredDatasets() {
  const value = state.filterText.trim().toLowerCase();
  const list = state.datasets.filter((item) => {
    if (state.activeTheme && item.mega_theme !== state.activeTheme) return false;
    if (!value) return true;
    const text = `${item.dataset_id} ${displayTitle(item)} ${(item.fields || []).join(" ")}`.toLowerCase();
    return text.includes(value);
  });

  return list.sort((a, b) => {
    const degreeDelta = (state.degreeById.get(b.dataset_id) || 0) - (state.degreeById.get(a.dataset_id) || 0);
    if (degreeDelta !== 0) return degreeDelta;
    return displayTitle(a).localeCompare(displayTitle(b));
  });
}

function visibleLinks() {
  const allowed = new Set(filteredDatasets().map((item) => item.dataset_id));
  return state.links.filter((edge) => allowed.has(edge.source) && allowed.has(edge.target));
}

function renderThemeOptions() {
  const current = themeFilter.value;
  themeFilter.innerHTML = '<option value="">Todos</option>';
  state.themes.forEach((theme) => {
    const option = document.createElement("option");
    option.value = theme.theme;
    option.textContent = `${theme.theme} (${theme.dataset_count})`;
    themeFilter.appendChild(option);
  });
  themeFilter.value = current || state.activeTheme;
}

function renderThemeCards() {
  themeCards.innerHTML = "";
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
    button.innerHTML = `
      <span>${theme.theme}</span>
      <strong>${formatNumber(theme.dataset_count)} datasets</strong>
      <small>${theme.description || "Datasets com classificação automática."}</small>
    `;
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
  datasetList.innerHTML = "";

  visible.forEach((dataset) => {
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
    meta.innerHTML = `<span>${dataset.field_count || 0} campos</span><span>${degree} ligações</span>`;

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
    datasetList.appendChild(item);
  });

  if (!visible.length) {
    datasetList.innerHTML = "<div class='empty-state'>Sem resultados para esta pesquisa.</div>";
  }
}

function graphData() {
  const visible = filteredDatasets();
  const links = visibleLinks();

  if (state.selectedDataset) {
    const focusLinks = links
      .filter((link) => link.source === state.selectedDataset || link.target === state.selectedDataset)
      .sort((a, b) => b.score - a.score)
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

  const topLinks = links
    .slice()
    .sort((a, b) => b.score - a.score)
    .slice(0, 260);
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
      tooltip.innerHTML = `<strong>${d.title}</strong><br>${d.degree} ligações no score atual`;
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

function buildTreeForSelected(datasetId) {
  if (!datasetId) {
    linkTree.innerHTML = "<div class='empty-state'>Seleciona um dataset para ver campos de ligação e datasets candidatos.</div>";
    return;
  }

  const links = visibleLinks()
    .filter((item) => item.source === datasetId || item.target === datasetId)
    .sort((a, b) => b.score - a.score)
    .slice(0, 60);

  if (!links.length) {
    linkTree.innerHTML = "<div class='empty-state'>Sem ligações com o score atual. Reduz a força da ligação.</div>";
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

  linkTree.innerHTML = "";
  Array.from(byField.entries())
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, 12)
    .forEach(([field, datasets]) => {
      const group = document.createElement("section");
      group.className = "relation-group";
      const heading = document.createElement("h3");
      heading.innerHTML = `<span class="tag">${field}</span> ${datasets.length} dataset(s)`;
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

  opportunitiesSection.innerHTML = "";
  const title = document.createElement("h2");
  title.textContent = selected ? "Campos com maior potencial" : "Oportunidades globais";
  opportunitiesSection.appendChild(title);

  if (!baseList.length) {
    opportunitiesSection.innerHTML += "<div class='empty-state'>Sem oportunidades para este filtro.</div>";
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
    box.innerHTML = `<strong>${opp.key}</strong><small>${opp.dataset_count} datasets · ${shown}${more}</small>`;
    opportunitiesSection.appendChild(box);
  });
}

function renderSelectedInfo() {
  if (!state.selectedDataset) {
    selectedModel.innerHTML = "<div class='meta'>Escolhe um dataset na lista ou no mapa para analisar cruzamentos possíveis.</div>";
    return;
  }
  const dataset = state.datasetById.get(state.selectedDataset);
  const fields = (dataset?.fields || []).slice(0, 8);
  selectedModel.innerHTML = `
    <div class="selected-title">${displayTitle(dataset)}</div>
    <div class="dataset-meta-row">
      <span class="tag theme-tag">${dataset?.mega_theme || "Outros"}</span>
      <span>${dataset?.field_count || 0} campos</span>
      <span>${state.degreeById.get(state.selectedDataset) || 0} ligações</span>
    </div>
    <div class="dataset-meta-row">${fields.map((field) => `<span class="tag">${field}</span>`).join("")}</div>
  `;
}

async function selectDataset(datasetId) {
  state.selectedDataset = datasetId;
  state.recentData = null;
  renderAll();
  loadRecentRecords(datasetId).catch(showRecordsError);

  try {
    const response = await fetch(`/api/dataset/${encodeURIComponent(datasetId)}`);
    const payload = await response.json();
    if (response.ok && payload?.metas?.default?.description && state.selectedDataset === datasetId) {
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
  recentDataMeta.textContent = "A carregar registos recentes…";
  const response = await fetch(`/api/recent/${encodeURIComponent(datasetId)}?limit=60`);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao consultar registos recentes");
  }
  state.recentData = payload;
  renderRecentTable();
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
      recentDataEmpty.textContent = "Sem dataset selecionado.";
      recentDataMeta.textContent = "Seleciona um dataset para consultar registos recentes da API.";
      return;
    }
  }

  const thead = recentDataTable.querySelector("thead");
  const tbody = recentDataTable.querySelector("tbody");
  thead.innerHTML = "";
  tbody.innerHTML = "";

  if (!selected) {
    recentDataTable.hidden = true;
    recentDataEmpty.hidden = false;
    recentDataEmpty.textContent = "Sem dataset selecionado.";
    recentDataMeta.textContent = "Seleciona um dataset para consultar registos recentes da API.";
    return;
  }

  if (!data || !columns.length) {
    recentDataTable.hidden = true;
    recentDataEmpty.hidden = false;
    recentDataEmpty.textContent = "Sem registos recentes disponíveis para este dataset.";
    recentDataMeta.textContent = "Sem dados recentes para este dataset com o filtro atual.";
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
}

function showRecordsError(error) {
  recentDataTable.hidden = true;
  recentDataEmpty.hidden = false;
  recentDataEmpty.textContent = error.message;
  recentDataMeta.textContent = "Falha ao consultar registos recentes.";
}

function stripHtml(value) {
  const temp = document.createElement("div");
  temp.innerHTML = value;
  return temp.textContent || temp.innerText || "";
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
  state.activeTheme = theme || "";
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
    loadAnalysis().catch(showError);
  });

  datasetFilter.addEventListener("input", () => {
    state.filterText = datasetFilter.value;
    ensureSelectionInFilter();
    renderAll();
  });

  themeFilter.addEventListener("change", () => {
    setActiveTheme(themeFilter.value);
  });

  document.getElementById("refreshButton").addEventListener("click", () => {
    loadAnalysis().catch(showError);
  });

  document.getElementById("clearSelectionButton").addEventListener("click", () => {
    state.selectedDataset = null;
    state.recentData = null;
    renderAll();
  });

  reloadRecordsButton.addEventListener("click", () => {
    if (state.selectedDataset) loadRecentRecords(state.selectedDataset).catch(showRecordsError);
  });

  window.addEventListener("resize", () => {
    if (state.datasets.length) renderAll();
  });
}

function showError(error) {
  syncState.textContent = "Erro";
  datasetSummary.textContent = error.message;
}

setupEvents();
loadAnalysis().catch(showError);
