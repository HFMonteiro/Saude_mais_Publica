const refreshButton = document.getElementById("processingRefresh");
const cacheState = document.getElementById("processingCacheState");
const datasetCount = document.getElementById("processingDatasets");
const linkCount = document.getElementById("processingLinks");
const cacheEntries = document.getElementById("processingCacheEntries");
const cacheList = document.getElementById("processingCacheList");
const limitsList = document.getElementById("processingLimits");
const updated = document.getElementById("processingUpdated");

function clearElement(element) {
  element.replaceChildren();
}

function formatNumber(value) {
  return new Intl.NumberFormat("pt-PT").format(value || 0);
}

function addListRow(container, label, value) {
  const row = document.createElement("div");
  row.className = "processing-row";
  const left = document.createElement("span");
  left.textContent = label;
  const right = document.createElement("strong");
  right.textContent = value;
  row.append(left, right);
  container.appendChild(row);
}

async function loadProcessingState() {
  updated.textContent = "A carregar...";
  const response = await fetch("/api/processing");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Erro ao consultar processamento");
  }

  cacheState.textContent = payload.analysis?.cached ? "Quente" : "Fria";
  datasetCount.textContent = formatNumber(payload.analysis?.datasets);
  linkCount.textContent = formatNumber(payload.analysis?.links);
  cacheEntries.textContent = `${formatNumber(payload.cache?.entry_count)}/${formatNumber(payload.cache?.max_entries)}`;

  clearElement(limitsList);
  addListRow(limitsList, "Dataset limit da análise", formatNumber(payload.limits?.analysis_dataset_limit));
  addListRow(limitsList, "Limite de registos recentes", formatNumber(payload.limits?.max_recent_limit));
  addListRow(limitsList, "Score máximo", formatNumber(payload.limits?.max_min_score));
  addListRow(limitsList, "TTL de cache", `${formatNumber(payload.cache?.ttl_seconds)}s`);

  clearElement(cacheList);
  const entries = payload.cache?.entries || [];
  if (!entries.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "Sem entradas de cache ativas.";
    cacheList.appendChild(empty);
  } else {
    entries
      .slice()
      .sort((a, b) => a.age_seconds - b.age_seconds)
      .slice(0, 40)
      .forEach((entry) => {
        addListRow(cacheList, entry.key, `${formatNumber(entry.age_seconds)}s`);
      });
  }

  updated.textContent = `Atualizado · ${new Date().toLocaleTimeString("pt-PT", {hour: "2-digit", minute: "2-digit"})}`;
}

refreshButton.addEventListener("click", () => {
  loadProcessingState().catch((error) => {
    updated.textContent = error.message;
  });
});

loadProcessingState().catch((error) => {
  updated.textContent = error.message;
});
