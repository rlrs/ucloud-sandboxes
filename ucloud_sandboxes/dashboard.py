from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardAsset:
    body: bytes
    content_type: str


def dashboard_asset(path: str) -> DashboardAsset | None:
    if path in {"/", "/dashboard", "/dashboard/"}:
        return DashboardAsset(DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
    if path == "/dashboard/dashboard.css":
        return DashboardAsset(DASHBOARD_CSS.encode("utf-8"), "text/css; charset=utf-8")
    if path == "/dashboard/dashboard.js":
        return DashboardAsset(
            DASHBOARD_JS.encode("utf-8"),
            "application/javascript; charset=utf-8",
        )
    return None


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>UCloud Sandboxes Dashboard</title>
  <link rel="stylesheet" href="/dashboard/dashboard.css">
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">Control plane</p>
        <h1>UCloud Sandboxes</h1>
      </div>
      <div class="status-strip">
        <span id="connectionStatus" class="status-pill status-warn">Waiting</span>
        <span id="lastUpdated" class="muted">Not refreshed yet</span>
      </div>
    </header>

    <section class="toolbar" aria-label="Dashboard controls">
      <label class="token-field">
        <span>Bearer token</span>
        <input id="tokenInput" type="password" autocomplete="off" spellcheck="false" placeholder="Required for /v1/metrics">
      </label>
      <button id="saveTokenButton" type="button">Save</button>
      <button id="clearTokenButton" type="button">Clear</button>
      <label class="select-field">
        <span>Refresh</span>
        <select id="refreshSelect">
          <option value="2000">2s</option>
          <option value="5000" selected>5s</option>
          <option value="10000">10s</option>
          <option value="30000">30s</option>
        </select>
      </label>
      <button id="pauseButton" type="button">Pause</button>
    </section>

    <section class="metric-grid" aria-label="Key metrics">
      <article class="metric-card">
        <span class="metric-label">Fresh nodes</span>
        <strong id="freshNodes">-</strong>
        <span id="nodeBreakdown" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Sandboxes</span>
        <strong id="activeSandboxes">-</strong>
        <span id="sandboxBreakdown" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Reserved CPU</span>
        <strong id="reservedCpu">-</strong>
        <span id="reservedCpuDetail" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Actual CPU</span>
        <strong id="actualCpu">-</strong>
        <span id="actualCpuDetail" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Memory</span>
        <strong id="memoryUsage">-</strong>
        <span id="memoryDetail" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Disk reserved</span>
        <strong id="diskUsage">-</strong>
        <span id="diskDetail" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Scale-up wait</span>
        <strong id="scaleWait">-</strong>
        <span id="scaleWaitDetail" class="metric-detail">-</span>
      </article>
      <article class="metric-card">
        <span class="metric-label">Image builds</span>
        <strong id="imageBuilds">-</strong>
        <span id="imageBuildDetail" class="metric-detail">-</span>
      </article>
    </section>

    <section class="chart-grid" aria-label="Live graphs">
      <article class="chart-panel">
        <div class="panel-header">
          <h2>Sandbox Demand</h2>
          <span>active, pending, and prepared</span>
        </div>
        <canvas id="sandboxChart" width="720" height="260"></canvas>
        <div class="legend">
          <span><i class="swatch green"></i>Active</span>
          <span><i class="swatch amber"></i>Pending</span>
          <span><i class="swatch blue"></i>Prepared</span>
        </div>
      </article>
      <article class="chart-panel">
        <div class="panel-header">
          <h2>CPU Pressure</h2>
          <span>actual and reserved usage</span>
        </div>
        <canvas id="cpuChart" width="720" height="260"></canvas>
        <div class="legend">
          <span><i class="swatch blue"></i>Actual %</span>
          <span><i class="swatch red"></i>Reserved %</span>
        </div>
      </article>
      <article class="chart-panel">
        <div class="panel-header">
          <h2>Memory Pressure</h2>
          <span>actual and reserved usage</span>
        </div>
        <canvas id="memoryChart" width="720" height="260"></canvas>
        <div class="legend">
          <span><i class="swatch violet"></i>Actual %</span>
          <span><i class="swatch gray"></i>Reserved %</span>
        </div>
      </article>
      <article class="chart-panel">
        <div class="panel-header">
          <h2>Scale-up Latency</h2>
          <span>recent scheduled sandboxes</span>
        </div>
        <canvas id="scaleChart" width="720" height="260"></canvas>
        <div class="legend">
          <span><i class="swatch amber"></i>Wait seconds</span>
        </div>
      </article>
    </section>

    <section class="table-panel" aria-label="Node metrics">
      <div class="panel-header">
        <h2>Nodes</h2>
        <span id="nodeTableSummary">No nodes</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Node</th>
              <th>Kind</th>
              <th>Sandboxes</th>
              <th>Reserved</th>
              <th>Actual</th>
              <th>Free</th>
              <th>Age</th>
              <th>Version</th>
            </tr>
          </thead>
          <tbody id="nodeRows">
            <tr><td colspan="8" class="empty-cell">No metrics loaded</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="event-panel" aria-label="Recent autoscaler events">
      <div class="panel-header">
        <h2>Recent Events</h2>
        <span id="eventSummary">No events loaded</span>
      </div>
      <ol id="eventList" class="event-list"></ol>
    </section>
  </main>
  <script src="/dashboard/dashboard.js" defer></script>
</body>
</html>
"""


DASHBOARD_CSS = """
:root {
  color-scheme: light;
  --background: #f6f7f3;
  --surface: #ffffff;
  --surface-soft: #eef1ea;
  --line: #d9ded3;
  --line-strong: #b9c1b2;
  --text: #1d241f;
  --muted: #647062;
  --green: #277d5b;
  --amber: #b96b12;
  --blue: #2d6fb7;
  --red: #b84a42;
  --violet: #7056a3;
  --gray: #737a80;
  --shadow: 0 1px 2px rgba(31, 42, 35, 0.08);
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  min-width: 320px;
  background: var(--background);
  color: var(--text);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
  line-height: 1.45;
}

button,
input,
select {
  font: inherit;
}

button {
  height: 38px;
  border: 1px solid var(--line-strong);
  border-radius: 6px;
  background: #f9faf7;
  color: var(--text);
  padding: 0 14px;
  cursor: pointer;
}

button:hover {
  background: #eef1ea;
}

button:focus-visible,
input:focus-visible,
select:focus-visible {
  outline: 2px solid var(--blue);
  outline-offset: 2px;
}

.shell {
  width: min(1480px, calc(100vw - 32px));
  margin: 0 auto;
  padding: 24px 0 40px;
}

.topbar {
  display: flex;
  justify-content: space-between;
  gap: 18px;
  align-items: flex-end;
  margin-bottom: 18px;
}

.eyebrow {
  margin: 0 0 4px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0;
  text-transform: uppercase;
}

h1,
h2 {
  margin: 0;
  letter-spacing: 0;
}

h1 {
  font-size: 28px;
  line-height: 1.1;
}

h2 {
  font-size: 15px;
  line-height: 1.2;
}

.status-strip {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
  color: var(--muted);
}

.status-pill {
  display: inline-flex;
  min-width: 86px;
  height: 28px;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  border: 1px solid var(--line-strong);
  font-weight: 700;
  color: var(--text);
}

.status-ok {
  background: #dff1e8;
  border-color: #9ed2ba;
}

.status-warn {
  background: #f8ead2;
  border-color: #e0b776;
}

.status-bad {
  background: #f5dfdc;
  border-color: #ddaaa4;
}

.muted {
  color: var(--muted);
}

.toolbar {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) auto auto minmax(130px, 170px) auto;
  gap: 10px;
  align-items: end;
  padding: 12px;
  margin-bottom: 16px;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}

.token-field,
.select-field {
  display: grid;
  gap: 5px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.token-field input,
.select-field select {
  width: 100%;
  height: 38px;
  border: 1px solid var(--line-strong);
  border-radius: 6px;
  background: #fbfcf9;
  color: var(--text);
  padding: 0 10px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}

.metric-card,
.chart-panel,
.table-panel,
.event-panel {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
}

.metric-card {
  display: grid;
  gap: 6px;
  min-height: 112px;
  padding: 14px;
}

.metric-label,
.metric-detail {
  color: var(--muted);
}

.metric-label {
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
}

.metric-card strong {
  font-size: 28px;
  line-height: 1.1;
  letter-spacing: 0;
  font-variant-numeric: tabular-nums;
}

.metric-detail {
  min-height: 20px;
  overflow-wrap: anywhere;
}

.chart-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}

.chart-panel {
  min-width: 0;
  padding: 14px;
}

.panel-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.panel-header span {
  color: var(--muted);
  font-size: 12px;
  text-align: right;
}

canvas {
  display: block;
  width: 100%;
  height: 260px;
  background: #fbfcf9;
  border: 1px solid var(--line);
  border-radius: 6px;
}

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 10px;
  color: var(--muted);
  font-size: 12px;
}

.legend span {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.swatch {
  width: 10px;
  height: 10px;
  border-radius: 3px;
  display: inline-block;
}

.green { background: var(--green); }
.amber { background: var(--amber); }
.blue { background: var(--blue); }
.red { background: var(--red); }
.violet { background: var(--violet); }
.gray { background: var(--gray); }

.table-panel,
.event-panel {
  padding: 14px;
  margin-bottom: 16px;
}

.table-wrap {
  overflow-x: auto;
}

table {
  width: 100%;
  min-width: 980px;
  border-collapse: collapse;
  font-variant-numeric: tabular-nums;
}

th,
td {
  padding: 10px 8px;
  border-bottom: 1px solid var(--line);
  text-align: left;
  vertical-align: top;
  white-space: nowrap;
}

th {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
}

td:first-child {
  white-space: normal;
  min-width: 210px;
}

.node-title {
  font-weight: 700;
}

.node-subtitle {
  display: block;
  color: var(--muted);
  font-size: 12px;
  overflow-wrap: anywhere;
}

.empty-cell {
  color: var(--muted);
  text-align: center;
}

.event-list {
  display: grid;
  gap: 8px;
  list-style: none;
  margin: 0;
  padding: 0;
}

.event-list li {
  display: grid;
  grid-template-columns: minmax(150px, 210px) minmax(130px, 190px) minmax(0, 1fr);
  gap: 12px;
  align-items: start;
  padding: 10px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcf9;
}

.event-kind {
  font-weight: 700;
}

.event-time,
.event-data {
  color: var(--muted);
}

.event-data {
  overflow-wrap: anywhere;
}

@media (max-width: 1100px) {
  .metric-grid,
  .chart-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .shell {
    width: min(100% - 20px, 1480px);
    padding-top: 14px;
  }

  .topbar,
  .panel-header {
    align-items: flex-start;
    flex-direction: column;
  }

  .status-strip {
    justify-content: flex-start;
  }

  .toolbar {
    grid-template-columns: 1fr 1fr;
  }

  .token-field,
  .select-field {
    grid-column: 1 / -1;
  }

  .metric-grid,
  .chart-grid {
    grid-template-columns: 1fr;
  }

  .event-list li {
    grid-template-columns: 1fr;
  }
}
"""


DASHBOARD_JS = """
const MAX_HISTORY = 180;

const state = {
  timer: null,
  paused: false,
  history: [],
  lastScaleEventKeys: new Set(),
};

const palette = {
  green: "#277d5b",
  amber: "#b96b12",
  blue: "#2d6fb7",
  red: "#b84a42",
  violet: "#7056a3",
  gray: "#737a80",
  grid: "#d9ded3",
  text: "#1d241f",
  muted: "#647062",
};

const els = {};

document.addEventListener("DOMContentLoaded", () => {
  for (const id of [
    "connectionStatus",
    "lastUpdated",
    "tokenInput",
    "saveTokenButton",
    "clearTokenButton",
    "refreshSelect",
    "pauseButton",
    "freshNodes",
    "nodeBreakdown",
    "activeSandboxes",
    "sandboxBreakdown",
    "reservedCpu",
    "reservedCpuDetail",
    "actualCpu",
    "actualCpuDetail",
    "memoryUsage",
    "memoryDetail",
    "diskUsage",
    "diskDetail",
    "scaleWait",
    "scaleWaitDetail",
    "imageBuilds",
    "imageBuildDetail",
    "nodeTableSummary",
    "nodeRows",
    "eventSummary",
    "eventList",
  ]) {
    els[id] = document.getElementById(id);
  }

  els.tokenInput.value = sessionStorage.getItem("ucloud.dashboard.token") || "";
  els.saveTokenButton.addEventListener("click", saveToken);
  els.clearTokenButton.addEventListener("click", clearToken);
  els.refreshSelect.addEventListener("change", scheduleNextRefresh);
  els.pauseButton.addEventListener("click", togglePause);
  window.addEventListener("resize", redrawCharts);
  refreshNow();
  scheduleNextRefresh();
});

function saveToken() {
  const token = els.tokenInput.value.trim();
  if (token) {
    sessionStorage.setItem("ucloud.dashboard.token", token);
    setStatus("Saved", "ok");
    refreshNow();
    return;
  }
  clearToken();
}

function clearToken() {
  sessionStorage.removeItem("ucloud.dashboard.token");
  els.tokenInput.value = "";
  setStatus("Auth required", "warn");
}

function togglePause() {
  state.paused = !state.paused;
  els.pauseButton.textContent = state.paused ? "Resume" : "Pause";
  if (!state.paused) {
    refreshNow();
  }
}

function scheduleNextRefresh() {
  if (state.timer !== null) {
    window.clearInterval(state.timer);
  }
  const intervalMs = Number(els.refreshSelect.value) || 5000;
  state.timer = window.setInterval(() => {
    if (!state.paused) {
      refreshNow();
    }
  }, intervalMs);
}

async function refreshNow() {
  const token = sessionStorage.getItem("ucloud.dashboard.token") || els.tokenInput.value.trim();
  const headers = token ? { Authorization: `Bearer ${token}` } : {};
  try {
    const response = await fetch("/v1/metrics", {
      headers,
      cache: "no-store",
    });
    if (response.status === 401) {
      setStatus("Auth required", "warn");
      els.lastUpdated.textContent = "Enter the gateway bearer token";
      return;
    }
    if (!response.ok) {
      setStatus(`HTTP ${response.status}`, "bad");
      return;
    }
    const snapshot = await response.json();
    setStatus("Live", "ok");
    renderSnapshot(snapshot);
  } catch (error) {
    setStatus("Offline", "bad");
    els.lastUpdated.textContent = String(error && error.message ? error.message : error);
  }
}

function setStatus(text, mode) {
  els.connectionStatus.textContent = text;
  els.connectionStatus.className = `status-pill status-${mode || "warn"}`;
}

function renderSnapshot(snapshot) {
  const point = pointFromSnapshot(snapshot);
  state.history.push(point);
  if (state.history.length > MAX_HISTORY) {
    state.history.splice(0, state.history.length - MAX_HISTORY);
  }
  els.lastUpdated.textContent = `Updated ${formatTime(snapshot.generated_at)}`;
  renderMetrics(snapshot, point);
  renderNodes(snapshot);
  renderEvents(snapshot);
  redrawCharts();
}

function pointFromSnapshot(snapshot) {
  const resources = snapshot.resources || {};
  const sandboxResources = resources.sandbox || {};
  const sandboxLoad = sandboxResources.load || {};
  const actual = sandboxResources.actual_usage || {};
  const sandboxes = snapshot.sandboxes || {};
  const capacity = snapshot.capacity || {};
  const images = snapshot.images || {};
  const scale = snapshot.scale_up || {};
  return {
    at: Date.parse(snapshot.generated_at) || Date.now(),
    active: asNumber(sandboxes.active_routes),
    pending: asNumber(sandboxes.pending),
    prepared: asNumber(capacity.prepared_sandboxes),
    pendingBuilds: asNumber(images.pending_builds),
    actualCpuPct: nullableNumber(actual.cpu_percent_avg),
    cpuReservedPct: ratioToPercent(sandboxLoad.vcpu),
    actualMemPct: nullableNumber(actual.memory_percent),
    memReservedPct: ratioToPercent(sandboxLoad.memory),
    diskReservedPct: ratioToPercent(sandboxLoad.disk),
    scaleLastSeconds: nullableNumber(scale.last_ms) === null ? null : Number(scale.last_ms) / 1000,
  };
}

function renderMetrics(snapshot, point) {
  const nodes = snapshot.nodes || {};
  const sandboxes = snapshot.sandboxes || {};
  const capacity = snapshot.capacity || {};
  const exec = snapshot.exec || {};
  const images = snapshot.images || {};
  const resources = snapshot.resources || {};
  const sandboxResources = resources.sandbox || {};
  const total = sandboxResources.effective || {};
  const used = sandboxResources.used || {};
  const free = sandboxResources.free || {};
  const actual = sandboxResources.actual_usage || {};
  const load = sandboxResources.load || {};
  const scale = snapshot.scale_up || {};
  const pendingResources = sandboxes.pending_resources || {};
  const preparedResources = capacity.prepared_resources || {};

  els.freshNodes.textContent = `${asNumber(nodes.fresh)} / ${asNumber(nodes.total)}`;
  els.nodeBreakdown.textContent = `${asNumber(nodes.sandbox)} sandbox, ${asNumber(nodes.builder)} builder`;

  els.activeSandboxes.textContent = String(asNumber(sandboxes.active_routes));
  els.sandboxBreakdown.textContent = `${asNumber(sandboxes.pending)} pending, ${asNumber(capacity.prepared_sandboxes)} prepared, ${asNumber(exec.sessions)} exec sessions`;

  els.reservedCpu.textContent = `${formatNumber(used.vcpu)} / ${formatNumber(total.vcpu)}`;
  els.reservedCpuDetail.textContent = `${formatPercent(load.vcpu)} reserved, ${formatNumber(free.vcpu)} vCPU free`;

  els.actualCpu.textContent = formatNullable(actual.cpu_vcpu, " vCPU");
  els.actualCpuDetail.textContent = `${formatPercentValue(actual.cpu_percent_avg)} avg CPU, ${asNumber(actual.samples)} node samples`;

  els.memoryUsage.textContent = formatPercentValue(actual.memory_percent);
  els.memoryDetail.textContent = `${formatMb(actual.memory_used_mb)} used, ${formatPercent(load.memory)} reserved`;

  els.diskUsage.textContent = formatPercent(load.disk);
  els.diskDetail.textContent = `${formatGb(used.disk_mb)} / ${formatGb(total.disk_mb)} reserved`;

  els.scaleWait.textContent = nullableNumber(scale.last_ms) === null ? "-" : formatDurationMs(scale.last_ms);
  els.scaleWaitDetail.textContent = `${asNumber(scale.samples)} samples, p95 ${formatDurationMs(scale.p95_ms)}`;

  els.imageBuilds.textContent = String(asNumber(images.pending_builds));
  els.imageBuildDetail.textContent = `oldest ${formatAge(images.oldest_pending_build_seconds)}`;

  if (asNumber(sandboxes.pending) > 0) {
    els.sandboxBreakdown.textContent += `, pending ${formatNumber(pendingResources.vcpu)} vCPU ${formatMb(pendingResources.memory_mb)}`;
  }
  if (asNumber(capacity.prepared_sandboxes) > 0) {
    els.sandboxBreakdown.textContent += `, prepared ${formatNumber(preparedResources.vcpu)} vCPU ${formatMb(preparedResources.memory_mb)}`;
  }

  if (point.pendingBuilds > 0 || point.pending > 0 || point.prepared > 0) {
    setStatus("Demand pending", "warn");
  }
}

function renderNodes(snapshot) {
  const nodes = ((snapshot.nodes || {}).items || []).slice().sort((a, b) => {
    const freshDelta = Number(Boolean(b.fresh)) - Number(Boolean(a.fresh));
    if (freshDelta !== 0) return freshDelta;
    return String(a.node_id || "").localeCompare(String(b.node_id || ""));
  });
  els.nodeTableSummary.textContent = `${nodes.length} known, ${nodes.filter((node) => node.fresh).length} fresh`;
  if (nodes.length === 0) {
    els.nodeRows.innerHTML = '<tr><td colspan="8" class="empty-cell">No nodes have reported yet</td></tr>';
    return;
  }
  els.nodeRows.replaceChildren(...nodes.map(nodeRow));
}

function nodeRow(node) {
  const tr = document.createElement("tr");
  const caps = Array.isArray(node.capabilities) ? node.capabilities : [];
  const actual = node.actual_usage || {};
  const load = node.load || {};
  const free = node.free_resources || {};
  const effective = node.effective_resources || {};
  appendCell(tr, nodeIdentity(node));
  appendCell(tr, caps.join(", ") || "-");
  appendCell(tr, String(asNumber(node.active_sandboxes)));
  appendCell(tr, `CPU ${formatPercent(load.vcpu)} / MEM ${formatPercent(load.memory)} / DISK ${formatPercent(load.disk)}`);
  appendCell(tr, `${formatNullable(actual.cpu_vcpu, " vCPU")} / ${formatPercentValue(actual.memory_percent)}`);
  appendCell(tr, `${formatNumber(free.vcpu)} vCPU, ${formatGb(free.memory_mb)} RAM, ${formatGb(free.disk_mb)} disk`);
  appendCell(tr, `${formatAge(node.age_seconds)}${node.fresh ? "" : " stale"}`);
  appendCell(tr, compactVersion(node, effective));
  return tr;
}

function nodeIdentity(node) {
  const wrap = document.createElement("span");
  const title = document.createElement("span");
  title.className = "node-title";
  title.textContent = node.node_id || node.job_id || "unknown";
  const subtitle = document.createElement("span");
  subtitle.className = "node-subtitle";
  subtitle.textContent = node.job_id ? `job ${node.job_id}` : node.node_url || "";
  wrap.append(title, subtitle);
  return wrap;
}

function compactVersion(node, effective) {
  const bits = [];
  if (node.agent_version) bits.push(`agent ${node.agent_version}`);
  if (node.deployment_id) bits.push(node.deployment_id);
  bits.push(`${formatNumber(effective.vcpu)} vCPU`);
  return bits.join(" / ");
}

function appendCell(row, value) {
  const td = document.createElement("td");
  if (value instanceof Node) {
    td.append(value);
  } else {
    td.textContent = value;
  }
  row.append(td);
}

function renderEvents(snapshot) {
  const events = ((snapshot.events || {}).recent || []).slice(-12).reverse();
  els.eventSummary.textContent = events.length ? `${events.length} recent events` : "No recent events";
  els.eventList.replaceChildren(...events.map(eventRow));
}

function eventRow(event) {
  const li = document.createElement("li");
  const time = document.createElement("span");
  time.className = "event-time";
  time.textContent = formatTime(event.timestamp);
  const kind = document.createElement("span");
  kind.className = "event-kind";
  kind.textContent = event.kind || "event";
  const data = document.createElement("span");
  data.className = "event-data";
  data.textContent = summarizeEvent(event);
  li.append(time, kind, data);
  return li;
}

function summarizeEvent(event) {
  const data = event.data || {};
  if (event.kind === "autoscaler_cycle") {
    const actions = Array.isArray(data.actions) ? data.actions.join(", ") : "";
    const builderActions = Array.isArray(data.builder_actions) ? data.builder_actions.join(", ") : "";
    const created = Array.isArray(data.created_job_ids) ? data.created_job_ids.length : 0;
    const stopped = Array.isArray(data.stop_job_ids) ? data.stop_job_ids.length : 0;
    return `ready ${asNumber(data.ready_nodes)}, provisioning ${asNumber(data.provisioning_nodes)}, created ${created}, stopped ${stopped}, actions ${actions || builderActions || "none"}`;
  }
  if (event.kind === "sandbox_scheduled") {
    return `${data.sandbox_id || "sandbox"} on ${data.node_id || data.job_id || "node"}, wait ${formatDurationMs(data.scale_up_wait_ms)}`;
  }
  if (event.kind === "sandbox_pending_deleted") {
    return `${data.sandbox_id || "sandbox"} deleted while pending after ${formatDurationMs(data.pending_age_ms)}`;
  }
  if (event.kind === "node_heartbeat") {
    return `${data.node_id || data.job_id || "node"} active ${asNumber(data.active_sandboxes)}, CPU ${formatPercent((data.load || {}).vcpu)}`;
  }
  return JSON.stringify(data).slice(0, 220);
}

function redrawCharts() {
  if (state.history.length === 0) {
    clearChart("sandboxChart", "Waiting for metrics");
    clearChart("cpuChart", "Waiting for metrics");
    clearChart("memoryChart", "Waiting for metrics");
    clearChart("scaleChart", "Waiting for metrics");
    return;
  }
  drawLineChart("sandboxChart", [
    { label: "active", color: palette.green, values: state.history.map((p) => p.active) },
    { label: "pending", color: palette.amber, values: state.history.map((p) => p.pending) },
    { label: "prepared", color: palette.blue, values: state.history.map((p) => p.prepared) },
  ], { min: 0 });
  drawLineChart("cpuChart", [
    { label: "actual %", color: palette.blue, values: state.history.map((p) => p.actualCpuPct) },
    { label: "reserved %", color: palette.red, values: state.history.map((p) => p.cpuReservedPct) },
  ], { min: 0, max: 100 });
  drawLineChart("memoryChart", [
    { label: "actual %", color: palette.violet, values: state.history.map((p) => p.actualMemPct) },
    { label: "reserved %", color: palette.gray, values: state.history.map((p) => p.memReservedPct) },
  ], { min: 0, max: 100 });
  drawLineChart("scaleChart", [
    { label: "scale-up seconds", color: palette.amber, values: state.history.map((p) => p.scaleLastSeconds) },
  ], { min: 0 });
}

function clearChart(id, label) {
  const canvas = document.getElementById(id);
  const prepared = prepareCanvas(canvas);
  const ctx = prepared.ctx;
  ctx.fillStyle = "#fbfcf9";
  ctx.fillRect(0, 0, prepared.width, prepared.height);
  ctx.fillStyle = palette.muted;
  ctx.font = "14px system-ui, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(label, prepared.width / 2, prepared.height / 2);
}

function drawLineChart(id, series, options) {
  const canvas = document.getElementById(id);
  const prepared = prepareCanvas(canvas);
  const ctx = prepared.ctx;
  const width = prepared.width;
  const height = prepared.height;
  const pad = { left: 48, right: 18, top: 16, bottom: 32 };
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#fbfcf9";
  ctx.fillRect(0, 0, width, height);

  const allValues = series.flatMap((line) => line.values).filter((value) => value !== null && Number.isFinite(value));
  if (allValues.length === 0) {
    drawEmptyPlot(ctx, width, height, "No numeric samples");
    return;
  }
  const min = options.min ?? Math.min(...allValues);
  const rawMax = options.max ?? Math.max(...allValues);
  const max = rawMax <= min ? min + 1 : rawMax * 1.08;
  drawGrid(ctx, width, height, pad, min, max);

  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const count = Math.max(1, state.history.length - 1);
  for (const line of series) {
    ctx.beginPath();
    ctx.strokeStyle = line.color;
    ctx.lineWidth = 2.5;
    let started = false;
    line.values.forEach((value, index) => {
      if (value === null || !Number.isFinite(value)) {
        return;
      }
      const x = pad.left + (plotWidth * index) / count;
      const y = pad.top + plotHeight - ((value - min) / (max - min)) * plotHeight;
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
  }
}

function prepareCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = Math.max(320, Math.round(rect.width));
  const cssHeight = Math.max(180, Math.round(rect.height));
  const nextWidth = Math.round(cssWidth * dpr);
  const nextHeight = Math.round(cssHeight * dpr);
  canvas.width = nextWidth;
  canvas.height = nextHeight;
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, width: cssWidth, height: cssHeight };
}

function drawGrid(ctx, width, height, pad, min, max) {
  const left = pad.left;
  const right = width - pad.right;
  const top = pad.top;
  const bottom = height - pad.bottom;
  ctx.strokeStyle = palette.grid;
  ctx.lineWidth = 1;
  ctx.fillStyle = palette.muted;
  ctx.font = "12px system-ui, sans-serif";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= 4; i += 1) {
    const y = top + ((bottom - top) * i) / 4;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(right, y);
    ctx.stroke();
    const value = max - ((max - min) * i) / 4;
    ctx.fillText(shortNumber(value), left - 8, y);
  }
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
  const first = state.history[0];
  const last = state.history[state.history.length - 1];
  if (first && last) {
    ctx.fillText(shortTime(first.at), left, height - 8);
    ctx.textAlign = "right";
    ctx.fillText(shortTime(last.at), right, height - 8);
  }
}

function drawEmptyPlot(ctx, width, height, label) {
  ctx.fillStyle = palette.muted;
  ctx.font = "14px system-ui, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(label, width / 2, height / 2);
}

function asNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function nullableNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function ratioToPercent(value) {
  const number = nullableNumber(value);
  return number === null ? null : number * 100;
}

function formatNumber(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(number);
}

function shortNumber(value) {
  if (Math.abs(value) >= 100) return String(Math.round(value));
  if (Math.abs(value) >= 10) return value.toFixed(1);
  return value.toFixed(2).replace(/\\.00$/, "");
}

function formatNullable(value, suffix) {
  const number = nullableNumber(value);
  return number === null ? "-" : `${formatNumber(number)}${suffix}`;
}

function formatPercent(ratio) {
  const number = nullableNumber(ratio);
  return number === null ? "-" : `${formatNumber(number * 100)}%`;
}

function formatPercentValue(value) {
  const number = nullableNumber(value);
  return number === null ? "-" : `${formatNumber(number)}%`;
}

function formatMb(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  if (number >= 1024) return `${formatNumber(number / 1024)} GiB`;
  return `${formatNumber(number)} MiB`;
}

function formatGb(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return `${formatNumber(number / 1024)} GiB`;
}

function formatDurationMs(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return formatAge(number / 1000);
}

function formatAge(value) {
  const seconds = nullableNumber(value);
  if (seconds === null) return "-";
  if (seconds < 1) return "<1s";
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  return `${Math.round(seconds / 3600)}h`;
}

function formatTime(value) {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) return "-";
  return new Date(timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function shortTime(value) {
  return new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}
"""
