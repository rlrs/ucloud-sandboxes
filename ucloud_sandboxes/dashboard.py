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
  <header class="app-bar">
    <div class="brand">
      <span class="menu-mark" aria-hidden="true"><span></span><span></span><span></span></span>
      <strong>CPU Sandbox Service</strong>
    </div>
    <div class="top-controls" aria-label="Dashboard controls">
      <span id="connectionStatus" class="status-pill status-warn">Waiting</span>
      <label class="select-control">
        <span class="clock-mark" aria-hidden="true"></span>
        <span class="visually-hidden">Time range</span>
        <select id="timeRangeSelect">
          <option value="900000">Last 15m</option>
          <option value="3600000" selected>Last 1h</option>
          <option value="21600000">Last 6h</option>
        </select>
      </label>
      <div class="select-control fixed-control" aria-label="Refresh interval" title="Refresh interval">
        <span class="refresh-mark" aria-hidden="true"></span>
        <span class="control-value">5s</span>
      </div>
      <button id="pauseButton" class="icon-button" type="button" title="Pause refresh" aria-label="Pause refresh">
        <span class="pause-mark" aria-hidden="true"></span>
      </button>
      <button id="themeButton" class="icon-button" type="button" title="Toggle dark charts" aria-label="Toggle dark charts">
        <span class="moon-mark" aria-hidden="true"></span>
      </button>
    </div>
  </header>

  <main class="page-shell">
    <section class="page-title">
      <div>
        <h1>CPU Sandbox Service</h1>
        <p>Internal Monitoring Dashboard</p>
        <span class="visually-hidden">UCloud Sandboxes</span>
      </div>
      <div class="title-actions">
        <span id="lastUpdated" class="last-updated">Not refreshed yet</span>
        <button id="authToggleButton" type="button">Bearer token</button>
      </div>
    </section>

    <section id="authPanel" class="auth-panel" aria-label="Metrics authentication">
      <label class="token-field">
        <span>Gateway bearer token</span>
        <input id="tokenInput" type="password" autocomplete="off" spellcheck="false" placeholder="Required for /v1/metrics">
      </label>
      <button id="saveTokenButton" type="button">Save</button>
      <button id="clearTokenButton" type="button">Clear</button>
    </section>

    <nav class="page-tabs" aria-label="Dashboard pages">
      <button class="page-tab is-active" type="button" data-page-target="overview">Overview</button>
      <button class="page-tab" type="button" data-page-target="registry">Registry</button>
    </nav>

    <section class="metric-grid overview-section" aria-label="Key metrics">
      <article class="metric-card accent-blue">
        <div>
          <span class="metric-label">Active Nodes</span>
          <strong id="activeNodesValue">-</strong>
          <span id="activeNodesDetail" class="metric-detail">-</span>
        </div>
        <canvas id="nodesSpark" class="sparkline" width="150" height="56"></canvas>
      </article>
      <article class="metric-card accent-blue">
        <div>
          <span class="metric-label">Running Sandboxes</span>
          <strong id="runningSandboxesValue">-</strong>
          <span id="runningSandboxesDetail" class="metric-detail">-</span>
        </div>
        <canvas id="sandboxesSpark" class="sparkline" width="150" height="56"></canvas>
      </article>
      <article class="metric-card accent-green">
        <div>
          <span class="metric-label">CPU Utilization</span>
          <strong id="cpuUtilizationValue">-</strong>
          <span id="cpuUtilizationDetail" class="metric-detail">-</span>
        </div>
        <canvas id="cpuSpark" class="sparkline" width="150" height="56"></canvas>
      </article>
      <article class="metric-card accent-orange">
        <div>
          <span class="metric-label">Memory Utilization</span>
          <strong id="memoryUtilizationValue">-</strong>
          <span id="memoryUtilizationDetail" class="metric-detail">-</span>
        </div>
        <canvas id="memorySpark" class="sparkline" width="150" height="56"></canvas>
      </article>
      <article class="metric-card accent-purple">
        <div>
          <span class="metric-label">Queue Depth</span>
          <strong id="queueDepthValue">-</strong>
          <span id="queueDepthDetail" class="metric-detail">-</span>
        </div>
        <canvas id="queueSpark" class="sparkline" width="150" height="56"></canvas>
      </article>
      <article class="metric-card accent-red">
        <div>
          <span class="metric-label">Error Rate</span>
          <strong id="errorRateValue">-</strong>
          <span id="errorRateDetail" class="metric-detail">-</span>
        </div>
        <canvas id="errorSpark" class="sparkline" width="150" height="56"></canvas>
      </article>
    </section>

    <section class="chart-grid overview-section" aria-label="Live graphs">
      <article class="chart-panel chart-wide">
        <div class="panel-header">
          <h2>Active Nodes</h2>
          <span class="info-dot" title="Fresh sandbox nodes"></span>
        </div>
        <canvas id="activeNodesChart" class="chart-canvas" width="620" height="210"></canvas>
        <div class="legend"><span><i class="swatch blue"></i>Active Nodes</span></div>
      </article>
      <article class="chart-panel chart-wide">
        <div class="panel-header">
          <h2>Running Sandboxes</h2>
          <span class="info-dot" title="Running sandboxes from fresh node heartbeats"></span>
        </div>
        <canvas id="activeSandboxesChart" class="chart-canvas" width="620" height="210"></canvas>
        <div class="legend"><span><i class="swatch blue"></i>Running Sandboxes</span></div>
      </article>
      <article class="chart-panel chart-wide">
        <div class="panel-header">
          <h2>Queue Depth</h2>
          <span class="info-dot" title="Pending sandboxes, prepared capacity, and image builds"></span>
        </div>
        <canvas id="queueDepthChart" class="chart-canvas" width="620" height="210"></canvas>
        <div class="legend"><span><i class="swatch purple"></i>Queue Depth</span></div>
      </article>
      <article class="chart-panel chart-small">
        <div class="panel-header">
          <h2>CPU Pressure</h2>
          <span class="info-dot" title="Actual average CPU and reserved CPU"></span>
        </div>
        <canvas id="cpuPressureChart" class="chart-canvas small" width="520" height="190"></canvas>
        <div class="legend">
          <span><i class="swatch green"></i>Actual</span>
          <span><i class="swatch blue"></i>Reserved</span>
        </div>
      </article>
      <article class="chart-panel chart-small">
        <div class="panel-header">
          <h2>Memory Pressure</h2>
          <span class="info-dot" title="Actual memory and reserved memory"></span>
        </div>
        <canvas id="memoryPressureChart" class="chart-canvas small" width="520" height="190"></canvas>
        <div class="legend">
          <span><i class="swatch orange"></i>Actual</span>
          <span><i class="swatch blue"></i>Reserved</span>
        </div>
      </article>
      <article class="chart-panel chart-small">
        <div class="panel-header">
          <h2>Scale-up Latency (s)</h2>
          <span class="info-dot" title="Recent sandbox scheduling wait"></span>
        </div>
        <canvas id="scaleLatencyChart" class="chart-canvas small" width="520" height="190"></canvas>
        <div class="legend">
          <span><i class="swatch blue"></i>p50</span>
          <span><i class="swatch blue-dash"></i>p95</span>
        </div>
      </article>
      <article class="chart-panel chart-small">
        <div class="panel-header">
          <h2>Sandbox Start Time (s)</h2>
          <span class="info-dot" title="Latest observed sandbox schedule wait"></span>
        </div>
        <canvas id="sandboxStartChart" class="chart-canvas small" width="520" height="190"></canvas>
        <div class="legend"><span><i class="swatch green"></i>Start Time (p50)</span></div>
      </article>
    </section>

    <section class="ops-grid overview-section" aria-label="Builder and registry operations">
      <article class="ops-panel ops-large">
        <div class="panel-header">
          <h2>Builder Pool</h2>
          <span id="builderSummary">No builder metrics loaded</span>
        </div>
        <div class="stat-strip">
          <div class="stat-box">
            <span>Ready Builders</span>
            <strong id="builderReadyValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Prepared</span>
            <strong id="builderPreparedValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Active Builds</span>
            <strong id="builderActiveBuildsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Builder CPU</span>
            <strong id="builderCpuValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Builder Memory</span>
            <strong id="builderMemoryValue">-</strong>
          </div>
        </div>
        <canvas id="builderBuildsChart" class="chart-canvas compact" width="760" height="160"></canvas>
        <div class="legend">
          <span><i class="swatch orange"></i>Active Builds</span>
          <span><i class="swatch blue-dash"></i>Ready Builders</span>
        </div>
      </article>

      <article class="ops-panel ops-small">
        <div class="panel-header">
          <h2>Registry</h2>
          <span id="registryStatusBadge" class="inline-badge badge-muted">Unknown</span>
        </div>
        <div class="registry-url" id="registryUrl">No registry configured</div>
        <div class="stat-strip compact-strip">
          <div class="stat-box">
            <span>Repositories</span>
            <strong id="registryReposValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Scanned Tags</span>
            <strong id="registryTagsValue">-</strong>
          </div>
        </div>
        <div id="registryDetail" class="registry-detail">Waiting for registry metrics</div>
        <div id="registryRepos" class="repo-list">
          <span class="empty-inline">No repositories loaded</span>
        </div>
      </article>
    </section>

    <section class="event-panel build-panel overview-section" aria-label="Recent image builds">
      <div class="panel-header table-header">
        <h2>Recent Image Builds</h2>
        <span id="buildSummary">No builds loaded</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Image</th>
              <th>Tag</th>
              <th>Location</th>
              <th>Age</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody id="buildRows">
            <tr><td colspan="6" class="empty-cell">No builds loaded</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="event-panel overview-section" aria-label="Recent request traces">
      <div class="panel-header table-header">
        <h2>Recent Traces</h2>
        <span id="traceSummary">No traces loaded</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Status</th>
              <th>Trace</th>
              <th>Duration</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody id="traceRows">
            <tr><td colspan="5" class="empty-cell">No traces loaded</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="event-panel overview-section" aria-label="Recent autoscaler events">
      <div class="panel-header table-header">
        <h2>Recent Events</h2>
        <span id="eventSummary">No events loaded</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Severity</th>
              <th>Event</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody id="eventRows">
            <tr><td colspan="4" class="empty-cell">No metrics loaded</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section id="registryPage" class="registry-page" aria-label="Registry" hidden>
      <section class="registry-hero">
        <div class="registry-hero-main">
          <div class="panel-header">
            <h2>Registry</h2>
            <span id="registryPageStatusBadge" class="inline-badge badge-muted">Unknown</span>
          </div>
          <div id="registryPageUrl" class="registry-url registry-url-large">No registry configured</div>
          <p id="registryPageHealthDetail" class="registry-copy">Waiting for registry metrics</p>
        </div>
        <div class="registry-stat-grid">
          <div class="stat-box">
            <span>Repositories</span>
            <strong id="registryPageReposValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Scanned Tags</span>
            <strong id="registryPageTagsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Visible Tags</span>
            <strong id="registryPageVisibleTagsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Coverage</span>
            <strong id="registryPageCoverageValue">-</strong>
          </div>
        </div>
      </section>

      <section class="registry-toolbar" aria-label="Registry filters">
        <label class="registry-search">
          <span>Search registry</span>
          <input id="registrySearchInput" type="search" autocomplete="off" spellcheck="false" placeholder="Repository, tag, image id">
        </label>
        <label class="registry-select">
          <span>Filter</span>
          <select id="registryFilterSelect">
            <option value="all">All repositories</option>
            <option value="with-builds">With tracked builds</option>
            <option value="truncated">Tag list truncated</option>
            <option value="empty">No visible tags</option>
          </select>
        </label>
        <div id="registryPageSummary" class="registry-copy">No repositories loaded</div>
      </section>

      <section class="registry-full-grid" aria-label="Registry details">
        <article class="event-panel registry-panel">
          <div class="panel-header table-header">
            <h2>Repositories</h2>
            <span id="registryRepoSummary">No repositories loaded</span>
          </div>
          <div class="table-wrap">
            <table class="registry-table">
              <thead>
                <tr>
                  <th>Repository</th>
                  <th>Tags</th>
                  <th>Latest</th>
                  <th>Builds</th>
                  <th>Visible Tags</th>
                </tr>
              </thead>
              <tbody id="registryRepoRows">
                <tr><td colspan="5" class="empty-cell">No repositories loaded</td></tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="event-panel registry-panel">
          <div class="panel-header table-header">
            <h2>Tags</h2>
            <span id="registryTagSummary">No tags loaded</span>
          </div>
          <div class="table-wrap">
            <table class="registry-table">
              <thead>
                <tr>
                  <th>Repository</th>
                  <th>Tag</th>
                  <th>Build Status</th>
                  <th>Image</th>
                  <th>Location</th>
                </tr>
              </thead>
              <tbody id="registryTagRows">
                <tr><td colspan="5" class="empty-cell">No tags loaded</td></tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section class="event-panel registry-builds-panel" aria-label="Registry backed image builds">
        <div class="panel-header table-header">
          <h2>Pushed Image Builds</h2>
          <span id="registryBuildSummary">No pushed builds loaded</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>Image</th>
                <th>Tag</th>
                <th>Location</th>
                <th>Age</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody id="registryBuildRows">
              <tr><td colspan="6" class="empty-cell">No pushed builds loaded</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </section>
  </main>
  <script src="/dashboard/dashboard.js" defer></script>
</body>
</html>
"""


DASHBOARD_CSS = """
:root {
  color-scheme: light;
  --app-bar: #07111f;
  --app-bar-line: #172235;
  --background: #f5f7fb;
  --surface: #ffffff;
  --surface-soft: #f8fafc;
  --line: #d8dee8;
  --line-soft: #e8edf4;
  --text: #0f172a;
  --muted: #64748b;
  --blue: #2563eb;
  --green: #16a34a;
  --orange: #f97316;
  --purple: #7c3aed;
  --red: #dc2626;
  --amber: #d97706;
  --shadow: 0 1px 2px rgba(15, 23, 42, 0.08);
}

:root.dark-charts {
  --background: #eef3f9;
  --surface-soft: #f7f9fc;
}

* {
  box-sizing: border-box;
}

html {
  min-width: 320px;
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

button,
select {
  cursor: pointer;
}

button:focus-visible,
input:focus-visible,
select:focus-visible {
  outline: 2px solid var(--blue);
  outline-offset: 2px;
}

.visually-hidden {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border: 0;
}

.app-bar {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  min-height: 54px;
  align-items: center;
  justify-content: space-between;
  gap: 18px;
  padding: 0 22px;
  background: var(--app-bar);
  border-bottom: 1px solid var(--app-bar-line);
  color: #f8fafc;
}

.brand,
.top-controls,
.select-control,
.icon-button,
.status-pill {
  display: inline-flex;
  align-items: center;
}

.brand {
  min-width: 0;
  gap: 14px;
}

.brand strong {
  overflow: hidden;
  font-size: 15px;
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.menu-mark {
  display: grid;
  width: 18px;
  gap: 4px;
}

.menu-mark span {
  display: block;
  height: 2px;
  border-radius: 2px;
  background: currentColor;
}

.top-controls {
  justify-content: flex-end;
  gap: 12px;
  min-width: 0;
}

.status-pill {
  min-width: 86px;
  height: 28px;
  justify-content: center;
  border: 1px solid rgba(255, 255, 255, 0.18);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.08);
  color: #dbeafe;
  font-size: 12px;
  font-weight: 700;
}

.status-ok {
  color: #bbf7d0;
}

.status-warn {
  color: #fde68a;
}

.status-bad {
  color: #fecaca;
}

.select-control {
  gap: 7px;
  color: #f8fafc;
  font-weight: 700;
}

.select-control select {
  height: 32px;
  min-width: 86px;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #f8fafc;
  font-weight: 700;
}

.select-control option {
  color: var(--text);
}

.control-value {
  display: inline-flex;
  min-width: 30px;
  height: 32px;
  align-items: center;
  color: #f8fafc;
  font-weight: 700;
}

.clock-mark,
.refresh-mark,
.pause-mark,
.moon-mark {
  position: relative;
  display: inline-block;
  width: 18px;
  height: 18px;
  flex: 0 0 auto;
}

.clock-mark {
  border: 2px solid currentColor;
  border-radius: 50%;
}

.clock-mark::before,
.clock-mark::after {
  position: absolute;
  left: 7px;
  top: 3px;
  width: 2px;
  height: 5px;
  border-radius: 2px;
  background: currentColor;
  content: "";
}

.clock-mark::after {
  top: 7px;
  width: 5px;
  height: 2px;
}

.refresh-mark {
  border: 2px solid currentColor;
  border-right-color: transparent;
  border-radius: 50%;
}

.refresh-mark::after {
  position: absolute;
  right: -1px;
  top: 0;
  width: 0;
  height: 0;
  border-left: 5px solid currentColor;
  border-top: 4px solid transparent;
  border-bottom: 4px solid transparent;
  content: "";
}

.pause-mark::before,
.pause-mark::after {
  position: absolute;
  top: 3px;
  width: 4px;
  height: 12px;
  border-radius: 1px;
  background: currentColor;
  content: "";
}

.pause-mark::before {
  left: 4px;
}

.pause-mark::after {
  right: 4px;
}

.moon-mark {
  border-radius: 50%;
  box-shadow: inset 5px 0 0 0 currentColor;
}

.icon-button {
  width: 34px;
  height: 34px;
  justify-content: center;
  border: 0;
  border-radius: 6px;
  background: transparent;
  color: #f8fafc;
  padding: 0;
}

.icon-button:hover,
.select-control:hover {
  background: rgba(255, 255, 255, 0.08);
}

.page-shell {
  width: min(100%, 1680px);
  margin: 0 auto;
  padding: 18px 22px 30px;
}

.page-title {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 14px;
}

h1,
h2,
p {
  margin: 0;
  letter-spacing: 0;
}

h1 {
  font-size: 28px;
  line-height: 1.15;
  font-weight: 800;
}

.page-title p {
  margin-top: 2px;
  color: var(--muted);
  font-size: 16px;
  font-weight: 500;
}

.title-actions {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 10px;
  flex-wrap: wrap;
}

.last-updated {
  color: var(--muted);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}

.title-actions button,
.auth-panel button {
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text);
  padding: 0 12px;
  font-weight: 700;
}

.title-actions button:hover,
.auth-panel button:hover {
  background: var(--surface-soft);
}

.auth-panel {
  display: grid;
  grid-template-columns: minmax(240px, 1fr) auto auto;
  gap: 10px;
  align-items: end;
  margin-bottom: 12px;
  padding: 12px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface);
  box-shadow: var(--shadow);
}

.auth-panel[hidden] {
  display: none;
}

.token-field {
  display: grid;
  gap: 5px;
}

.token-field span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}

.token-field input {
  width: 100%;
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfdff;
  color: var(--text);
  padding: 0 10px;
}

.page-tabs {
  display: flex;
  gap: 4px;
  margin: 0 0 12px;
  border-bottom: 1px solid var(--line);
}

.page-tab {
  height: 38px;
  border: 0;
  border-bottom: 3px solid transparent;
  background: transparent;
  color: var(--muted);
  padding: 0 14px;
  font-weight: 800;
}

.page-tab:hover {
  color: var(--text);
  background: rgba(37, 99, 235, 0.06);
}

.page-tab.is-active {
  border-bottom-color: var(--blue);
  color: var(--blue);
}

.overview-section[hidden],
.registry-page[hidden] {
  display: none;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 14px;
  margin-bottom: 12px;
}

.metric-card,
.chart-panel,
.ops-panel,
.event-panel {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  box-shadow: var(--shadow);
}

.metric-card {
  position: relative;
  display: block;
  min-height: 112px;
  padding: 15px 16px;
  overflow: hidden;
}

.metric-card > div {
  position: relative;
  z-index: 1;
  padding-right: 86px;
}

.metric-label,
.metric-detail {
  display: block;
}

.metric-label {
  color: var(--text);
  font-size: 12px;
  font-weight: 700;
  white-space: nowrap;
}

.metric-card strong {
  display: block;
  margin-top: 7px;
  color: var(--accent);
  font-size: 25px;
  line-height: 1.05;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}

.metric-detail {
  min-height: 20px;
  margin-top: 5px;
  color: var(--muted);
  font-size: 12px;
  white-space: nowrap;
  overflow-wrap: anywhere;
}

.accent-blue { --accent: var(--blue); }
.accent-green { --accent: var(--green); }
.accent-orange { --accent: var(--orange); }
.accent-purple { --accent: var(--purple); }
.accent-red { --accent: var(--red); }

.sparkline {
  position: absolute;
  right: 14px;
  bottom: 16px;
  display: block;
  width: 86px;
  height: 46px;
}

.chart-grid {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}

.chart-wide {
  grid-column: span 4;
}

.chart-small {
  grid-column: span 3;
}

.chart-panel {
  min-width: 0;
  padding: 12px 14px 11px;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 8px;
}

h2 {
  color: var(--text);
  font-size: 14px;
  line-height: 1.2;
  font-weight: 800;
}

.info-dot {
  display: inline-flex;
  width: 16px;
  height: 16px;
  align-items: center;
  justify-content: center;
  border: 1px solid #9aa7b8;
  border-radius: 50%;
  color: #64748b;
  font-size: 11px;
  font-weight: 800;
}

.info-dot::before {
  content: "i";
}

.chart-canvas {
  display: block;
  width: 100%;
  height: 208px;
}

.chart-canvas.small {
  height: 188px;
}

.chart-canvas.compact {
  height: 158px;
}

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  min-height: 18px;
  margin-top: 7px;
  color: #475569;
  font-size: 12px;
}

.legend span {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}

.swatch {
  display: inline-block;
  width: 14px;
  height: 3px;
  border-radius: 999px;
  background: currentColor;
}

.swatch.blue { color: var(--blue); }
.swatch.green { color: var(--green); }
.swatch.orange { color: var(--orange); }
.swatch.purple { color: var(--purple); }
.swatch.red { color: var(--red); }
.swatch.blue-dash {
  width: 16px;
  background: repeating-linear-gradient(90deg, var(--blue) 0 5px, transparent 5px 9px);
}

.event-panel {
  overflow: hidden;
}

.ops-grid {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}

.ops-panel {
  min-width: 0;
  padding: 12px 14px 11px;
}

.ops-panel .panel-header > span:not(.inline-badge) {
  color: var(--muted);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}

.ops-large {
  grid-column: span 7;
}

.ops-small {
  grid-column: span 5;
}

.stat-strip {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 8px;
  margin-bottom: 10px;
}

.compact-strip {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}

.stat-box {
  min-width: 0;
  padding: 9px 10px;
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  background: var(--surface-soft);
}

.stat-box span {
  display: block;
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
  white-space: nowrap;
}

.stat-box strong {
  display: block;
  margin-top: 4px;
  color: var(--text);
  font-size: 21px;
  line-height: 1.05;
  font-weight: 800;
  font-variant-numeric: tabular-nums;
}

.inline-badge,
.build-status {
  display: inline-flex;
  min-width: 68px;
  height: 22px;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  padding: 0 8px;
  font-size: 12px;
  font-weight: 800;
  white-space: nowrap;
}

.badge-ok,
.build-status.succeeded {
  background: #dcfce7;
  color: #15803d;
}

.badge-warn,
.build-status.running,
.build-status.queued {
  background: #fef3c7;
  color: #b45309;
}

.badge-bad,
.build-status.failed {
  background: #fee2e2;
  color: #b91c1c;
}

.badge-muted,
.build-status.unknown {
  background: #e2e8f0;
  color: #475569;
}

.registry-url,
.registry-detail {
  color: var(--muted);
  font-size: 12px;
  overflow-wrap: anywhere;
}

.registry-url {
  min-height: 18px;
  margin-bottom: 10px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
}

.registry-detail {
  margin: 0 0 8px;
}

.repo-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  max-height: 72px;
  overflow: auto;
}

.repo-pill,
.empty-inline {
  display: inline-flex;
  max-width: 100%;
  align-items: center;
  border-radius: 4px;
  padding: 4px 7px;
  font-size: 12px;
}

.repo-pill {
  border: 1px solid #dbeafe;
  background: #eff6ff;
  color: #1d4ed8;
  overflow-wrap: anywhere;
}

.tag-chip-list {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
  min-width: 280px;
  max-width: 620px;
}

.tag-chip {
  display: inline-flex;
  max-width: 220px;
  align-items: center;
  border: 1px solid var(--line-soft);
  border-radius: 4px;
  background: var(--surface-soft);
  color: #334155;
  padding: 3px 6px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-size: 11px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.registry-page {
  display: grid;
  gap: 12px;
}

.registry-hero,
.registry-toolbar {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  box-shadow: var(--shadow);
}

.registry-hero {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(420px, 0.85fr);
  gap: 14px;
  padding: 14px;
}

.registry-hero-main {
  min-width: 0;
}

.registry-url-large {
  min-height: 22px;
  margin-bottom: 8px;
  color: var(--text);
  font-size: 13px;
}

.registry-copy {
  color: var(--muted);
  font-size: 12px;
  overflow-wrap: anywhere;
}

.registry-stat-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.registry-toolbar {
  display: grid;
  grid-template-columns: minmax(280px, 1fr) 220px minmax(220px, auto);
  gap: 10px;
  align-items: end;
  padding: 12px 14px;
}

.registry-search,
.registry-select {
  display: grid;
  gap: 5px;
}

.registry-search span,
.registry-select span {
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
}

.registry-search input,
.registry-select select {
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfdff;
  color: var(--text);
  padding: 0 10px;
}

.registry-full-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr);
  gap: 12px;
}

.registry-panel {
  min-width: 0;
}

.registry-builds-panel {
  min-width: 0;
}

.registry-table {
  min-width: 780px;
}

.registry-table td:first-child,
.registry-table td:nth-child(2),
.registry-table td:nth-child(4) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}

.registry-builds-panel table {
  table-layout: fixed;
}

.registry-builds-panel th:nth-child(1),
.registry-builds-panel td:nth-child(1) {
  width: 112px;
}

.registry-builds-panel th:nth-child(2),
.registry-builds-panel td:nth-child(2) {
  width: 220px;
}

.registry-builds-panel th:nth-child(3),
.registry-builds-panel td:nth-child(3) {
  width: 46%;
  white-space: normal;
  overflow-wrap: anywhere;
}

.registry-builds-panel th:nth-child(4),
.registry-builds-panel td:nth-child(4),
.registry-builds-panel th:nth-child(5),
.registry-builds-panel td:nth-child(5) {
  width: 90px;
}

.registry-builds-panel th:nth-child(6),
.registry-builds-panel td:nth-child(6) {
  width: 220px;
}

.empty-inline {
  color: var(--muted);
}

.build-panel {
  margin-bottom: 12px;
}

.table-header {
  min-height: 32px;
  margin: 0;
  padding: 0 14px;
  border-bottom: 1px solid var(--line);
}

.table-header span {
  color: var(--muted);
  font-size: 12px;
}

.table-wrap {
  overflow-x: auto;
}

table {
  width: 100%;
  min-width: 920px;
  border-collapse: collapse;
  font-variant-numeric: tabular-nums;
}

th,
td {
  padding: 8px 14px;
  border-bottom: 1px solid var(--line-soft);
  text-align: left;
  vertical-align: top;
  white-space: nowrap;
}

th {
  background: linear-gradient(#fbfcfe, #f8fafc);
  color: #334155;
  font-size: 12px;
  font-weight: 800;
}

tbody tr:last-child td {
  border-bottom: 0;
}

td:last-child {
  white-space: normal;
  overflow-wrap: anywhere;
}

.severity-badge {
  display: inline-flex;
  min-width: 44px;
  height: 20px;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  padding: 0 8px;
  font-size: 12px;
  font-weight: 800;
}

.severity-info {
  background: #dbeafe;
  color: #1d4ed8;
}

.severity-warn {
  background: #fef3c7;
  color: #b45309;
}

.severity-alert {
  background: #fee2e2;
  color: #b91c1c;
}

.empty-cell {
  color: var(--muted);
  text-align: center;
}

@media (max-width: 1320px) {
  .metric-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .chart-wide,
  .chart-small {
    grid-column: span 6;
  }

  .ops-large,
  .ops-small {
    grid-column: 1 / -1;
  }

  .registry-full-grid {
    grid-template-columns: 1fr;
  }

  .registry-toolbar {
    grid-template-columns: 1fr 220px;
  }

  .registry-toolbar .registry-copy {
    grid-column: 1 / -1;
  }
}

@media (max-width: 860px) {
  .app-bar,
  .page-title {
    align-items: flex-start;
    flex-direction: column;
  }

  .app-bar {
    position: static;
    padding: 12px 16px;
  }

  .top-controls {
    width: 100%;
    justify-content: flex-start;
    flex-wrap: wrap;
  }

  .page-shell {
    padding: 16px 12px 24px;
  }

  h1 {
    font-size: 24px;
  }

  .title-actions {
    width: 100%;
    justify-content: flex-start;
  }

  .auth-panel {
    grid-template-columns: 1fr 1fr;
  }

  .token-field {
    grid-column: 1 / -1;
  }

  .metric-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .metric-card {
    min-height: 106px;
  }

  .sparkline {
    width: 96px;
    height: 44px;
  }

  .chart-wide,
  .chart-small,
  .ops-large,
  .ops-small {
    grid-column: 1 / -1;
  }

  .stat-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .registry-toolbar,
  .registry-stat-grid {
    grid-template-columns: 1fr;
  }

  .registry-hero {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 560px) {
  .metric-grid {
    grid-template-columns: 1fr;
  }

  .stat-strip,
  .compact-strip {
    grid-template-columns: 1fr;
  }

  .status-pill {
    min-width: 76px;
  }
}
"""


DASHBOARD_JS = """
const MAX_HISTORY = 720;
const REFRESH_INTERVAL_MS = 5000;

const state = {
  timer: null,
  paused: false,
  currentPage: "overview",
  history: [],
  lastSnapshot: null,
};

const palette = {
  blue: "#2563eb",
  blueSoft: "rgba(37, 99, 235, 0.12)",
  green: "#16a34a",
  greenSoft: "rgba(22, 163, 74, 0.12)",
  orange: "#f97316",
  orangeSoft: "rgba(249, 115, 22, 0.12)",
  purple: "#7c3aed",
  purpleSoft: "rgba(124, 58, 237, 0.12)",
  red: "#dc2626",
  redSoft: "rgba(220, 38, 38, 0.12)",
  grid: "#dfe5ee",
  text: "#0f172a",
  muted: "#64748b",
  plotBg: "#ffffff",
};

const els = {};

document.addEventListener("DOMContentLoaded", () => {
  for (const id of [
    "connectionStatus",
    "lastUpdated",
    "timeRangeSelect",
    "pauseButton",
    "themeButton",
    "authToggleButton",
    "authPanel",
    "tokenInput",
    "saveTokenButton",
    "clearTokenButton",
    "activeNodesValue",
    "activeNodesDetail",
    "runningSandboxesValue",
    "runningSandboxesDetail",
    "cpuUtilizationValue",
    "cpuUtilizationDetail",
    "memoryUtilizationValue",
    "memoryUtilizationDetail",
    "queueDepthValue",
    "queueDepthDetail",
    "errorRateValue",
    "errorRateDetail",
    "builderSummary",
    "builderReadyValue",
    "builderPreparedValue",
    "builderActiveBuildsValue",
    "builderCpuValue",
    "builderMemoryValue",
    "registryStatusBadge",
    "registryUrl",
    "registryReposValue",
    "registryTagsValue",
    "registryDetail",
    "registryRepos",
    "registryPage",
    "registryPageStatusBadge",
    "registryPageUrl",
    "registryPageHealthDetail",
    "registryPageReposValue",
    "registryPageTagsValue",
    "registryPageVisibleTagsValue",
    "registryPageCoverageValue",
    "registrySearchInput",
    "registryFilterSelect",
    "registryPageSummary",
    "registryRepoSummary",
    "registryRepoRows",
    "registryTagSummary",
    "registryTagRows",
    "registryBuildSummary",
    "registryBuildRows",
    "buildSummary",
    "buildRows",
    "traceSummary",
    "traceRows",
    "eventSummary",
    "eventRows",
  ]) {
    els[id] = document.getElementById(id);
  }

  const savedToken = sessionStorage.getItem("ucloud.dashboard.token") || "";
  els.tokenInput.value = savedToken;
  syncAuthPanel(!savedToken);
  els.authToggleButton.addEventListener("click", () => syncAuthPanel(els.authPanel.hidden));
  els.saveTokenButton.addEventListener("click", saveToken);
  els.clearTokenButton.addEventListener("click", clearToken);
  els.timeRangeSelect.addEventListener("change", () => {
    trimHistory();
    redrawCharts();
  });
  els.pauseButton.addEventListener("click", togglePause);
  els.themeButton.addEventListener("click", () => document.documentElement.classList.toggle("dark-charts"));
  document.querySelectorAll("[data-page-target]").forEach((button) => {
    button.addEventListener("click", () => setPage(button.dataset.pageTarget || "overview"));
  });
  window.addEventListener("hashchange", () => setPage(pageFromHash(), { updateHash: false }));
  els.registrySearchInput.addEventListener("input", () => renderRegistryPage(state.lastSnapshot || {}));
  els.registryFilterSelect.addEventListener("change", () => renderRegistryPage(state.lastSnapshot || {}));
  window.addEventListener("resize", redrawCharts);
  setPage(pageFromHash(), { updateHash: false });
  refreshNow();
  scheduleNextRefresh();
});

function saveToken() {
  const token = els.tokenInput.value.trim();
  if (token) {
    sessionStorage.setItem("ucloud.dashboard.token", token);
    syncAuthPanel(false);
    setStatus("Saved", "ok");
    refreshNow();
    return;
  }
  clearToken();
}

function clearToken() {
  sessionStorage.removeItem("ucloud.dashboard.token");
  els.tokenInput.value = "";
  syncAuthPanel(true);
  setStatus("Auth required", "warn");
  els.lastUpdated.textContent = "Enter the gateway bearer token";
}

function syncAuthPanel(show) {
  els.authPanel.hidden = !show;
  els.authToggleButton.setAttribute("aria-expanded", String(show));
}

function togglePause() {
  state.paused = !state.paused;
  els.pauseButton.title = state.paused ? "Resume refresh" : "Pause refresh";
  els.pauseButton.setAttribute("aria-label", els.pauseButton.title);
  els.pauseButton.classList.toggle("is-paused", state.paused);
  if (!state.paused) {
    refreshNow();
  }
}

function pageFromHash() {
  return window.location.hash === "#registry" ? "registry" : "overview";
}

function setPage(page, options = {}) {
  const next = page === "registry" ? "registry" : "overview";
  state.currentPage = next;
  document.querySelectorAll(".overview-section").forEach((section) => {
    section.hidden = next !== "overview";
  });
  if (els.registryPage) {
    els.registryPage.hidden = next !== "registry";
  }
  document.querySelectorAll("[data-page-target]").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.pageTarget === next);
  });
  if (options.updateHash !== false) {
    const hash = next === "registry" ? "#registry" : "#overview";
    if (window.location.hash !== hash) {
      window.history.replaceState(null, "", hash);
    }
  }
  if (next === "overview") {
    redrawCharts();
  } else {
    renderRegistryPage(state.lastSnapshot || {});
  }
}

function scheduleNextRefresh() {
  if (state.timer !== null) {
    window.clearInterval(state.timer);
  }
  state.timer = window.setInterval(() => {
    if (!state.paused) {
      refreshNow();
    }
  }, REFRESH_INTERVAL_MS);
}

async function refreshNow() {
  const token = sessionStorage.getItem("ucloud.dashboard.token") || els.tokenInput.value.trim();
  const headers = token ? { "X-UCloud-Sandbox-Token": token } : {};
  try {
    const response = await fetch("/v1/metrics", {
      headers,
      cache: "no-store",
    });
    if (response.status === 401) {
      setStatus("Auth required", "warn");
      els.lastUpdated.textContent = "Enter the gateway bearer token";
      syncAuthPanel(true);
      redrawCharts();
      return;
    }
    if (!response.ok) {
      setStatus(`HTTP ${response.status}`, "bad");
      els.lastUpdated.textContent = "Metrics request failed";
      return;
    }
    const snapshot = await response.json();
    setStatus("Live", "ok");
    syncAuthPanel(false);
    renderSnapshot(snapshot);
  } catch (error) {
    setStatus("Offline", "bad");
    els.lastUpdated.textContent = String(error && error.message ? error.message : error);
    redrawCharts();
  }
}

function setStatus(text, mode) {
  els.connectionStatus.textContent = text;
  els.connectionStatus.className = `status-pill status-${mode || "warn"}`;
}

function renderSnapshot(snapshot) {
  state.lastSnapshot = snapshot;
  state.history.push(pointFromSnapshot(snapshot));
  trimHistory();
  els.lastUpdated.textContent = `Updated ${formatTime(snapshot.generated_at)}`;
  renderMetrics(snapshot);
  renderRegistryPage(snapshot);
  renderBuilds(snapshot);
  renderTraces(snapshot);
  renderEvents(snapshot);
  redrawCharts();
}

function trimHistory() {
  const windowMs = Number(els.timeRangeSelect.value) || 3600000;
  const cutoff = Date.now() - windowMs;
  state.history = state.history.filter((point) => point.at >= cutoff);
  if (state.history.length > MAX_HISTORY) {
    state.history.splice(0, state.history.length - MAX_HISTORY);
  }
}

function pointFromSnapshot(snapshot) {
  const nodes = snapshot.nodes || {};
  const resources = snapshot.resources || {};
  const sandboxResources = resources.sandbox || {};
  const actual = sandboxResources.actual_usage || {};
  const load = sandboxResources.load || {};
  const sandboxes = snapshot.sandboxes || {};
  const capacity = snapshot.capacity || {};
  const images = snapshot.images || {};
  const builders = snapshot.builders || {};
  const scale = snapshot.scale_up || {};
  const recentEvents = ((snapshot.events || {}).recent || []);
  const queueDepth = asNumber(sandboxes.pending)
    + asNumber(capacity.prepared_sandboxes)
    + asNumber(images.pending_builds)
    + asNumber(builders.prepared_builders);
  const cpuActual = nullableNumber(actual.cpu_percent_avg);
  const memoryActual = nullableNumber(actual.memory_percent);
  const cpuReserved = ratioToPercent(load.vcpu);
  const memoryReserved = ratioToPercent(load.memory);
  return {
    at: Date.parse(snapshot.generated_at) || Date.now(),
    activeNodes: asNumber(nodes.sandbox),
    freshNodes: asNumber(nodes.fresh),
    activeSandboxes: asNumber(sandboxes.running),
    builderNodes: asNumber(nodes.builder),
    queueDepth,
    pendingSandboxes: asNumber(sandboxes.pending),
    preparedSandboxes: asNumber(capacity.prepared_sandboxes),
    pendingBuilds: asNumber(images.pending_builds),
    activeBuilds: asNumber(images.active_builds),
    preparedBuilders: asNumber(builders.prepared_builders),
    cpuUtilization: firstNumber(cpuActual, cpuReserved),
    cpuReserved,
    memoryUtilization: firstNumber(memoryActual, memoryReserved),
    memoryReserved,
    scaleP50Seconds: msToSeconds(scale.p50_ms),
    scaleP95Seconds: msToSeconds(scale.p95_ms),
    startP50Seconds: msToSeconds(scale.p50_ms),
    errorRate: eventErrorRate(recentEvents),
  };
}

function renderMetrics(snapshot) {
  const latest = state.history[state.history.length - 1] || pointFromSnapshot(snapshot);
  const nodes = snapshot.nodes || {};
  const sandboxes = snapshot.sandboxes || {};
  const capacity = snapshot.capacity || {};
  const exec = snapshot.exec || {};
  const images = snapshot.images || {};
  const builders = snapshot.builders || {};
  const registry = snapshot.registry || {};
  const resources = snapshot.resources || {};
  const sandboxResources = resources.sandbox || {};
  const builderResources = resources.builder || {};
  const actual = sandboxResources.actual_usage || {};
  const load = sandboxResources.load || {};
  const builderActual = builderResources.actual_usage || {};
  const builderLoad = builderResources.load || {};
  const scale = snapshot.scale_up || {};

  setText("activeNodesValue", String(latest.activeNodes));
  setText("activeNodesDetail", `${asNumber(nodes.sandbox)} sandbox, ${asNumber(nodes.builder)} builder`);

  setText("runningSandboxesValue", formatInteger(latest.activeSandboxes));
  const staleRouteText = asNumber(sandboxes.stale_routes) > 0
    ? `, ${asNumber(sandboxes.stale_routes)} stale routes`
    : "";
  setText("runningSandboxesDetail", `${asNumber(sandboxes.pending)} pending, ${asNumber(exec.sessions)} exec, ${asNumber(sandboxes.active_routes)} routes${staleRouteText}`);

  setText("cpuUtilizationValue", formatPercentPoint(latest.cpuUtilization));
  setText("cpuUtilizationDetail", `Avg ${formatPercentPoint(actual.cpu_percent_avg)}, reserved ${formatPercentPoint(ratioToPercent(load.vcpu))}`);

  setText("memoryUtilizationValue", formatPercentPoint(latest.memoryUtilization));
  setText("memoryUtilizationDetail", `Avg ${formatPercentPoint(actual.memory_percent)}, reserved ${formatPercentPoint(ratioToPercent(load.memory))}`);

  setText("queueDepthValue", formatInteger(latest.queueDepth));
  setText("queueDepthDetail", `${asNumber(sandboxes.pending)} pend, ${asNumber(capacity.prepared_sandboxes)} warm, ${asNumber(images.active_builds)} active builds, ${asNumber(images.pending_builds)} build waits, ${asNumber(builders.prepared_builders)} builders`);

  setText("errorRateValue", formatPercentDecimal(latest.errorRate));
  setText("errorRateDetail", `Avg ${formatPercentDecimal(average(state.history.map((p) => p.errorRate)))}`);

  setText("builderReadyValue", formatInteger(nodes.builder));
  setText("builderPreparedValue", formatInteger(builders.prepared_builders));
  setText("builderActiveBuildsValue", formatInteger(images.active_builds));
  setText("builderCpuValue", formatPercentPoint(firstNumber(builderActual.cpu_percent_avg, ratioToPercent(builderLoad.vcpu))));
  setText("builderMemoryValue", formatPercentPoint(firstNumber(builderActual.memory_percent, ratioToPercent(builderLoad.memory))));
  const oldestBuildWait = asNumber(images.pending_builds) > 0
    ? formatAge(images.oldest_pending_build_seconds)
    : "none";
  setText(
    "builderSummary",
    `${asNumber(images.pending_builds)} waiting, ${oldestBuildWait} oldest wait, ${asNumber(images.failed_builds)} failed`
  );
  renderRegistry(registry);

  if (latest.queueDepth > 0 || asNumber(images.pending_builds) > 0) {
    setStatus("Demand pending", "warn");
  }
  if (asNumber(images.active_builds) > 0) {
    setStatus("Build running", "warn");
  }
  if (asNumber(capacity.prepared_sandboxes) > 0 && asNumber(sandboxes.pending) === 0) {
    setStatus("Prepared", "ok");
  }
  if (latest.errorRate > 0) {
    setStatus("Alerts", "bad");
  }
  if (nullableNumber(scale.p95_ms) !== null && Number(scale.p95_ms) > 300000) {
    setStatus("Slow scale-up", "warn");
  }
}

function renderEvents(snapshot) {
  const events = ((snapshot.events || {}).recent || []).slice(-12).reverse();
  els.eventSummary.textContent = events.length ? `${events.length} recent events` : "No recent events";
  if (events.length === 0) {
    els.eventRows.innerHTML = '<tr><td colspan="4" class="empty-cell">No recent events</td></tr>';
    return;
  }
  els.eventRows.replaceChildren(...events.map(eventRow));
}

function renderRegistry(registry) {
  const configured = Boolean(registry.configured);
  const ok = Boolean(registry.ok);
  els.registryStatusBadge.textContent = configured ? (ok ? "Online" : "Offline") : "Not set";
  els.registryStatusBadge.className = `inline-badge ${configured ? (ok ? "badge-ok" : "badge-bad") : "badge-muted"}`;
  setText("registryUrl", registry.url || "No registry configured");
  setText("registryReposValue", configured ? formatInteger(registry.repository_count) : "-");
  setText("registryTagsValue", configured ? formatInteger(registry.scanned_tag_count) : "-");
  if (!configured) {
    setText("registryDetail", "Set --registry-url or UCLOUD_SANDBOX_REGISTRY_URL to show registry health.");
    els.registryRepos.innerHTML = '<span class="empty-inline">No registry configured</span>';
    return;
  }
  if (!ok) {
    setText("registryDetail", registry.error ? `Registry check failed: ${registry.error}` : "Registry check failed");
    els.registryRepos.innerHTML = '<span class="empty-inline">Registry unavailable</span>';
    return;
  }
  const repos = Array.isArray(registry.repositories) ? registry.repositories : [];
  const truncated = registry.catalog_truncated ? ", catalog truncated" : "";
  setText("registryDetail", `${formatInteger(registry.scanned_repository_count)} repositories scanned${truncated}`);
  if (repos.length === 0) {
    els.registryRepos.innerHTML = '<span class="empty-inline">Registry is empty</span>';
    return;
  }
  els.registryRepos.replaceChildren(...repos.slice(0, 12).map(repoPill));
}

function renderRegistryPage(snapshot) {
  if (!els.registryPage) return;
  const registry = snapshot.registry || {};
  const images = snapshot.images || {};
  const builds = Array.isArray(images.builds) ? images.builds.slice() : [];
  const registryBuilds = pushedRegistryBuilds(builds);
  const buildByTag = buildsByRegistryTag(registryBuilds);
  const configured = Boolean(registry.configured);
  const ok = Boolean(registry.ok);
  const repos = Array.isArray(registry.repositories) ? registry.repositories : [];
  const query = String(els.registrySearchInput.value || "").trim().toLowerCase();
  const filter = String(els.registryFilterSelect.value || "all");

  els.registryPageStatusBadge.textContent = configured ? (ok ? "Online" : "Offline") : "Not set";
  els.registryPageStatusBadge.className = `inline-badge ${configured ? (ok ? "badge-ok" : "badge-bad") : "badge-muted"}`;
  setText("registryPageUrl", registry.url || "No registry configured");
  setText("registryPageReposValue", configured ? formatInteger(registry.repository_count) : "-");
  setText("registryPageTagsValue", configured ? formatInteger(registry.scanned_tag_count) : "-");
  setText("registryPageVisibleTagsValue", configured ? formatInteger(registry.visible_tag_count) : "-");
  const scanned = asNumber(registry.scanned_repository_count);
  const total = asNumber(registry.repository_count);
  setText("registryPageCoverageValue", configured && total > 0 ? `${formatInteger(scanned)}/${formatInteger(total)}` : "-");

  if (!configured) {
    setText("registryPageHealthDetail", "Set --registry-url or UCLOUD_SANDBOX_REGISTRY_URL to show registry health.");
    setText("registryPageSummary", "No registry configured");
    setText("registryRepoSummary", "No repositories loaded");
    setText("registryTagSummary", "No tags loaded");
    renderEmptyRow(els.registryRepoRows, 5, "No registry configured");
    renderEmptyRow(els.registryTagRows, 5, "No registry configured");
    renderRegistryBuildRows(registryBuilds);
    return;
  }
  if (!ok) {
    setText("registryPageHealthDetail", registry.error ? `Registry check failed: ${registry.error}` : "Registry check failed");
    setText("registryPageSummary", "Registry unavailable");
    setText("registryRepoSummary", "No repositories loaded");
    setText("registryTagSummary", "No tags loaded");
    renderEmptyRow(els.registryRepoRows, 5, "Registry unavailable");
    renderEmptyRow(els.registryTagRows, 5, "Registry unavailable");
    renderRegistryBuildRows(registryBuilds);
    return;
  }

  const truncated = registry.catalog_truncated ? ", catalog truncated" : "";
  setText(
    "registryPageHealthDetail",
    `${formatInteger(scanned)} repositories scanned, ${formatInteger(registry.scanned_tag_count)} tags observed${truncated}`
  );

  const filteredRepos = repos.filter((repo) => registryRepoMatches(repo, buildByTag, filter, query));
  const flattenedTags = flattenRegistryTags(filteredRepos, buildByTag)
    .filter((item) => !query || matchesRegistrySearch(item.searchText, query));
  const summaryParts = [
    `${formatInteger(filteredRepos.length)} repositories`,
    `${formatInteger(flattenedTags.length)} visible tags`,
    `${formatInteger(registryBuilds.length)} pushed builds`,
  ];
  if (query) summaryParts.push(`matching "${query}"`);
  setText("registryPageSummary", summaryParts.join(", "));
  setText("registryRepoSummary", `${formatInteger(filteredRepos.length)} shown`);
  setText("registryTagSummary", `${formatInteger(flattenedTags.length)} shown`);

  if (filteredRepos.length === 0) {
    renderEmptyRow(els.registryRepoRows, 5, "No repositories match the current filter");
  } else {
    els.registryRepoRows.replaceChildren(...filteredRepos.map((repo) => registryRepoRow(repo, buildByTag)));
  }
  if (flattenedTags.length === 0) {
    renderEmptyRow(els.registryTagRows, 5, "No tags match the current filter");
  } else {
    els.registryTagRows.replaceChildren(...flattenedTags.slice(0, 200).map(registryTagRow));
  }
  renderRegistryBuildRows(registryBuilds.filter((build) => !query || matchesRegistrySearch(buildSearchText(build), query)));
}

function registryRepoMatches(repo, buildByTag, filter, query) {
  const tags = Array.isArray(repo.tags) ? repo.tags : [];
  const builds = tags.flatMap((tag) => buildByTag.get(`${repo.repository}:${tag}`) || []);
  if (filter === "with-builds" && builds.length === 0) return false;
  if (filter === "truncated" && !repo.tags_truncated) return false;
  if (filter === "empty" && tags.length > 0) return false;
  if (!query) return true;
  return matchesRegistrySearch([
    repo.repository,
    repo.namespace,
    repo.latest_tag,
    tags.join(" "),
    builds.map(buildSearchText).join(" "),
  ].join(" "), query);
}

function flattenRegistryTags(repos, buildByTag) {
  const items = [];
  for (const repo of repos) {
    const tags = Array.isArray(repo.tags) ? repo.tags : [];
    for (const tag of tags) {
      const key = `${repo.repository}:${tag}`;
      const builds = buildByTag.get(key) || [];
      items.push({
        repository: repo.repository || "-",
        tag,
        key,
        builds,
        latestBuild: latestBuild(builds),
        searchText: [
          repo.repository,
          tag,
          ...builds.map(buildSearchText),
        ].join(" "),
      });
    }
  }
  items.sort((a, b) => a.repository.localeCompare(b.repository) || b.tag.localeCompare(a.tag));
  return items;
}

function registryRepoRow(repo, buildByTag) {
  const tr = document.createElement("tr");
  const tags = Array.isArray(repo.tags) ? repo.tags : [];
  const buildCount = tags.reduce((total, tag) => total + (buildByTag.get(`${repo.repository}:${tag}`)?.length || 0), 0);
  appendCell(tr, repo.repository || "-");
  appendCell(tr, formatInteger(repo.tag_count));
  appendCell(tr, repo.latest_tag || "-");
  appendCell(tr, formatInteger(buildCount));
  const tagCell = document.createElement("td");
  appendTagChips(tagCell, tags, repo.tags_truncated, repo.tag_count);
  tr.append(tagCell);
  return tr;
}

function registryTagRow(item) {
  const tr = document.createElement("tr");
  const build = item.latestBuild || {};
  appendCell(tr, item.repository);
  appendCell(tr, item.tag || "-");
  appendCell(tr, build.status || "-");
  appendCell(tr, build.image_id || "-");
  appendCell(tr, buildLocation(build));
  return tr;
}

function renderRegistryBuildRows(builds) {
  const ordered = builds.slice().sort((a, b) => (Date.parse(b.updated_at) || 0) - (Date.parse(a.updated_at) || 0));
  setText("registryBuildSummary", ordered.length ? `${formatInteger(ordered.length)} pushed builds` : "No pushed builds loaded");
  if (ordered.length === 0) {
    renderEmptyRow(els.registryBuildRows, 6, "No pushed builds loaded");
    return;
  }
  els.registryBuildRows.replaceChildren(...ordered.slice(0, 100).map(buildRow));
}

function pushedRegistryBuilds(builds) {
  return builds.filter((build) => {
    const image = build.image || {};
    return Boolean(build.push || image.pushed || imageTagHasRegistryHost(build.tag));
  });
}

function buildsByRegistryTag(builds) {
  const byTag = new Map();
  for (const build of builds) {
    const tag = String(build.tag || "");
    const repository = registryRepositoryFromTag(tag);
    const tagName = registryTagName(tag);
    if (!repository || !tagName) continue;
    const key = `${repository}:${tagName}`;
    const items = byTag.get(key) || [];
    items.push(build);
    byTag.set(key, items);
  }
  return byTag;
}

function registryRepositoryFromTag(imageTag) {
  const parts = splitRegistryTag(imageTag);
  return parts.repository;
}

function registryTagName(imageTag) {
  const parts = splitRegistryTag(imageTag);
  return parts.tag;
}

function splitRegistryTag(imageTag) {
  const raw = String(imageTag || "").trim();
  if (!raw) return { repository: "", tag: "" };
  const lastSlash = raw.lastIndexOf("/");
  const lastColon = raw.lastIndexOf(":");
  const hasTag = lastColon > lastSlash;
  const name = hasTag ? raw.slice(0, lastColon) : raw;
  const tag = hasTag ? raw.slice(lastColon + 1) : "";
  const segments = name.split("/").filter(Boolean);
  if (segments.length > 1 && isRegistryHostSegment(segments[0])) {
    segments.shift();
  }
  return { repository: segments.join("/"), tag };
}

function isRegistryHostSegment(segment) {
  return segment === "localhost" || segment.includes(".") || segment.includes(":");
}

function imageTagHasRegistryHost(imageTag) {
  const raw = String(imageTag || "").trim();
  const firstSlash = raw.indexOf("/");
  if (firstSlash < 0) return false;
  return isRegistryHostSegment(raw.slice(0, firstSlash));
}

function latestBuild(builds) {
  if (!Array.isArray(builds) || builds.length === 0) return null;
  return builds.slice().sort((a, b) => (Date.parse(b.updated_at) || 0) - (Date.parse(a.updated_at) || 0))[0];
}

function appendTagChips(cell, tags, truncated, totalTagCount) {
  if (!tags.length) {
    cell.textContent = "-";
    return;
  }
  const list = document.createElement("div");
  list.className = "tag-chip-list";
  for (const tag of tags.slice(0, 10)) {
    const chip = document.createElement("span");
    chip.className = "tag-chip";
    chip.textContent = tag;
    chip.title = tag;
    list.append(chip);
  }
  if (truncated || tags.length > 10) {
    const more = document.createElement("span");
    more.className = "tag-chip";
    const hidden = Math.max(0, asNumber(totalTagCount) - Math.min(tags.length, 10));
    more.textContent = hidden > 0 ? `+${hidden} older` : "older tags omitted";
    more.title = "Older tags omitted from the dashboard payload";
    list.append(more);
  }
  cell.append(list);
}

function buildSearchText(build) {
  return [
    build.image_id,
    build.tag,
    build.status,
    buildLocation(build),
    buildDetails(build),
  ].join(" ");
}

function matchesRegistrySearch(text, query) {
  if (!query) return true;
  return String(text || "").toLowerCase().includes(query);
}

function renderEmptyRow(tbody, columns, message) {
  const tr = document.createElement("tr");
  const td = document.createElement("td");
  td.colSpan = columns;
  td.className = "empty-cell";
  td.textContent = message;
  tr.append(td);
  tbody.replaceChildren(tr);
}

function repoPill(repo) {
  const item = document.createElement("span");
  item.className = "repo-pill";
  const tags = Array.isArray(repo.tags) ? repo.tags : [];
  const latest = tags.length ? `:${tags[tags.length - 1]}` : "";
  item.textContent = `${repo.repository || "repository"}${latest} (${asNumber(repo.tag_count)})`;
  item.title = tags.join(", ");
  return item;
}

function renderBuilds(snapshot) {
  const images = snapshot.images || {};
  const builds = Array.isArray(images.builds) ? images.builds.slice() : [];
  builds.sort((a, b) => (Date.parse(b.updated_at) || 0) - (Date.parse(a.updated_at) || 0));
  const active = builds.filter((build) => !["succeeded", "failed"].includes(String(build.status || ""))).length;
  els.buildSummary.textContent = builds.length
    ? `${builds.length} tracked, ${active} active`
    : "No tracked builds";
  if (builds.length === 0) {
    els.buildRows.innerHTML = '<tr><td colspan="6" class="empty-cell">No image builds tracked yet</td></tr>';
    return;
  }
  els.buildRows.replaceChildren(...builds.slice(0, 10).map(buildRow));
}

function buildRow(build) {
  const tr = document.createElement("tr");
  const statusCell = document.createElement("td");
  const status = String(build.status || "unknown");
  const badge = document.createElement("span");
  badge.className = `build-status ${statusClass(status)}`;
  badge.textContent = status;
  statusCell.append(badge);
  tr.append(statusCell);
  appendCell(tr, build.image_id || "-");
  appendCell(tr, build.tag || "-");
  appendCell(tr, buildLocation(build));
  appendCell(tr, buildAge(build));
  appendCell(tr, buildDetails(build));
  return tr;
}

function statusClass(status) {
  if (status === "succeeded" || status === "failed" || status === "running" || status === "queued") {
    return status;
  }
  return "unknown";
}

function buildLocation(build) {
  if (build.location) return build.location;
  const node = build.node || {};
  return node.node_id || node.job_id || "-";
}

function buildAge(build) {
  const started = Date.parse(build.started_at || build.created_at);
  const finished = Date.parse(build.finished_at || build.updated_at);
  if (!Number.isFinite(started)) return "-";
  if (String(build.status || "") === "running") {
    return `${formatAge((Date.now() - started) / 1000)} running`;
  }
  if (!Number.isFinite(finished) || finished < started) {
    return formatAge((Date.now() - started) / 1000);
  }
  return formatAge((finished - started) / 1000);
}

function buildDetails(build) {
  if (build.error) return build.error;
  const parts = [];
  const timings = build.timings || {};
  const phases = timings.phases || {};
  if (Number.isFinite(Number(timings.total_ms))) parts.push(`total ${formatDurationMs(timings.total_ms)}`);
  if (Number.isFinite(Number(phases.docker_build_ms))) parts.push(`build ${formatDurationMs(phases.docker_build_ms)}`);
  if (Number.isFinite(Number(phases.docker_push_ms))) parts.push(`push ${formatDurationMs(phases.docker_push_ms)}`);
  if (build.push) parts.push("push enabled");
  if (build.exit_code !== null && build.exit_code !== undefined) parts.push(`build exit ${build.exit_code}`);
  if (build.push_exit_code !== null && build.push_exit_code !== undefined) parts.push(`push exit ${build.push_exit_code}`);
  const tail = String(build.log_tail || "").trim().split("\\n").filter(Boolean).slice(-1)[0];
  if (tail) parts.push(tail.slice(0, 160));
  return parts.join(", ") || "-";
}

function renderTraces(snapshot) {
  const traces = snapshot.traces || {};
  const items = Array.isArray(traces.recent) ? traces.recent.slice(-12).reverse() : [];
  els.traceSummary.textContent = items.length
    ? `${items.length} traces, ${formatInteger(traces.span_count)} spans`
    : "No traces";
  if (items.length === 0) {
    els.traceRows.innerHTML = '<tr><td colspan="5" class="empty-cell">No traces loaded</td></tr>';
    return;
  }
  els.traceRows.replaceChildren(...items.map(traceRow));
}

function traceRow(trace) {
  const tr = document.createElement("tr");
  appendCell(tr, formatTime(trace.started_at));
  const statusCell = document.createElement("td");
  const status = String(trace.status || "ok");
  const badge = document.createElement("span");
  badge.className = `build-status ${status === "ok" ? "succeeded" : "failed"}`;
  badge.textContent = status;
  statusCell.append(badge);
  tr.append(statusCell);
  appendCell(tr, trace.name || "-");
  appendCell(tr, formatDurationMs(trace.duration_ms));
  appendCell(tr, traceDetails(trace));
  return tr;
}

function traceDetails(trace) {
  const spans = Array.isArray(trace.spans) ? trace.spans.slice() : [];
  spans.sort((a, b) => asNumber(b.duration_ms) - asNumber(a.duration_ms));
  const slow = spans.slice(0, 3).map((span) => `${span.name || "span"} ${formatDurationMs(span.duration_ms)}`);
  const attrs = (spans[0] && spans[0].attributes) || {};
  const outcome = attrs.outcome ? `outcome ${attrs.outcome}` : "";
  return [outcome, ...slow].filter(Boolean).join(", ") || `${formatInteger(trace.span_count)} spans`;
}

function eventRow(event) {
  const tr = document.createElement("tr");
  const severity = severityForEvent(event);
  appendCell(tr, formatTime(event.timestamp));
  const severityCell = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = `severity-badge severity-${severity.toLowerCase()}`;
  badge.textContent = severity;
  severityCell.append(badge);
  tr.append(severityCell);
  appendCell(tr, titleForEvent(event));
  appendCell(tr, summarizeEvent(event));
  return tr;
}

function appendCell(row, value) {
  const td = document.createElement("td");
  td.textContent = value;
  row.append(td);
}

function severityForEvent(event) {
  const data = event.data || {};
  if (event.kind === "sandbox_pending_deleted") return "ALERT";
  if (event.kind === "autoscaler_cycle") {
    const actions = Array.isArray(data.actions) ? data.actions : [];
    const builderActions = Array.isArray(data.builder_actions) ? data.builder_actions : [];
    const hasDeficit = resourceHasPositiveValue(data.resource_deficit);
    if (hasDeficit) return "WARN";
    if (actions.includes("scale_up") || builderActions.includes("scale_up_builder")) return "INFO";
    return "INFO";
  }
  if (event.kind === "node_heartbeat") {
    const actual = data.actual_usage || {};
    const load = data.load || {};
    if (asNumber(actual.memory_percent) >= 85 || asNumber(load.vcpu) >= 0.9) return "WARN";
    return "INFO";
  }
  return "INFO";
}

function titleForEvent(event) {
  if (event.kind === "autoscaler_cycle") return "Autoscaler cycle";
  if (event.kind === "sandbox_scheduled") return "Sandbox scheduled";
  if (event.kind === "sandbox_pending_deleted") return "Sandbox pending deleted";
  if (event.kind === "node_heartbeat") return "Node heartbeat";
  return event.kind || "Event";
}

function summarizeEvent(event) {
  const data = event.data || {};
  if (event.kind === "autoscaler_cycle") {
    const actions = Array.isArray(data.actions) ? data.actions : [];
    const builderActions = Array.isArray(data.builder_actions) ? data.builder_actions : [];
    const created = Array.isArray(data.created_job_ids) ? data.created_job_ids.length : 0;
    const stopped = Array.isArray(data.stop_job_ids) ? data.stop_job_ids.length : 0;
    const pending = data.pending_resources || {};
    const prepared = data.prepared_resources || {};
    const actionText = actions.concat(builderActions).join(", ") || "none";
    return `ready ${asNumber(data.ready_nodes)}, provisioning ${asNumber(data.provisioning_nodes)}, created ${created}, stopped ${stopped}, pending ${formatResources(pending)}, prepared ${formatResources(prepared)}, actions ${actionText}`;
  }
  if (event.kind === "sandbox_scheduled") {
    return `${data.sandbox_id || "sandbox"} on ${data.node_id || data.job_id || "node"}, wait ${formatDurationMs(data.scale_up_wait_ms)}`;
  }
  if (event.kind === "sandbox_pending_deleted") {
    return `${data.sandbox_id || "sandbox"} deleted while pending after ${formatDurationMs(data.pending_age_ms)}`;
  }
  if (event.kind === "node_heartbeat") {
    const actual = data.actual_usage || {};
    const load = data.load || {};
    return `${data.node_id || data.job_id || "node"} active ${asNumber(data.active_sandboxes)}, CPU ${formatPercentPoint(ratioToPercent(load.vcpu))} reserved, actual ${formatPercentPoint(actual.cpu_percent)}`;
  }
  return JSON.stringify(data).slice(0, 240);
}

function redrawCharts() {
  if (state.history.length === 0) {
    for (const id of [
      "activeNodesChart",
      "activeSandboxesChart",
      "queueDepthChart",
      "cpuPressureChart",
      "memoryPressureChart",
      "scaleLatencyChart",
      "sandboxStartChart",
      "builderBuildsChart",
      "nodesSpark",
      "sandboxesSpark",
      "cpuSpark",
      "memorySpark",
      "queueSpark",
      "errorSpark",
    ]) {
      clearPlot(id, "Waiting for metrics");
    }
    return;
  }

  drawSpark("nodesSpark", state.history.map((p) => p.activeNodes), palette.blue, palette.blueSoft);
  drawSpark("sandboxesSpark", state.history.map((p) => p.activeSandboxes), palette.blue, palette.blueSoft);
  drawSpark("cpuSpark", state.history.map((p) => p.cpuUtilization), palette.green, palette.greenSoft, { min: 0, max: 100 });
  drawSpark("memorySpark", state.history.map((p) => p.memoryUtilization), palette.orange, palette.orangeSoft, { min: 0, max: 100 });
  drawSpark("queueSpark", state.history.map((p) => p.queueDepth), palette.purple, palette.purpleSoft);
  drawSpark("errorSpark", state.history.map((p) => p.errorRate), palette.red, palette.redSoft, { min: 0 });

  drawLineChart("activeNodesChart", [
    { label: "Active Nodes", color: palette.blue, fill: palette.blueSoft, values: state.history.map((p) => p.activeNodes) },
  ], { min: 0, ticks: 4 });
  drawLineChart("activeSandboxesChart", [
    { label: "Running Sandboxes", color: palette.blue, fill: palette.blueSoft, values: state.history.map((p) => p.activeSandboxes) },
  ], { min: 0, ticks: 4, integerAxis: true });
  drawLineChart("queueDepthChart", [
    { label: "Queue Depth", color: palette.purple, fill: palette.purpleSoft, values: state.history.map((p) => p.queueDepth) },
  ], { min: 0, ticks: 4, integerAxis: true });
  drawLineChart("cpuPressureChart", [
    { label: "Actual", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.cpuUtilization) },
    { label: "Reserved", color: palette.blue, dashed: true, values: state.history.map((p) => p.cpuReserved) },
  ], { min: 0, max: 100, ticks: 4, suffix: "%" });
  drawLineChart("memoryPressureChart", [
    { label: "Actual", color: palette.orange, fill: palette.orangeSoft, values: state.history.map((p) => p.memoryUtilization) },
    { label: "Reserved", color: palette.blue, dashed: true, values: state.history.map((p) => p.memoryReserved) },
  ], { min: 0, max: 100, ticks: 4, suffix: "%" });
  drawLineChart("scaleLatencyChart", [
    { label: "p50", color: palette.blue, values: state.history.map((p) => p.scaleP50Seconds) },
    { label: "p95", color: palette.blue, dashed: true, values: state.history.map((p) => p.scaleP95Seconds) },
  ], { min: 0, ticks: 4 });
  drawLineChart("sandboxStartChart", [
    { label: "Start Time", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.startP50Seconds) },
  ], { min: 0, ticks: 4 });
  drawLineChart("builderBuildsChart", [
    { label: "Active Builds", color: palette.orange, fill: palette.orangeSoft, values: state.history.map((p) => p.activeBuilds) },
    { label: "Ready Builders", color: palette.blue, dashed: true, values: state.history.map((p) => p.builderNodes) },
  ], { min: 0, ticks: 4, integerAxis: true });
}

function clearPlot(id, label) {
  const prepared = prepareCanvas(document.getElementById(id));
  const ctx = prepared.ctx;
  ctx.fillStyle = palette.plotBg;
  ctx.fillRect(0, 0, prepared.width, prepared.height);
  ctx.fillStyle = palette.muted;
  ctx.font = "13px system-ui, sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(label, prepared.width / 2, prepared.height / 2);
}

function drawSpark(id, values, color, fill, options = {}) {
  const prepared = prepareCanvas(document.getElementById(id));
  const ctx = prepared.ctx;
  const width = prepared.width;
  const height = prepared.height;
  ctx.clearRect(0, 0, width, height);
  const numeric = values.filter((value) => value !== null && Number.isFinite(value));
  if (numeric.length === 0) return;
  const min = options.min ?? Math.min(...numeric, 0);
  const rawMax = options.max ?? Math.max(...numeric);
  const max = rawMax <= min ? min + 1 : rawMax;
  const points = valuesToPoints(values, width, height, { left: 0, right: 0, top: 6, bottom: 4 }, min, max);
  fillUnderLine(ctx, points, height - 4, fill);
  strokeLine(ctx, points, color, false, 2);
}

function drawLineChart(id, series, options = {}) {
  const prepared = prepareCanvas(document.getElementById(id));
  const ctx = prepared.ctx;
  const width = prepared.width;
  const height = prepared.height;
  const pad = { left: 42, right: 14, top: 10, bottom: 28 };
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = palette.plotBg;
  ctx.fillRect(0, 0, width, height);

  const allValues = series.flatMap((line) => line.values).filter((value) => value !== null && Number.isFinite(value));
  if (allValues.length === 0) {
    drawEmptyPlot(ctx, width, height, "No numeric samples");
    return;
  }

  const min = options.min ?? Math.min(...allValues);
  const rawMax = options.max ?? Math.max(...allValues);
  const max = rawMax <= min ? min + 1 : rawMax * 1.08;
  drawGrid(ctx, width, height, pad, min, max, options);

  for (const line of series) {
    const points = valuesToPoints(line.values, width, height, pad, min, max);
    if (line.fill) {
      fillUnderLine(ctx, points, height - pad.bottom, line.fill);
    }
    strokeLine(ctx, points, line.color, Boolean(line.dashed), 2);
  }
}

function valuesToPoints(values, width, height, pad, min, max) {
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const count = Math.max(1, values.length - 1);
  return values.map((value, index) => {
    if (value === null || !Number.isFinite(value)) return null;
    const x = pad.left + (plotWidth * index) / count;
    const y = pad.top + plotHeight - ((value - min) / (max - min)) * plotHeight;
    return { x, y };
  });
}

function fillUnderLine(ctx, points, bottom, fillStyle) {
  const valid = points.filter(Boolean);
  if (valid.length < 2) return;
  ctx.beginPath();
  ctx.moveTo(valid[0].x, bottom);
  for (const point of valid) {
    ctx.lineTo(point.x, point.y);
  }
  ctx.lineTo(valid[valid.length - 1].x, bottom);
  ctx.closePath();
  ctx.fillStyle = fillStyle;
  ctx.fill();
}

function strokeLine(ctx, points, color, dashed, width) {
  ctx.save();
  ctx.beginPath();
  ctx.strokeStyle = color;
  ctx.lineWidth = width;
  ctx.lineJoin = "round";
  ctx.lineCap = "round";
  if (dashed) {
    ctx.setLineDash([6, 6]);
  }
  let started = false;
  for (const point of points) {
    if (!point) {
      started = false;
      continue;
    }
    if (!started) {
      ctx.moveTo(point.x, point.y);
      started = true;
    } else {
      ctx.lineTo(point.x, point.y);
    }
  }
  ctx.stroke();
  ctx.restore();
}

function drawGrid(ctx, width, height, pad, min, max, options) {
  const left = pad.left;
  const right = width - pad.right;
  const top = pad.top;
  const bottom = height - pad.bottom;
  const ticks = options.ticks || 4;
  ctx.save();
  ctx.strokeStyle = palette.grid;
  ctx.lineWidth = 1;
  ctx.setLineDash([4, 4]);
  ctx.fillStyle = palette.muted;
  ctx.font = "12px system-ui, sans-serif";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let index = 0; index <= ticks; index += 1) {
    const y = top + ((bottom - top) * index) / ticks;
    ctx.beginPath();
    ctx.moveTo(left, y);
    ctx.lineTo(right, y);
    ctx.stroke();
    const value = max - ((max - min) * index) / ticks;
    ctx.fillText(formatAxisValue(value, options), left - 8, y);
  }
  ctx.restore();
  ctx.fillStyle = palette.muted;
  ctx.font = "12px system-ui, sans-serif";
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
  ctx.font = "13px system-ui, sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(label, width / 2, height / 2);
}

function prepareCanvas(canvas) {
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = Math.max(80, Math.round(rect.width));
  const cssHeight = Math.max(40, Math.round(rect.height));
  const nextWidth = Math.round(cssWidth * dpr);
  const nextHeight = Math.round(cssHeight * dpr);
  if (canvas.width !== nextWidth || canvas.height !== nextHeight) {
    canvas.width = nextWidth;
    canvas.height = nextHeight;
  }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { ctx, width: cssWidth, height: cssHeight };
}

function eventErrorRate(events) {
  if (!Array.isArray(events) || events.length === 0) return 0;
  const bad = events.filter((event) => severityForEvent(event) === "ALERT").length;
  return (bad / events.length) * 100;
}

function resourceHasPositiveValue(value) {
  if (!value || typeof value !== "object") return false;
  return Object.values(value).some((item) => Number(item) > 0);
}

function setText(id, value) {
  els[id].textContent = value;
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

function firstNumber(...values) {
  for (const value of values) {
    const number = nullableNumber(value);
    if (number !== null) return number;
  }
  return null;
}

function ratioToPercent(value) {
  const number = nullableNumber(value);
  return number === null ? null : number * 100;
}

function msToSeconds(value) {
  const number = nullableNumber(value);
  return number === null ? null : number / 1000;
}

function average(values) {
  const numeric = values.filter((value) => value !== null && Number.isFinite(value));
  if (numeric.length === 0) return null;
  return numeric.reduce((total, value) => total + value, 0) / numeric.length;
}

function formatInteger(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(number);
}

function formatNumber(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(number);
}

function formatAxisValue(value, options) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  if (options.suffix === "%") return `${Math.round(number)}%`;
  if (options.integerAxis || Math.abs(number) >= 100) return formatInteger(number);
  if (Math.abs(number) >= 10) return number.toFixed(1);
  return number.toFixed(2).replace(/\\.00$/, "");
}

function formatPercentPoint(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return `${Math.round(number)}%`;
}

function formatPercentDecimal(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  return `${number.toFixed(2)}%`;
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

function formatResources(value) {
  if (!value || typeof value !== "object") return "0 vCPU";
  const cpu = formatNumber(value.vcpu || 0);
  const memory = formatMemory(value.memory_mb || 0);
  return `${cpu} vCPU ${memory}`;
}

function formatMemory(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  if (number >= 1024) return `${formatNumber(number / 1024)} GiB`;
  return `${formatNumber(number)} MiB`;
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
