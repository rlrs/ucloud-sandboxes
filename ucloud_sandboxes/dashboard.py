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
  <a class="skip-link" href="#pageHeading">Skip to dashboard content</a>
  <header class="app-bar">
    <div class="brand">
      <span class="brand-mark" aria-hidden="true"><span></span><span></span><span></span><span></span></span>
      <span class="brand-copy">
        <strong>UCloud Sandboxes</strong>
      </span>
    </div>
    <div class="app-context">
      <span id="pageKicker" class="visually-hidden">Control plane</span>
      <h1 id="pageHeading" tabindex="-1">Overview</h1>
      <p id="pageDescription" class="visually-hidden">Overview</p>
    </div>
    <div class="top-controls" aria-label="Dashboard controls">
      <span id="connectionStatus" class="status-pill status-warn" aria-live="polite">Waiting</span>
      <span id="lastUpdated" class="last-updated">Not refreshed</span>
      <button id="authToggleButton" type="button" aria-expanded="false" aria-controls="authPanel">Bearer token</button>
      <label class="select-control">
        <span class="clock-mark" aria-hidden="true"></span>
        <span class="visually-hidden">Session chart range</span>
        <select id="timeRangeSelect">
          <option value="900000">Session 15m</option>
          <option value="3600000" selected>Session 1h</option>
          <option value="21600000">Session 6h</option>
        </select>
      </label>
      <button id="refreshNowButton" class="icon-button" type="button" title="Refresh now" aria-label="Refresh now">
        <span class="refresh-mark" aria-hidden="true"></span>
      </button>
      <button id="themeButton" class="icon-button" type="button" title="Toggle dark charts" aria-label="Toggle dark charts">
        <span class="moon-mark" aria-hidden="true"></span>
      </button>
    </div>
  </header>

  <main class="page-shell">
    <section id="authPanel" class="auth-panel" aria-label="Metrics authentication" hidden>
      <label class="token-field">
        <span>Gateway bearer token</span>
        <input id="tokenInput" type="password" autocomplete="off" spellcheck="false" placeholder="Required for /v1/metrics">
      </label>
      <button id="saveTokenButton" type="button">Save</button>
      <button id="clearTokenButton" type="button">Clear</button>
    </section>

    <nav class="page-tabs" role="tablist" aria-label="Dashboard pages" aria-orientation="horizontal">
      <span class="nav-section-label" role="presentation">Monitor</span>
      <button id="overviewTab" class="page-tab is-active" role="tab" aria-selected="true" aria-controls="overviewPage" type="button" data-page-target="overview">
        <span class="nav-label"><i class="nav-icon nav-icon-overview" aria-hidden="true"></i>Overview</span><span id="overviewNavBadge" class="nav-badge">Live</span>
      </button>
      <button id="schedulerTab" class="page-tab" role="tab" aria-selected="false" aria-controls="schedulerPage" type="button" data-page-target="scheduler">
        <span class="nav-label"><i class="nav-icon nav-icon-scheduler" aria-hidden="true"></i>Demand &amp; scaling</span><span id="schedulerNavBadge" class="nav-badge">0</span>
      </button>
      <button id="nodesTab" class="page-tab" role="tab" aria-selected="false" aria-controls="nodesPage" type="button" data-page-target="nodes">
        <span class="nav-label"><i class="nav-icon nav-icon-nodes" aria-hidden="true"></i>Fleet</span><span id="nodesNavBadge" class="nav-badge">0</span>
      </button>
      <span class="nav-section-label nav-section-manage" role="presentation">Manage</span>
      <button id="sandboxesTab" class="page-tab" role="tab" aria-selected="false" aria-controls="sandboxesPage" type="button" data-page-target="sandboxes">
        <span class="nav-label"><i class="nav-icon nav-icon-sandboxes" aria-hidden="true"></i>Sandboxes</span><span id="sandboxesNavBadge" class="nav-badge">0</span>
      </button>
      <button id="registryTab" class="page-tab" role="tab" aria-selected="false" aria-controls="registryPage" type="button" data-page-target="registry">
        <span class="nav-label"><i class="nav-icon nav-icon-registry" aria-hidden="true"></i>Images</span><span id="registryNavBadge" class="nav-badge">0</span>
      </button>
    </nav>

    <section id="overviewPage" class="overview-page" role="tabpanel" aria-labelledby="overviewTab">
      <section class="command-grid overview-section" aria-label="Current operational posture">
        <article class="health-strip">
          <div class="health-primary">
            <span id="healthBadge" class="health-icon health-neutral" aria-hidden="true"></span>
            <div>
              <span class="eyebrow">Status</span>
              <strong id="healthTitle">No data</strong>
              <span id="healthDetail">Metrics unavailable</span>
            </div>
          </div>
          <div id="healthSignals" class="health-signals" aria-live="polite"></div>
          <div class="health-actions">
            <button id="copyDiagnosticsButton" class="table-action" type="button">Copy summary</button>
            <button id="downloadSnapshotButton" class="table-action" type="button">Download snapshot</button>
          </div>
        </article>

        <article class="decision-brief">
          <div class="panel-header">
            <div>
              <span class="eyebrow">Autoscaler</span>
              <h2 id="overviewDecisionTitle">No decision</h2>
            </div>
            <span id="overviewDecisionBadge" class="inline-badge badge-muted">No cycle</span>
          </div>
          <p id="autoscalerSummary" class="decision-summary">No cycle</p>
          <div id="overviewDecisionReasons" class="decision-reasons"></div>
          <div class="decision-facts">
            <div><span>Ready / booting / unreachable</span><strong id="overviewSupplyValue">-</strong></div>
            <div><span>Projected free</span><strong id="overviewProjectedValue">-</strong></div>
            <div><span>Deficit</span><strong id="overviewDeficitValue">-</strong></div>
          </div>
        </article>
      </section>

      <section class="section-heading overview-section overview-heading">
        <div>
          <span class="eyebrow">Current</span>
          <h2>Workload</h2>
        </div>
      </section>

      <section class="metric-grid overview-section" aria-label="Current demand and capacity">
        <article class="metric-card">
          <span class="metric-label">Ready for a tool call</span>
          <strong id="readyWakeValue">-</strong>
          <span id="readyWakeDetail" class="metric-detail">Oldest -</span>
        </article>
        <article class="metric-card">
          <span class="metric-label">Ready capacity</span>
          <strong id="activeNodesValue">-</strong>
          <span id="activeNodesDetail" class="metric-detail">Ready / provisioning / total</span>
        </article>
        <article class="metric-card">
          <span class="metric-label">Sandbox fleet</span>
          <strong id="runningSandboxesValue">-</strong>
          <span id="runningSandboxesDetail" class="metric-detail">Running / parked / waking</span>
        </article>
        <article class="metric-card">
          <span class="metric-label">Hard disk committed</span>
          <strong id="diskCommitValue">-</strong>
          <span id="diskCommitDetail" class="metric-detail">Free -</span>
        </article>
        <article class="metric-card">
          <span class="metric-label">Waiting on the model</span>
          <strong id="modelWaitValue">-</strong>
          <span id="modelWaitDetail" class="metric-detail">Oldest -</span>
        </article>
        <article class="metric-card">
          <span class="metric-label">Response → ready p95</span>
          <strong id="wakeLatencyValue">-</strong>
          <span id="wakeLatencyDetail" class="metric-detail">Session p95</span>
        </article>
      </section>

      <section class="overview-workbench overview-section" aria-label="Capacity and request flow">
        <article class="capacity-card">
          <div class="section-heading compact">
            <div>
              <span class="eyebrow">Placement headroom</span>
              <h2>Resource fit</h2>
            </div>
            <span id="capacityFitBadge" class="inline-badge badge-muted">Waiting</span>
          </div>
          <p id="capacitySummary" class="workspace-copy">No data</p>
          <div class="headroom-list">
            <div class="headroom-row">
              <div class="headroom-label"><span>CPU</span><strong id="capacityCpuValue">-</strong></div>
              <div class="dual-meter" aria-label="CPU actual and reserved utilization">
                <span id="capacityCpuActualMeter" class="meter-actual"></span>
                <span id="capacityCpuReservedMeter" class="meter-reserved"></span>
              </div>
              <span id="capacityCpuDetail" class="headroom-detail">actual / reserved</span>
            </div>
            <div class="headroom-row">
              <div class="headroom-label"><span>Memory</span><strong id="capacityMemoryValue">-</strong></div>
              <div class="dual-meter" aria-label="Memory actual and reserved utilization">
                <span id="capacityMemoryActualMeter" class="meter-actual"></span>
                <span id="capacityMemoryReservedMeter" class="meter-reserved"></span>
              </div>
              <span id="capacityMemoryDetail" class="headroom-detail">actual / reserved</span>
            </div>
            <div class="headroom-row hard-limit">
              <div class="headroom-label"><span>Hard disk</span><strong id="capacityDiskValue">-</strong></div>
              <div class="dual-meter single" aria-label="Hard disk committed utilization">
                <span id="capacityDiskMeter" class="meter-disk"></span>
              </div>
              <span id="capacityDiskDetail" class="headroom-detail">committed / total</span>
            </div>
          </div>
        </article>

        <article class="pipeline-card">
          <div class="section-heading compact">
            <div>
              <span class="eyebrow">Agent lifecycle</span>
              <h2>Request flow</h2>
            </div>
            <span id="programSummary" class="section-summary">No requests loaded</span>
          </div>
          <div class="overview-pipeline">
            <div class="pipeline-stage forecast">
              <span>Model generation</span>
              <strong id="programModelWaitValue">-</strong>
              <small id="overviewModelWaitAge">No active wait</small>
            </div>
            <span class="pipeline-connector" aria-hidden="true"></span>
            <div class="pipeline-stage attention">
              <span>Ready for tool</span>
              <strong id="programReadyValue">-</strong>
              <small id="programOldestReadyValue">No ready work</small>
            </div>
            <span class="pipeline-connector" aria-hidden="true"></span>
            <div class="pipeline-stage">
              <span>Restoring</span>
              <strong id="overviewWakingValue">-</strong>
              <small>Restore</small>
            </div>
            <span class="pipeline-connector" aria-hidden="true"></span>
            <div class="pipeline-stage success">
              <span>Tool executing</span>
              <strong id="overviewActingValue">-</strong>
              <small>Active</small>
            </div>
          </div>
          <div class="latency-strip">
            <div><span>Model wait p95</span><strong id="overviewModelLatency">-</strong></div>
            <div><span>Response → ready p95</span><strong id="programWakeLatencyValue">-</strong></div>
            <div><span>Node provisioning p95</span><strong id="autoscalerProvisioningValue">-</strong></div>
            <div><span>Idle grace</span><strong id="autoscalerIdleGraceValue">-</strong></div>
          </div>
        </article>
      </section>

      <section class="section-heading overview-section overview-heading">
        <div>
          <span class="eyebrow">Session</span>
          <h2>Trends</h2>
        </div>
      </section>

      <section class="trend-grid overview-section" aria-label="Operational trends">
        <article class="chart-panel trend-supply">
          <div class="panel-header">
            <div><h2>Fleet</h2></div>
          </div>
          <canvas id="activeNodesChart" class="chart-canvas" width="760" height="230"></canvas>
          <div class="legend"><span><i class="swatch blue"></i>Ready nodes</span><span><i class="swatch green"></i>Sandbox routes</span></div>
        </article>
        <article class="chart-panel trend-demand">
          <div class="panel-header">
            <div><h2>Queues</h2></div>
          </div>
          <canvas id="queueDepthChart" class="chart-canvas" width="760" height="230"></canvas>
          <div class="legend"><span><i class="swatch green"></i>Ready to wake</span><span><i class="swatch purple"></i>Sandbox creates</span><span><i class="swatch orange"></i>Image builds</span></div>
        </article>
        <article class="chart-panel trend-pressure">
          <div class="panel-header"><div><h2>CPU</h2></div></div>
          <canvas id="cpuPressureChart" class="chart-canvas small" width="560" height="190"></canvas>
          <div class="legend"><span><i class="swatch green"></i>Actual</span><span><i class="swatch blue-dash"></i>Reserved</span></div>
        </article>
        <article class="chart-panel trend-pressure">
          <div class="panel-header"><div><h2>Memory</h2></div></div>
          <canvas id="memoryPressureChart" class="chart-canvas small" width="560" height="190"></canvas>
          <div class="legend"><span><i class="swatch orange"></i>Actual</span><span><i class="swatch blue-dash"></i>Reserved</span></div>
        </article>
        <article class="chart-panel trend-latency">
          <div class="panel-header">
            <div><h2>Latency</h2></div>
          </div>
          <canvas id="sandboxStartChart" class="chart-canvas small" width="760" height="190"></canvas>
          <div class="legend"><span><i class="swatch purple"></i>Model wait p95</span><span><i class="swatch green"></i>Response → ready p95</span></div>
        </article>
      </section>

      <section class="section-heading overview-section overview-heading">
        <div><span class="eyebrow">Recent</span><h2>Activity</h2></div>
      </section>
      <section class="activity-grid overview-section" aria-label="Recent operational activity">
        <section class="event-panel activity-event-panel" aria-label="Recent autoscaler events">
          <div class="panel-header table-header"><h2>Autoscaler</h2><span id="eventSummary">No events</span></div>
          <div class="table-wrap"><table><thead><tr><th>Time</th><th>Severity</th><th>Event</th><th>Details</th></tr></thead><tbody id="eventRows"><tr><td colspan="4" class="empty-cell">No metrics loaded</td></tr></tbody></table></div>
        </section>
        <details class="event-panel diagnostic-disclosure build-panel">
          <summary><span>Builds</span><span id="buildSummary">No builds</span></summary>
          <div class="table-wrap"><table><thead><tr><th>Status</th><th>Image</th><th>Tag</th><th>Location</th><th>Age</th><th>Details</th></tr></thead><tbody id="buildRows"><tr><td colspan="6" class="empty-cell">No builds loaded</td></tr></tbody></table></div>
        </details>
        <details class="event-panel diagnostic-disclosure trace-panel">
          <summary><span>Traces</span><span id="traceSummary">No traces</span></summary>
          <div class="table-wrap"><table><thead><tr><th>Time</th><th>Status</th><th>Trace</th><th>Duration</th><th>Details</th></tr></thead><tbody id="traceRows"><tr><td colspan="5" class="empty-cell">No traces loaded</td></tr></tbody></table></div>
        </details>
      </section>
    </section>

    <section id="schedulerPage" class="workspace-page" role="tabpanel" aria-labelledby="schedulerTab" hidden>
      <section class="decision-hero">
        <div class="decision-copy">
          <div class="eyebrow">Autoscaler</div>
          <div class="decision-title-row">
            <h2 id="schedulerDecisionTitle">Waiting for a decision</h2>
            <span id="schedulerModeBadge" class="inline-badge badge-muted">Unknown</span>
          </div>
          <p id="schedulerDecisionDetail" class="workspace-copy">No data</p>
          <div id="schedulerReasons" class="reason-list"></div>
        </div>
        <div class="decision-stats">
          <div class="stat-box">
            <span>Ready nodes</span>
            <strong id="schedulerReadyNodesValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Provisioning</span>
            <strong id="schedulerProvisioningValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Wake plan</span>
            <strong id="schedulerWakePlanValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Unplaced</span>
            <strong id="schedulerUnplacedValue">-</strong>
          </div>
        </div>
      </section>

      <section class="flow-panel" aria-label="Program lifecycle">
        <div class="section-heading">
          <div>
            <div class="eyebrow">Requests</div>
            <h2>Flow</h2>
          </div>
          <span id="programFlowSummary" class="section-summary">No program requests loaded</span>
        </div>
        <div class="program-flow">
          <button class="flow-stage is-selected" type="button" data-program-state="all" aria-pressed="true">
            <span class="flow-index">All</span>
            <strong id="flowAllValue">-</strong>
            <small>active requests</small>
          </button>
          <span class="flow-arrow" aria-hidden="true">→</span>
          <button class="flow-stage" type="button" data-program-state="model_wait" aria-pressed="false">
            <span class="flow-index">Model wait</span>
            <strong id="flowModelWaitValue">-</strong>
            <small id="flowModelWaitDetail">waiting on model</small>
          </button>
          <span class="flow-arrow" aria-hidden="true">→</span>
          <button class="flow-stage" type="button" data-program-state="ready_to_wake" aria-pressed="false">
            <span class="flow-index">Ready</span>
            <strong id="flowReadyValue">-</strong>
            <small id="flowReadyDetail">ready to wake</small>
          </button>
          <span class="flow-arrow" aria-hidden="true">→</span>
          <button class="flow-stage" type="button" data-program-state="waking" aria-pressed="false">
            <span class="flow-index">Waking</span>
            <strong id="flowWakingValue">-</strong>
            <small>waking</small>
          </button>
          <span class="flow-arrow" aria-hidden="true">→</span>
          <button class="flow-stage" type="button" data-program-state="acting" aria-pressed="false">
            <span class="flow-index">Acting</span>
            <strong id="flowActingValue">-</strong>
            <small>acting</small>
          </button>
        </div>
      </section>

      <section class="scheduler-grid">
        <article class="workspace-card capacity-equation-card">
          <div class="section-heading compact">
            <div>
              <div class="eyebrow">Placement</div>
              <h2>Capacity equation</h2>
            </div>
            <span id="decisionPressureValue" class="section-summary">No pressure samples</span>
          </div>
          <div class="table-wrap capacity-equation-wrap">
            <table class="capacity-equation-table">
              <thead><tr><th>Stage</th><th>vCPU</th><th>Memory</th><th>Hard disk</th></tr></thead>
              <tbody>
                <tr><th>Immediate demand</th><td id="equationImmediateCpu">-</td><td id="equationImmediateMemory">-</td><td id="equationImmediateDisk">-</td></tr>
                <tr><th>Response-ready demand</th><td id="equationReadyCpu">-</td><td id="equationReadyMemory">-</td><td id="equationReadyDisk">-</td></tr>
                <tr><th>Predictive model demand</th><td id="equationPredictiveCpu">-</td><td id="equationPredictiveMemory">-</td><td id="equationPredictiveDisk">-</td></tr>
                <tr class="supply-row"><th>Already prepared</th><td id="equationPreparedCpu">-</td><td id="equationPreparedMemory">-</td><td id="equationPreparedDisk">-</td></tr>
                <tr class="supply-row"><th>Free after commitments</th><td id="equationFreeCpu">-</td><td id="equationFreeMemory">-</td><td id="equationFreeDisk">-</td></tr>
                <tr class="deficit-row"><th>Uncovered demand</th><td id="equationDeficitCpu">-</td><td id="equationDeficitMemory">-</td><td id="equationDeficitDisk">-</td></tr>
              </tbody>
            </table>
          </div>
          <div class="equation-footnote"><span id="decisionIdleGraceValue">Idle grace -</span></div>
        </article>
        <details class="workspace-card policy-card" open>
          <summary class="section-heading compact">
            <div>
              <div class="eyebrow">Policy</div>
              <h2>Current tuning</h2>
            </div>
            <span class="read-only-badge">Read only</span>
          </summary>
          <dl id="policyValues" class="policy-values">
            <div><dt>Program action</dt><dd>-</dd></div>
          </dl>
        </details>
      </section>

      <section class="event-panel queue-panel" aria-label="Shadow wake queue">
        <div class="queue-toolbar">
          <div>
            <div class="eyebrow">Queue</div>
            <h2>Wake placement</h2>
          </div>
          <label class="inline-search">
            <span class="visually-hidden">Search wake queue</span>
            <input id="programSearchInput" type="search" autocomplete="off" spellcheck="false" placeholder="Rollout, request, sandbox, or node">
          </label>
          <label class="inline-select">
            <span class="visually-hidden">Wake result filter</span>
            <select id="programResultFilter">
              <option value="all">All results</option>
              <option value="unplaced">Unplaced first</option>
              <option value="local">Local wakes</option>
              <option value="migration">Migrations</option>
            </select>
          </label>
          <span id="programQueueSummary" class="section-summary">No queue loaded</span>
        </div>
        <div class="table-wrap">
          <table class="program-table">
            <thead>
              <tr>
                <th>Position</th>
                <th>Age</th>
                <th>Rollout / Request</th>
                <th>Sandbox</th>
                <th>Requested</th>
                <th>Planned node</th>
                <th>Path</th>
                <th>Result</th>
              </tr>
            </thead>
            <tbody id="programQueueRows">
              <tr><td colspan="8" class="empty-cell">No wake queue loaded</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </section>

    <section id="nodesPage" class="workspace-page" role="tabpanel" aria-labelledby="nodesTab" hidden>
      <section class="workspace-hero">
        <div>
          <div class="eyebrow">Fleet</div>
          <h2>Placement supply</h2>
          <p id="nodesPageDetail" class="workspace-copy">No heartbeats</p>
        </div>
        <div class="node-hero-stats">
          <div class="stat-box"><span>Schedulable</span><strong id="nodesReadyValue">-</strong></div>
          <div class="stat-box"><span>Provisioning</span><strong id="nodesProvisioningValue">-</strong></div>
          <div class="stat-box"><span>Draining</span><strong id="nodesDrainingValue">-</strong></div>
          <div class="stat-box"><span>Stale / incompatible</span><strong id="nodesIncompatibleValue">-</strong></div>
          <div class="stat-box"><span>Hard disk free</span><strong id="nodesDiskFreeValue">-</strong></div>
        </div>
      </section>
      <section class="fleet-signal-grid" aria-label="Fleet pressure summary">
        <article><span>CPU actual / reserved</span><strong id="nodesCpuPressureValue">-</strong><small id="nodesCpuPressureDetail">No fresh samples</small></article>
        <article><span>Memory actual / reserved</span><strong id="nodesMemoryPressureValue">-</strong><small id="nodesMemoryPressureDetail">No fresh samples</small></article>
        <article><span>Memory PSI full</span><strong id="nodesPsiValue">-</strong><small>10-second average</small></article>
        <article><span>Storage queue</span><strong id="nodesStorageQueueValue">-</strong><small id="nodesStorageQueueDetail">active / waiting / limit</small></article>
        <article><span>Volume errors</span><strong id="nodesVolumeErrorsValue">-</strong><small>storage-native volumes</small></article>
      </section>
      <section class="queue-toolbar node-toolbar" aria-label="Node filters">
        <label class="inline-search">
          <span class="visually-hidden">Search nodes</span>
          <input id="nodeSearchInput" type="search" autocomplete="off" spellcheck="false" placeholder="Node or job id">
        </label>
        <label class="inline-select">
          <span class="visually-hidden">Node state filter</span>
          <select id="nodeStateFilter">
            <option value="all">All nodes</option>
            <option value="ready">Ready</option>
            <option value="constrained">Constrained</option>
            <option value="draining">Draining / closed</option>
            <option value="stale">Stale / incompatible</option>
          </select>
        </label>
        <span id="nodeTableSummary" class="section-summary">No nodes loaded</span>
      </section>
      <section class="event-panel">
        <div class="table-wrap">
          <table class="node-table">
            <thead>
              <tr>
                <th>State</th>
                <th>Node</th>
                <th>Work</th>
                <th>CPU actual / reserved</th>
                <th>Memory actual / reserved</th>
                <th>Hard disk free</th>
                <th>Pressure</th>
                <th>Heartbeat</th>
              </tr>
            </thead>
            <tbody id="nodeRows">
              <tr><td colspan="8" class="empty-cell">No node metrics loaded</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </section>

    <section id="sandboxesPage" class="sandboxes-page" role="tabpanel" aria-labelledby="sandboxesTab" hidden>
      <section class="sandbox-hero">
        <div class="sandbox-hero-main">
          <div class="panel-header">
            <h2>Lifecycle</h2>
            <span id="sandboxesPageStatusBadge" class="inline-badge badge-muted">Not loaded</span>
          </div>
          <p id="sandboxesPageDetail" class="registry-copy">No data</p>
        </div>
        <div class="sandbox-stat-grid">
          <div class="stat-box">
            <span>Loaded</span>
            <strong id="sandboxesPageRowsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Running</span>
            <strong id="sandboxesPageTerminableValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Parked</span>
            <strong id="sandboxesPagePendingValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Waking / moving</span>
            <strong id="sandboxesPageRoutesValue">-</strong>
          </div>
        </div>
      </section>

      <section class="sandbox-toolbar" aria-label="Sandbox controls">
        <label class="sandbox-search">
          <span>Search sandboxes</span>
          <input id="sandboxSearchInput" type="search" autocomplete="off" spellcheck="false" placeholder="Sandbox id, image, node, label">
        </label>
        <label class="sandbox-filter">
          <span>Lifecycle state</span>
          <select id="sandboxStateFilter">
            <option value="all">All states</option>
            <option value="attention">Needs attention</option>
            <option value="running">Running</option>
            <option value="parked">Parked</option>
            <option value="transitioning">Parking / waking / migrating</option>
            <option value="pending">Pending / creating</option>
            <option value="failed">Failed / stale</option>
          </select>
        </label>
        <button id="refreshSandboxesButton" class="table-action" type="button">Refresh</button>
        <div id="sandboxesPageSummary" class="registry-copy">No sandboxes loaded</div>
      </section>

      <section class="event-panel sandbox-list-panel" aria-label="Latest sandboxes">
        <div class="panel-header table-header">
          <h2>Latest Sandboxes</h2>
          <span id="sandboxListSummary">No sandboxes loaded</span>
        </div>
        <div class="table-wrap">
          <table class="sandbox-table">
            <thead>
              <tr>
                <th>State</th>
                <th>Sandbox</th>
                <th>Image</th>
                <th>Node</th>
                <th>Resources</th>
                <th>Age</th>
                <th>Labels</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="sandboxRows">
              <tr><td colspan="8" class="empty-cell">No sandboxes loaded</td></tr>
            </tbody>
          </table>
        </div>
      </section>
    </section>

    <section id="registryPage" class="registry-page" role="tabpanel" aria-labelledby="registryTab" hidden>
      <section class="registry-hero">
        <div class="registry-hero-main">
          <div class="panel-header">
            <h2>Registry</h2>
            <span id="registryPageStatusBadge" class="inline-badge badge-muted">Unknown</span>
          </div>
          <div id="registryPageUrl" class="registry-url registry-url-large">No registry configured</div>
          <p id="registryPageHealthDetail" class="registry-copy">No data</p>
        </div>
        <div class="registry-stat-grid">
          <div class="stat-box">
            <span>Repositories</span>
            <strong id="registryPageReposValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Scanned tags</span>
            <strong id="registryPageTagsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Active builds</span>
            <strong id="registryActiveBuildsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Failed builds</span>
            <strong id="registryFailedBuildsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Visible tags</span>
            <strong id="registryPageVisibleTagsValue">-</strong>
          </div>
          <div class="stat-box">
            <span>Coverage</span>
            <strong id="registryPageCoverageValue">-</strong>
          </div>
        </div>
      </section>

      <section class="image-supply-grid" aria-label="Image build supply">
        <article class="ops-panel builder-service">
          <div class="panel-header">
            <div><span class="eyebrow">Builders</span><h2>Capacity</h2></div>
            <span id="builderSummary">No builder metrics loaded</span>
          </div>
          <div class="stat-strip">
            <div class="stat-box"><span>Ready</span><strong id="builderReadyValue">-</strong></div>
            <div class="stat-box"><span>Prepared</span><strong id="builderPreparedValue">-</strong></div>
            <div class="stat-box"><span>Building</span><strong id="builderActiveBuildsValue">-</strong></div>
            <div class="stat-box"><span>CPU</span><strong id="builderCpuValue">-</strong></div>
            <div class="stat-box"><span>Memory</span><strong id="builderMemoryValue">-</strong></div>
          </div>
          <canvas id="builderBuildsChart" class="chart-canvas compact" width="760" height="150"></canvas>
          <div class="legend"><span><i class="swatch orange"></i>Active builds</span><span><i class="swatch blue-dash"></i>Ready builders</span></div>
        </article>
        <article class="workspace-card image-queue-summary">
          <div class="section-heading compact"><div><span class="eyebrow">Builds</span><h2>Queue</h2></div></div>
          <div class="resource-vector-list">
            <div class="resource-vector"><span>Pending builds</span><strong id="registryPendingBuildsValue">-</strong></div>
            <div class="resource-vector"><span>Oldest pending</span><strong id="registryOldestBuildValue">-</strong></div>
            <div class="resource-vector"><span>Active builds</span><strong id="registryActiveBuildsSummaryValue">-</strong></div>
            <div class="resource-vector emphasis"><span>Failed builds</span><strong id="registryFailedBuildsSummaryValue">-</strong></div>
          </div>
        </article>
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
  <div id="toastRegion" class="toast-region" aria-live="polite" aria-atomic="true"></div>
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

:root.dark {
  color-scheme: dark;
  --app-bar: #060b13;
  --app-bar-line: #1e293b;
  --background: #0b1220;
  --surface: #111b2e;
  --surface-soft: #162238;
  --line: #2b3a52;
  --line-soft: #233149;
  --text: #e5edf8;
  --muted: #94a3b8;
  --blue: #60a5fa;
  --green: #4ade80;
  --orange: #fb923c;
  --purple: #a78bfa;
  --red: #f87171;
  --amber: #fbbf24;
  --shadow: 0 1px 2px rgba(0, 0, 0, 0.35);
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
  flex-direction: row;
  align-items: stretch;
  gap: 4px;
  margin: 0 0 12px;
  border-bottom: 1px solid var(--line);
  overflow-x: auto;
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

.overview-page[hidden],
.overview-section[hidden],
.workspace-page[hidden],
.sandboxes-page[hidden],
.registry-page[hidden] {
  display: none;
}

.overview-page,
.workspace-page {
  display: grid;
  gap: 12px;
}

.health-strip,
.decision-hero,
.flow-panel,
.workspace-card,
.workspace-hero,
.queue-toolbar {
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface);
  box-shadow: var(--shadow);
}

.health-strip {
  display: grid;
  grid-template-columns: minmax(320px, 1.2fr) minmax(280px, 1fr) auto;
  gap: 16px;
  align-items: center;
  padding: 14px 16px;
}

.health-primary {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 12px;
}

.health-primary strong,
.health-primary span {
  display: block;
}

.health-primary strong {
  font-size: 15px;
}

.health-primary span {
  margin-top: 2px;
  color: var(--muted);
  font-size: 12px;
}

.health-icon {
  width: 12px;
  height: 12px;
  flex: 0 0 auto;
  border-radius: 50%;
  background: var(--muted);
  box-shadow: 0 0 0 5px color-mix(in srgb, var(--muted) 14%, transparent);
}

.health-ok {
  background: var(--green);
  box-shadow: 0 0 0 5px color-mix(in srgb, var(--green) 14%, transparent);
}

.health-warn {
  background: var(--amber);
  box-shadow: 0 0 0 5px color-mix(in srgb, var(--amber) 14%, transparent);
}

.health-bad {
  background: var(--red);
  box-shadow: 0 0 0 5px color-mix(in srgb, var(--red) 14%, transparent);
}

.health-signals {
  display: flex;
  min-width: 0;
  flex-wrap: wrap;
  gap: 6px;
}

.signal-chip,
.reason-chip,
.read-only-badge {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--line-soft);
  border-radius: 999px;
  background: var(--surface-soft);
  color: var(--muted);
  padding: 4px 8px;
  font-size: 11px;
  font-weight: 800;
}

.signal-chip.warn,
.reason-chip.warn {
  border-color: color-mix(in srgb, var(--amber) 35%, var(--line));
  color: var(--amber);
}

.signal-chip.bad,
.reason-chip.bad {
  border-color: color-mix(in srgb, var(--red) 35%, var(--line));
  color: var(--red);
}

.health-actions {
  display: flex;
  gap: 8px;
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

.decision-hero,
.workspace-hero {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(420px, 0.75fr);
  gap: 22px;
  padding: 18px;
}

.decision-title-row,
.section-heading,
.queue-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.decision-title-row {
  justify-content: flex-start;
  margin: 3px 0 6px;
}

.decision-title-row h2,
.section-heading h2,
.workspace-hero h2,
.queue-toolbar h2 {
  font-size: 18px;
}

.eyebrow {
  color: var(--blue);
  font-size: 10px;
  font-weight: 900;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

.workspace-copy,
.policy-note {
  color: var(--muted);
  font-size: 12px;
}

.reason-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 10px;
}

.decision-stats,
.node-hero-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.flow-panel,
.workspace-card {
  padding: 16px;
}

.section-heading {
  margin-bottom: 14px;
}

.section-heading.compact {
  align-items: flex-start;
  margin-bottom: 12px;
}

.section-summary {
  color: var(--muted);
  font-size: 12px;
  font-variant-numeric: tabular-nums;
}

.program-flow {
  display: grid;
  grid-template-columns: minmax(110px, 0.8fr) repeat(4, auto minmax(130px, 1fr));
  align-items: stretch;
  gap: 8px;
}

.flow-stage {
  min-width: 0;
  min-height: 96px;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  background: var(--surface-soft);
  color: var(--text);
  padding: 10px 12px;
  text-align: left;
}

.flow-stage:hover,
.flow-stage.is-selected {
  border-color: color-mix(in srgb, var(--blue) 55%, var(--line));
  background: color-mix(in srgb, var(--blue) 8%, var(--surface));
}

.flow-stage span,
.flow-stage strong,
.flow-stage small {
  display: block;
}

.flow-stage strong {
  margin: 5px 0 3px;
  font-size: 25px;
  font-variant-numeric: tabular-nums;
}

.flow-stage small {
  color: var(--muted);
}

.flow-index {
  color: var(--blue);
  font-size: 10px;
  font-weight: 900;
  text-transform: uppercase;
}

.flow-arrow {
  align-self: center;
  color: var(--muted);
  font-size: 18px;
}

.scheduler-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
}

.resource-vector-list {
  display: grid;
  gap: 6px;
}

.resource-vector {
  display: flex;
  min-width: 0;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--surface-soft);
}

.resource-vector span {
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
}

.resource-vector strong {
  font-size: 12px;
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.resource-vector.emphasis {
  border: 1px solid color-mix(in srgb, var(--blue) 25%, var(--line-soft));
}

.policy-values {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
  margin: 0;
}

.policy-values div {
  padding: 7px 8px;
  border-radius: 6px;
  background: var(--surface-soft);
}

.policy-values dt {
  color: var(--muted);
  font-size: 10px;
  font-weight: 800;
}

.policy-values dd {
  margin: 3px 0 0;
  font-size: 12px;
  font-weight: 800;
}

.policy-note {
  margin-top: 10px;
}

.queue-panel {
  min-width: 0;
}

.queue-toolbar {
  flex-wrap: wrap;
  padding: 12px 14px;
  border: 0;
  border-bottom: 1px solid var(--line);
  border-radius: 0;
  box-shadow: none;
}

.inline-search,
.inline-select {
  display: inline-flex;
}

.inline-search {
  flex: 1 1 260px;
  max-width: 420px;
}

.inline-search input,
.inline-select select {
  width: 100%;
  height: 36px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface-soft);
  color: var(--text);
  padding: 0 10px;
}

.program-table,
.node-table {
  min-width: 1080px;
}

.program-table td:nth-child(3),
.program-table td:nth-child(4),
.program-table td:nth-child(6),
.node-table td:nth-child(2) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-size: 11px;
}

.node-hero-stats {
  align-content: start;
}

.node-toolbar {
  justify-content: flex-start;
  border-radius: 8px;
  border-bottom: 1px solid var(--line);
}

.node-toolbar .section-summary {
  margin-left: auto;
}

.meter-stack {
  display: grid;
  min-width: 140px;
  gap: 4px;
}

.meter-label {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  color: var(--muted);
  font-size: 10px;
}

.meter {
  height: 5px;
  overflow: hidden;
  border-radius: 999px;
  background: var(--line-soft);
}

.meter > span {
  display: block;
  height: 100%;
  border-radius: inherit;
  background: var(--blue);
}

.meter.warn > span {
  background: var(--amber);
}

.meter.bad > span {
  background: var(--red);
}

.state-cell {
  display: inline-flex;
  align-items: center;
  gap: 7px;
}

.state-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--muted);
}

.state-dot.ok { background: var(--green); }
.state-dot.warn { background: var(--amber); }
.state-dot.bad { background: var(--red); }

:root.dark th {
  background: #162238;
  color: var(--text);
}

:root.dark .token-field input,
:root.dark .registry-search input,
:root.dark .registry-select select,
:root.dark .sandbox-search input {
  background: var(--surface-soft);
  color: var(--text);
}

:root.dark .badge-muted,
:root.dark .build-status.unknown {
  background: #334155;
  color: #dbeafe;
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

.registry-page,
.sandboxes-page {
  display: grid;
  gap: 12px;
}

.registry-hero,
.registry-toolbar,
.sandbox-hero,
.sandbox-toolbar {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  box-shadow: var(--shadow);
}

.registry-hero,
.sandbox-hero {
  display: grid;
  grid-template-columns: minmax(0, 1.15fr) minmax(420px, 0.85fr);
  gap: 14px;
  padding: 14px;
}

.registry-hero-main,
.sandbox-hero-main {
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

.registry-stat-grid,
.sandbox-stat-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
}

.registry-toolbar,
.sandbox-toolbar {
  display: grid;
  grid-template-columns: minmax(280px, 1fr) 220px minmax(220px, auto);
  gap: 10px;
  align-items: end;
  padding: 12px 14px;
}

.sandbox-toolbar {
  grid-template-columns: minmax(280px, 1fr) auto auto minmax(220px, auto);
}

.registry-search,
.registry-select,
.sandbox-search {
  display: grid;
  gap: 5px;
}

.registry-search span,
.registry-select span,
.sandbox-search span {
  color: var(--muted);
  font-size: 11px;
  font-weight: 800;
}

.registry-search input,
.registry-select select,
.sandbox-search input {
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

.sandbox-list-panel {
  min-width: 0;
}

.sandbox-table {
  min-width: 1180px;
  table-layout: fixed;
}

.sandbox-table th:nth-child(1),
.sandbox-table td:nth-child(1) {
  width: 105px;
}

.sandbox-table th:nth-child(2),
.sandbox-table td:nth-child(2) {
  width: 210px;
}

.sandbox-table th:nth-child(3),
.sandbox-table td:nth-child(3) {
  width: 245px;
}

.sandbox-table th:nth-child(4),
.sandbox-table td:nth-child(4) {
  width: 165px;
}

.sandbox-table th:nth-child(5),
.sandbox-table td:nth-child(5) {
  width: 175px;
}

.sandbox-table th:nth-child(6),
.sandbox-table td:nth-child(6) {
  width: 80px;
}

.sandbox-table th:nth-child(8),
.sandbox-table td:nth-child(8) {
  width: 130px;
}

.sandbox-id,
.sandbox-image,
.sandbox-node,
.sandbox-resources {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}

.sandbox-id,
.sandbox-image,
.sandbox-resources,
.sandbox-labels {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.sandbox-labels {
  color: var(--muted);
  font-size: 12px;
}

.sandbox-status {
  display: inline-flex;
  min-width: 76px;
  height: 22px;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  padding: 0 8px;
  font-size: 12px;
  font-weight: 800;
  white-space: nowrap;
}

.sandbox-status.running {
  background: #dcfce7;
  color: #15803d;
}

.sandbox-status.creating,
.sandbox-status.pending {
  background: #fef3c7;
  color: #b45309;
}

.sandbox-status.unknown {
  background: #e2e8f0;
  color: #475569;
}

.sandbox-status.failed,
.sandbox-status.error {
  background: #fee2e2;
  color: #b91c1c;
}

.table-action {
  height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--text);
  padding: 0 12px;
  font-weight: 800;
  white-space: nowrap;
}

.table-action:hover:not(:disabled) {
  background: var(--surface-soft);
}

.table-action.danger {
  border-color: #fecaca;
  background: #fff7f7;
  color: #b91c1c;
}

.table-action.danger:hover:not(:disabled) {
  background: #fee2e2;
}

.table-action:disabled {
  cursor: not-allowed;
  opacity: 0.52;
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

  .health-strip,
  .decision-hero,
  .workspace-hero {
    grid-template-columns: 1fr;
  }

  .scheduler-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .policy-card {
    grid-column: 1 / -1;
  }

  .registry-toolbar,
  .sandbox-toolbar {
    grid-template-columns: 1fr 220px;
  }

  .registry-toolbar .registry-copy,
  .sandbox-toolbar .registry-copy {
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

  .health-actions {
    flex-wrap: wrap;
  }

  .program-flow {
    grid-template-columns: 1fr;
  }

  .flow-arrow {
    display: none;
  }

  .scheduler-grid {
    grid-template-columns: 1fr;
  }

  .policy-card {
    grid-column: auto;
  }

  .decision-stats,
  .node-hero-stats {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .registry-toolbar,
  .registry-stat-grid,
  .sandbox-toolbar,
  .sandbox-stat-grid {
    grid-template-columns: 1fr;
  }

  .registry-hero,
  .sandbox-hero {
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

/* Operations cockpit: density comes from composition, not smaller controls. */
:root {
  --rail-width: 228px;
  --app-bar-height: 68px;
  --background: #edf1f7;
  --surface: #ffffff;
  --surface-soft: #f4f7fb;
  --line: #d5dce7;
  --line-soft: #e5eaf1;
  --text: #101828;
  --muted: #667085;
  --blue: #2563eb;
  --purple: #6941c6;
  --shadow: 0 1px 2px rgba(16, 24, 40, 0.05), 0 8px 24px rgba(16, 24, 40, 0.055);
}

:root.dark {
  --background: #080e18;
  --surface: #101827;
  --surface-soft: #151f31;
  --line: #2a374b;
  --line-soft: #202c40;
  --text: #eef4ff;
  --muted: #98a7bc;
  --shadow: 0 1px 2px rgba(0, 0, 0, 0.3), 0 10px 30px rgba(0, 0, 0, 0.22);
}

body {
  background:
    radial-gradient(circle at 75% -15%, rgba(37, 99, 235, 0.08), transparent 32rem),
    var(--background);
  font-size: 14px;
}

.app-bar {
  position: fixed;
  inset: 0 0 auto 0;
  z-index: 20;
  min-height: var(--app-bar-height);
  padding: 0 18px 0 0;
  border-bottom: 1px solid #253149;
  background: #0b1220;
  box-shadow: 0 1px 0 rgba(255, 255, 255, 0.03);
}

.brand {
  width: var(--rail-width);
  height: var(--app-bar-height);
  flex: 0 0 var(--rail-width);
  gap: 12px;
  padding: 0 18px;
  border-right: 1px solid #253149;
}

.brand-mark {
  display: grid;
  width: 26px;
  height: 26px;
  grid-template-columns: repeat(2, 1fr);
  gap: 3px;
  flex: 0 0 auto;
  padding: 3px;
  border-radius: 7px;
  background: rgba(255, 255, 255, 0.08);
}

.brand-mark span {
  border-radius: 2px;
  background: #60a5fa;
}

.brand-mark span:nth-child(2) { background: #a78bfa; }
.brand-mark span:nth-child(3) { background: #34d399; }
.brand-mark span:nth-child(4) { background: #fbbf24; }

.brand-copy {
  display: grid;
  min-width: 0;
  line-height: 1.15;
}

.brand-copy strong {
  font-size: 14px;
  letter-spacing: -0.01em;
}

.brand-copy small {
  margin-top: 3px;
  color: #8ea0ba;
  font-size: 11px;
  font-weight: 650;
}

.top-controls {
  gap: 8px;
}

.select-control,
.icon-button,
.status-pill {
  min-height: 36px;
  border: 1px solid #2a3850;
  background: #111c2e;
}

.select-control {
  gap: 4px;
  border-radius: 8px;
  padding-left: 9px;
}

.select-control select {
  height: 34px;
  padding: 0 8px 0 3px;
}

.icon-button {
  width: 38px;
  height: 38px;
  border-radius: 8px;
}

.status-pill {
  min-width: 104px;
  border-radius: 8px;
  font-size: 12px;
  letter-spacing: 0.01em;
}

.page-shell {
  width: auto;
  max-width: none;
  margin: 0 0 0 var(--rail-width);
  padding: calc(var(--app-bar-height) + 20px) 22px 36px;
}

.page-tabs {
  position: fixed;
  inset: var(--app-bar-height) auto 0 0;
  z-index: 12;
  display: flex;
  width: var(--rail-width);
  flex-direction: column;
  gap: 5px;
  margin: 0;
  padding: 22px 12px;
  overflow: auto;
  border: 0;
  border-right: 1px solid #253149;
  background: #0b1220;
}

.page-tabs::before {
  margin: 0 10px 8px;
  color: #667892;
  content: "WORKSPACES";
  font-size: 10px;
  font-weight: 850;
  letter-spacing: 0.14em;
}

.page-tab {
  display: flex;
  width: 100%;
  height: 48px;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  border: 1px solid transparent;
  border-radius: 9px;
  background: transparent;
  color: #a9b6c9;
  padding: 0 10px;
  text-align: left;
}

.page-tab:hover {
  border-color: #293750;
  background: #111c2e;
  color: #f8fafc;
}

.page-tab.is-active {
  border-color: #31558d;
  background: linear-gradient(135deg, #172d4f, #13243e);
  color: #ffffff;
  box-shadow: inset 3px 0 #60a5fa;
}

.nav-label {
  display: flex;
  min-width: 0;
  align-items: center;
  gap: 10px;
}

.nav-label i {
  display: inline-flex;
  width: 27px;
  height: 27px;
  flex: 0 0 auto;
  align-items: center;
  justify-content: center;
  border: 1px solid #314158;
  border-radius: 7px;
  background: #111c2e;
  color: #91a4c0;
  font-size: 9px;
  font-style: normal;
  font-weight: 900;
  letter-spacing: 0.06em;
}

.page-tab.is-active .nav-label i {
  border-color: #3767aa;
  background: #1d3a65;
  color: #bfdbfe;
}

.nav-badge {
  display: inline-flex;
  min-width: 28px;
  height: 23px;
  align-items: center;
  justify-content: center;
  border: 1px solid #314158;
  border-radius: 999px;
  background: #111c2e;
  color: #a9b6c9;
  padding: 0 7px;
  font-size: 10px;
  font-weight: 850;
  font-variant-numeric: tabular-nums;
}

.nav-badge-ok { border-color: #166534; color: #86efac; }
.nav-badge-warn { border-color: #92400e; color: #fde68a; }
.nav-badge-bad { border-color: #991b1b; color: #fecaca; }

.page-title {
  align-items: center;
  margin-bottom: 14px;
  padding: 0 2px;
}

.page-kicker {
  display: block;
  margin-bottom: 4px;
  color: var(--blue);
  font-size: 10px;
  font-weight: 850;
  letter-spacing: 0.12em;
  text-transform: uppercase;
}

h1 {
  font-size: clamp(24px, 2vw, 31px);
  letter-spacing: -0.035em;
}

.page-title p {
  margin-top: 3px;
  font-size: 14px;
}

.last-updated {
  font-size: 12px;
  font-weight: 650;
}

.title-actions button,
.auth-panel button,
.table-action {
  height: 38px;
  border-radius: 8px;
}

.auth-panel {
  border-radius: 11px;
  box-shadow: var(--shadow);
}

.overview-page,
.workspace-page,
.registry-page,
.sandboxes-page {
  gap: 10px;
}

.health-strip,
.decision-hero,
.flow-panel,
.workspace-card,
.workspace-hero,
.queue-toolbar,
.registry-hero,
.registry-toolbar,
.sandbox-hero,
.sandbox-toolbar,
.metric-card,
.chart-panel,
.ops-panel,
.event-panel {
  border-color: var(--line);
  border-radius: 11px;
  box-shadow: var(--shadow);
}

.health-strip {
  min-height: 72px;
  grid-template-columns: minmax(340px, 1.2fr) minmax(260px, 1fr) auto;
  gap: 12px;
  padding: 13px 15px;
  border-left: 4px solid var(--blue);
  background:
    linear-gradient(100deg, color-mix(in srgb, var(--blue) 6%, var(--surface)), var(--surface) 44%);
}

.health-primary strong {
  font-size: 16px;
  letter-spacing: -0.01em;
}

.health-primary span {
  font-size: 12px;
}

.signal-chip,
.reason-chip,
.read-only-badge {
  min-height: 26px;
  padding: 4px 9px;
  font-size: 11px;
}

.metric-grid {
  gap: 10px;
  margin: 0;
}

.metric-card {
  min-height: 118px;
  padding: 16px;
  background: linear-gradient(145deg, var(--surface), color-mix(in srgb, var(--accent) 3%, var(--surface)));
}

.metric-card::before {
  position: absolute;
  inset: 0 0 auto 0;
  height: 3px;
  background: var(--accent);
  content: "";
  opacity: 0.8;
}

.metric-label {
  color: var(--muted);
  font-size: 11px;
  font-weight: 850;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.metric-card strong {
  margin-top: 9px;
  font-size: 27px;
  letter-spacing: -0.035em;
}

.metric-detail {
  margin-top: 7px;
  font-size: 12px;
}

.chart-grid,
.ops-grid {
  gap: 10px;
  margin: 0;
}

.chart-panel,
.ops-panel {
  padding: 14px 15px 12px;
}

.panel-header {
  margin-bottom: 10px;
}

h2 {
  font-size: 15px;
  letter-spacing: -0.015em;
}

.chart-canvas {
  height: 200px;
}

.chart-canvas.small {
  height: 184px;
}

.legend {
  color: var(--muted);
  font-size: 11px;
  font-weight: 650;
}

.ops-grid .ops-panel:nth-child(1),
.ops-grid .ops-panel:nth-child(2) {
  grid-column: span 6;
}

.ops-grid .ops-panel:nth-child(3) {
  grid-column: span 8;
}

.ops-grid .ops-panel:nth-child(4) {
  grid-column: span 4;
}

.stat-strip {
  gap: 7px;
  margin-bottom: 0;
}

.stat-box {
  min-height: 68px;
  padding: 10px 11px;
  border: 0;
  border-radius: 8px;
  background: var(--surface-soft);
}

.stat-box span {
  font-size: 10px;
  letter-spacing: 0.035em;
  text-transform: uppercase;
}

.stat-box strong {
  margin-top: 7px;
  font-size: 21px;
  letter-spacing: -0.025em;
}

.activity-grid {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: 10px;
}

.activity-grid .build-panel {
  grid-column: span 7;
  margin: 0;
}

.activity-grid .trace-panel {
  grid-column: span 5;
}

.activity-grid .activity-event-panel {
  grid-column: 1 / -1;
}

.activity-grid .table-wrap {
  max-height: 318px;
  overflow: auto;
}

.activity-grid .build-panel table {
  min-width: 780px;
}

.activity-grid .trace-panel table {
  min-width: 640px;
}

.activity-grid .activity-event-panel table {
  min-width: 820px;
}

.table-header {
  min-height: 44px;
  padding: 0 15px;
}

table {
  min-width: 860px;
}

th,
td {
  height: 42px;
  padding: 10px 13px;
}

th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: var(--surface-soft);
  color: var(--muted);
  font-size: 10px;
  letter-spacing: 0.045em;
  text-transform: uppercase;
}

tbody tr {
  transition: background 120ms ease;
}

tbody tr:hover {
  background: color-mix(in srgb, var(--blue) 4%, var(--surface));
}

.decision-hero,
.workspace-hero,
.registry-hero,
.sandbox-hero {
  grid-template-columns: minmax(0, 1.05fr) minmax(520px, 0.95fr);
  gap: 18px;
  padding: 16px;
}

.decision-title-row h2,
.section-heading h2,
.workspace-hero h2,
.queue-toolbar h2 {
  font-size: 18px;
}

.program-flow {
  gap: 7px;
}

.flow-stage {
  min-height: 104px;
  border-radius: 9px;
  padding: 12px;
}

.flow-stage strong {
  font-size: 27px;
}

.scheduler-grid {
  grid-template-columns: minmax(0, 5fr) minmax(0, 5fr) minmax(300px, 4fr);
  gap: 10px;
}

.flow-panel,
.workspace-card {
  padding: 15px;
}

.resource-vector {
  min-height: 38px;
  padding: 8px 10px;
  border-radius: 7px;
}

.queue-toolbar,
.registry-toolbar,
.sandbox-toolbar {
  padding: 12px 14px;
}

.inline-search input,
.inline-select select,
.registry-search input,
.registry-select select,
.sandbox-search input {
  height: 38px;
  border-radius: 8px;
}

.node-table,
.program-table,
.sandbox-table,
.registry-table {
  min-width: 1080px;
}

.meter {
  height: 7px;
}

:root.dark .page-title button,
:root.dark .table-action,
:root.dark .auth-panel button {
  background: var(--surface-soft);
}

:root.dark .repo-pill {
  border-color: #244b7f;
  background: #152c4d;
  color: #bfdbfe;
}

:root.dark .tag-chip {
  color: var(--text);
}

@media (max-width: 1480px) {
  :root { --rail-width: 204px; }

  .metric-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .chart-wide,
  .chart-small {
    grid-column: span 6;
  }

  .ops-grid .ops-panel:nth-child(n) {
    grid-column: span 6;
  }

  .activity-grid .build-panel,
  .activity-grid .trace-panel {
    grid-column: 1 / -1;
  }

  .health-strip,
  .decision-hero,
  .workspace-hero,
  .registry-hero,
  .sandbox-hero {
    grid-template-columns: 1fr;
  }

  .scheduler-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .policy-card {
    grid-column: 1 / -1;
  }
}

@media (max-width: 940px) {
  :root {
    --rail-width: 0px;
    --app-bar-height: 62px;
  }

  .app-bar {
    position: sticky;
    min-height: var(--app-bar-height);
    flex-direction: row;
    align-items: center;
    padding: 0 12px;
  }

  .brand {
    width: auto;
    height: var(--app-bar-height);
    flex: 0 1 auto;
    border: 0;
    padding: 0;
  }

  .brand-copy small {
    display: none;
  }

  .page-shell {
    margin: 0;
    padding: 16px 12px 26px;
  }

  .page-tabs {
    position: sticky;
    inset: auto;
    top: 0;
    z-index: 10;
    width: auto;
    flex-direction: row;
    gap: 5px;
    margin: 0 -12px 14px;
    padding: 8px 12px;
    overflow-x: auto;
    border: 0;
    border-bottom: 1px solid #253149;
  }

  .page-tabs::before {
    display: none;
  }

  .page-tab {
    width: auto;
    min-width: max-content;
    padding: 0 11px;
  }

  .nav-label i {
    display: none;
  }

  .page-title {
    flex-direction: row;
    align-items: center;
  }

  .top-controls {
    width: auto;
    justify-content: flex-end;
    flex-wrap: nowrap;
    overflow-x: auto;
  }

  .scheduler-grid {
    grid-template-columns: 1fr;
  }

  .policy-card {
    grid-column: auto;
  }
}

@media (max-width: 700px) {
  .brand-copy strong {
    display: none;
  }

  .brand-mark {
    width: 30px;
    height: 30px;
  }

  .select-control .clock-mark,
  .select-control .refresh-mark {
    display: none;
  }

  .status-pill {
    min-width: 86px;
  }

  .page-title {
    align-items: flex-start;
    flex-direction: column;
  }

  .metric-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .chart-wide,
  .chart-small,
  .ops-grid .ops-panel:nth-child(n) {
    grid-column: 1 / -1;
  }

  .health-strip {
    grid-template-columns: 1fr;
  }

  .activity-grid {
    display: grid;
    grid-template-columns: 1fr;
  }

  .activity-grid > * {
    grid-column: 1 !important;
  }
}

@media (max-width: 480px) {
  .metric-grid,
  .stat-strip,
  .compact-strip {
    grid-template-columns: 1fr;
  }

  .nav-badge {
    display: none;
  }
}

/* Operations workspace v2: one hierarchy built around operator decisions. */
:root {
  --space-1: 4px;
  --space-2: 8px;
  --space-3: 12px;
  --space-4: 16px;
  --space-5: 24px;
  --space-6: 32px;
  --radius-sm: 7px;
  --radius-md: 11px;
  --healthy: #15803d;
  --warning: #b45309;
  --critical: #b42318;
  --selection: #2563eb;
}

.skip-link {
  position: fixed;
  left: 12px;
  top: 10px;
  z-index: 100;
  padding: 9px 12px;
  border-radius: 7px;
  background: white;
  color: #111827;
  font-weight: 800;
  transform: translateY(-160%);
}

.skip-link:focus {
  transform: translateY(0);
}

.toast-region {
  position: fixed;
  right: 18px;
  bottom: 18px;
  z-index: 80;
  max-width: min(420px, calc(100vw - 36px));
  padding: 11px 14px;
  border: 1px solid #166534;
  border-radius: 9px;
  background: #052e16;
  color: #dcfce7;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.22);
  font-size: 13px;
  font-weight: 750;
  opacity: 0;
  pointer-events: none;
  transform: translateY(8px);
  transition: opacity 140ms ease, transform 140ms ease;
}

.toast-region.is-visible {
  opacity: 1;
  transform: translateY(0);
}

.toast-region.bad {
  border-color: #991b1b;
  background: #450a0a;
  color: #fee2e2;
}

.page-shell {
  padding-top: calc(var(--app-bar-height) + var(--space-5));
}

.page-title {
  min-height: 66px;
  margin-bottom: var(--space-4);
}

.page-title h1 {
  max-width: 840px;
}

.page-title p {
  max-width: 780px;
  color: var(--muted);
  line-height: 1.45;
}

.page-tabs::before {
  display: none;
}

.nav-section-label {
  display: block;
  margin: 4px 10px 5px;
  color: #667892;
  font-size: 10px;
  font-weight: 850;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}

.nav-section-manage {
  margin-top: 17px;
}

.nav-icon {
  position: relative;
}

.nav-icon::before,
.nav-icon::after {
  position: absolute;
  content: "";
}

.nav-icon-overview::before {
  inset: 7px;
  border: 2px solid currentColor;
  border-radius: 3px;
  box-shadow: 7px 0 0 -5px currentColor, 0 7px 0 -5px currentColor;
}

.nav-icon-scheduler::before {
  left: 7px;
  top: 7px;
  width: 4px;
  height: 4px;
  border-radius: 50%;
  background: currentColor;
  box-shadow: 10px 5px 0 currentColor, 0 11px 0 currentColor;
}

.nav-icon-scheduler::after {
  left: 10px;
  top: 9px;
  width: 9px;
  height: 12px;
  border-left: 1px solid currentColor;
  border-bottom: 1px solid currentColor;
}

.nav-icon-nodes::before {
  inset: 6px 5px;
  border: 1px solid currentColor;
  border-radius: 3px;
  box-shadow: inset 0 5px transparent, inset 0 6px currentColor, inset 0 11px transparent, inset 0 12px currentColor;
}

.nav-icon-sandboxes::before {
  inset: 7px 6px 6px;
  border: 1px solid currentColor;
  border-radius: 2px;
  transform: rotate(30deg) skew(-4deg, -4deg);
}

.nav-icon-registry::before {
  left: 6px;
  top: 7px;
  width: 15px;
  height: 5px;
  border: 1px solid currentColor;
  border-radius: 50%;
  box-shadow: 0 5px 0 -1px #111c2e, 0 5px 0 0 currentColor, 0 10px 0 -1px #111c2e, 0 10px 0 0 currentColor;
}

.page-tab.is-active .nav-icon-registry::before {
  box-shadow: 0 5px 0 -1px #1d3a65, 0 5px 0 0 currentColor, 0 10px 0 -1px #1d3a65, 0 10px 0 0 currentColor;
}

.overview-page {
  gap: var(--space-4);
}

.overview-section {
  margin: 0;
}

.command-grid {
  display: grid;
  grid-template-columns: minmax(0, 7fr) minmax(340px, 5fr);
  gap: var(--space-3);
}

.command-grid .health-strip,
.decision-brief,
.capacity-card,
.pipeline-card,
.fleet-signal-grid article,
.image-supply-grid > * {
  border: 1px solid var(--line);
  border-radius: var(--radius-md);
  background: var(--surface);
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.command-grid .health-strip {
  display: flex;
  min-height: 196px;
  flex-direction: column;
  align-items: stretch;
  justify-content: space-between;
  padding: var(--space-5);
  border-left: 4px solid var(--selection);
  background: var(--surface);
}

.health-primary {
  align-items: flex-start;
}

.health-primary strong {
  display: block;
  margin: 3px 0 6px;
  font-size: clamp(21px, 2vw, 27px);
  letter-spacing: -0.035em;
}

.health-primary > div > span:last-child {
  max-width: 720px;
  color: var(--muted);
  font-size: 14px;
  line-height: 1.5;
}

.health-signals {
  justify-content: flex-start;
}

button.signal-chip {
  cursor: pointer;
  font-family: inherit;
}

button.signal-chip:hover {
  border-color: var(--selection);
}

.health-actions {
  justify-content: flex-start;
}

.decision-brief {
  min-height: 196px;
  padding: var(--space-4);
}

.decision-brief .panel-header {
  align-items: flex-start;
}

.decision-brief h2 {
  margin-top: 3px;
  font-size: 20px;
}

.decision-summary {
  min-height: 20px;
  margin: 0 0 10px;
  color: var(--muted);
  line-height: 1.45;
}

.decision-reasons {
  display: grid;
  gap: 5px;
  margin-bottom: var(--space-3);
}

.decision-reasons span {
  position: relative;
  padding-left: 14px;
  color: var(--text);
  font-size: 12px;
  line-height: 1.35;
}

.decision-reasons span::before {
  position: absolute;
  left: 1px;
  top: 0.55em;
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--selection);
  content: "";
}

.decision-facts {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 1px;
  overflow: hidden;
  border: 1px solid var(--line-soft);
  border-radius: var(--radius-sm);
  background: var(--line-soft);
}

.decision-facts > div {
  min-width: 0;
  padding: 9px 10px;
  background: var(--surface-soft);
}

.decision-facts span,
.metric-context {
  display: block;
  color: var(--muted);
  font-size: 10px;
  font-weight: 750;
  letter-spacing: 0.03em;
  text-transform: uppercase;
}

.decision-facts strong {
  display: block;
  overflow: hidden;
  margin-top: 5px;
  font-size: 13px;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.overview-heading {
  min-height: auto;
  align-items: end;
  padding: 5px 2px 0;
}

.overview-heading h2 {
  margin-top: 2px;
  font-size: 18px;
}

.metric-grid {
  grid-template-columns: repeat(6, minmax(0, 1fr));
}

.metric-card {
  display: flex;
  min-width: 0;
  min-height: 142px;
  flex-direction: column;
  align-items: flex-start;
  padding: 15px;
  border-top: 1px solid var(--line);
  background: var(--surface);
}

.metric-card::before {
  display: none;
}

.metric-card strong {
  margin-top: 12px;
  font-size: 30px;
  font-variant-numeric: tabular-nums;
}

.metric-card .metric-detail {
  min-height: 34px;
  margin: 5px 0 10px;
  line-height: 1.4;
}

.metric-context {
  margin-top: auto;
  font-size: 9px;
}

.accent-green,
.accent-slate {
  --accent: var(--selection);
}

.overview-workbench {
  display: grid;
  grid-template-columns: minmax(360px, 5fr) minmax(560px, 7fr);
  gap: var(--space-3);
}

.capacity-card,
.pipeline-card {
  padding: var(--space-4);
}

.headroom-list {
  display: grid;
  gap: 16px;
  margin: 18px 0 14px;
}

.headroom-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 110px;
  gap: 7px 14px;
}

.headroom-label {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.headroom-label span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 750;
}

.headroom-label strong {
  font-size: 14px;
  font-variant-numeric: tabular-nums;
}

.dual-meter {
  position: relative;
  grid-column: 1 / -1;
  height: 10px;
  overflow: hidden;
  border-radius: 999px;
  background: var(--surface-soft);
  box-shadow: inset 0 0 0 1px var(--line-soft);
}

.dual-meter span {
  position: absolute;
  inset: 0 auto 0 0;
  border-radius: inherit;
  transition: width 180ms ease;
}

.meter-reserved {
  z-index: 1;
  background: rgba(37, 99, 235, 0.28);
}

.meter-actual {
  z-index: 2;
  height: 5px;
  margin-top: 2.5px;
  background: var(--selection);
}

.meter-disk {
  background: var(--text);
}

.hard-limit .headroom-label span::after {
  margin-left: 7px;
  border: 1px solid var(--line);
  border-radius: 999px;
  content: "hard limit";
  padding: 2px 6px;
  color: var(--muted);
  font-size: 9px;
  letter-spacing: 0.03em;
  text-transform: uppercase;
}

.headroom-detail {
  grid-column: 1 / -1;
  color: var(--muted);
  font-size: 11px;
}

.capacity-rule {
  display: flex;
  gap: 9px;
  align-items: flex-start;
  padding: 10px 11px;
  border: 1px solid color-mix(in srgb, var(--warning) 30%, var(--line));
  border-radius: var(--radius-sm);
  background: color-mix(in srgb, var(--warning) 7%, var(--surface));
  color: var(--muted);
  font-size: 11px;
  line-height: 1.4;
}

.capacity-rule > span {
  display: inline-flex;
  width: 17px;
  height: 17px;
  flex: 0 0 auto;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  background: var(--warning);
  color: white;
  font-size: 10px;
  font-weight: 900;
}

.overview-pipeline {
  display: grid;
  grid-template-columns: minmax(115px, 1fr) 20px minmax(115px, 1fr) 20px minmax(115px, 1fr) 20px minmax(115px, 1fr);
  align-items: center;
  margin-top: 17px;
}

.pipeline-stage {
  min-height: 122px;
  padding: 13px;
  border: 1px solid var(--line);
  border-radius: 9px;
  background: var(--surface-soft);
}

.pipeline-stage > span {
  display: block;
  min-height: 30px;
  color: var(--muted);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.03em;
  line-height: 1.35;
  text-transform: uppercase;
}

.pipeline-stage strong {
  display: block;
  margin: 8px 0 5px;
  font-size: 28px;
  font-variant-numeric: tabular-nums;
}

.pipeline-stage small {
  display: block;
  color: var(--muted);
  font-size: 10px;
  line-height: 1.35;
}

.pipeline-stage.attention {
  border-color: color-mix(in srgb, var(--selection) 35%, var(--line));
  background: color-mix(in srgb, var(--selection) 5%, var(--surface));
}

.pipeline-connector {
  position: relative;
  height: 1px;
  background: var(--line);
}

.pipeline-connector::after {
  position: absolute;
  right: -1px;
  top: -3px;
  width: 6px;
  height: 6px;
  border-top: 1px solid var(--muted);
  border-right: 1px solid var(--muted);
  content: "";
  transform: rotate(45deg);
}

.latency-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 1px;
  margin-top: var(--space-3);
  overflow: hidden;
  border: 1px solid var(--line-soft);
  border-radius: var(--radius-sm);
  background: var(--line-soft);
}

.latency-strip > div {
  padding: 9px 10px;
  background: var(--surface);
}

.latency-strip span {
  display: block;
  color: var(--muted);
  font-size: 9px;
  font-weight: 800;
  letter-spacing: 0.03em;
  text-transform: uppercase;
}

.latency-strip strong {
  display: block;
  margin-top: 5px;
  font-size: 15px;
  font-variant-numeric: tabular-nums;
}

.trend-grid {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: var(--space-3);
}

.trend-grid .chart-panel {
  margin: 0;
  box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
}

.trend-grid .panel-header > div > span {
  display: block;
  margin-top: 2px;
  color: var(--muted);
  font-size: 11px;
}

.trend-supply,
.trend-demand {
  grid-column: span 6;
}

.trend-pressure {
  grid-column: span 3;
}

.trend-latency {
  grid-column: span 6;
}

.trend-grid .chart-canvas {
  height: 220px;
}

.trend-grid .chart-canvas.small {
  height: 184px;
}

.fleet-signal-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: var(--space-2);
}

.scheduler-grid {
  grid-template-columns: minmax(0, 8fr) minmax(300px, 4fr);
}

.capacity-equation-card {
  min-width: 0;
}

.capacity-equation-wrap {
  border: 1px solid var(--line-soft);
  border-radius: var(--radius-sm);
}

.capacity-equation-table {
  width: 100%;
  min-width: 620px;
}

.capacity-equation-table th,
.capacity-equation-table td {
  height: 39px;
  font-variant-numeric: tabular-nums;
}

.capacity-equation-table tbody th {
  position: static;
  background: transparent;
  color: var(--text);
  font-size: 12px;
  letter-spacing: 0;
  text-transform: none;
}

.capacity-equation-table .supply-row {
  background: color-mix(in srgb, var(--healthy) 4%, var(--surface));
}

.capacity-equation-table .deficit-row {
  background: color-mix(in srgb, var(--critical) 6%, var(--surface));
  font-weight: 800;
}

.equation-footnote {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  margin-top: 10px;
  color: var(--muted);
  font-size: 10px;
}

.policy-card summary {
  cursor: pointer;
  list-style: none;
}

.policy-card summary::-webkit-details-marker {
  display: none;
}

.policy-card:not([open]) {
  align-self: start;
}

.policy-card[open] .policy-values {
  margin-top: 12px;
}

.fleet-signal-grid article {
  min-height: 90px;
  padding: 12px;
}

.fleet-signal-grid span,
.fleet-signal-grid small {
  display: block;
  color: var(--muted);
  font-size: 10px;
  line-height: 1.35;
}

.fleet-signal-grid span {
  font-weight: 800;
  letter-spacing: 0.03em;
  text-transform: uppercase;
}

.fleet-signal-grid strong {
  display: block;
  margin: 9px 0 5px;
  font-size: 20px;
  font-variant-numeric: tabular-nums;
}

.node-hero-stats {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.sandbox-toolbar {
  grid-template-columns: minmax(280px, 2fr) minmax(200px, 1fr) auto minmax(200px, 1fr);
}

.sandbox-filter {
  display: grid;
  gap: 6px;
  color: var(--muted);
  font-size: 11px;
  font-weight: 750;
}

.sandbox-filter select {
  height: 38px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface);
  color: var(--text);
  padding: 0 10px;
}

.sandbox-status.parked {
  border-color: #94a3b8;
  background: #f1f5f9;
  color: #475569;
}

.sandbox-status.transitioning,
.sandbox-status.migrating {
  border-color: #60a5fa;
  background: #eff6ff;
  color: #1d4ed8;
}

.sandbox-status.stale {
  border-color: #f59e0b;
  background: #fffbeb;
  color: #92400e;
}

:root.dark .sandbox-status.parked {
  background: #1e293b;
  color: #cbd5e1;
}

:root.dark .sandbox-status.transitioning,
:root.dark .sandbox-status.migrating {
  background: #172554;
  color: #bfdbfe;
}

:root.dark .sandbox-status.stale {
  background: #451a03;
  color: #fde68a;
}

.image-supply-grid {
  display: grid;
  grid-template-columns: minmax(0, 8fr) minmax(280px, 4fr);
  gap: var(--space-3);
}

.image-supply-grid .ops-panel,
.image-supply-grid .workspace-card {
  margin: 0;
}

.registry-stat-grid {
  grid-template-columns: repeat(6, minmax(0, 1fr));
}

.activity-grid .activity-event-panel {
  grid-column: 1 / -1;
  order: -1;
}

.activity-grid .activity-event-panel tbody tr:first-child {
  background: color-mix(in srgb, var(--selection) 4%, var(--surface));
}

.diagnostic-disclosure {
  padding: 0;
}

.diagnostic-disclosure summary {
  display: flex;
  min-height: 48px;
  cursor: pointer;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 0 15px;
  color: var(--text);
  font-weight: 800;
  list-style: none;
}

.diagnostic-disclosure summary::-webkit-details-marker {
  display: none;
}

.diagnostic-disclosure summary::after {
  width: 7px;
  height: 7px;
  flex: 0 0 auto;
  border-right: 1.5px solid var(--muted);
  border-bottom: 1.5px solid var(--muted);
  content: "";
  transform: rotate(45deg);
}

.diagnostic-disclosure[open] summary::after {
  transform: rotate(225deg);
}

.diagnostic-disclosure summary span:nth-child(2) {
  margin-left: auto;
  color: var(--muted);
  font-size: 11px;
  font-weight: 650;
}

button:focus-visible,
input:focus-visible,
select:focus-visible,
[role="tab"]:focus-visible {
  outline: 3px solid color-mix(in srgb, var(--selection) 45%, transparent);
  outline-offset: 2px;
}

@media (max-width: 1480px) {
  .metric-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .command-grid,
  .overview-workbench,
  .image-supply-grid {
    grid-template-columns: 1fr;
  }

  .trend-pressure {
    grid-column: span 6;
  }

  .trend-latency {
    grid-column: 1 / -1;
  }

  .fleet-signal-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .node-hero-stats,
  .registry-stat-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 940px) {
  .page-shell {
    padding-top: var(--space-4);
  }

  .page-tabs {
    top: var(--app-bar-height);
  }

  .nav-section-label {
    display: none;
  }

  .overview-workbench,
  .command-grid,
  .scheduler-grid {
    grid-template-columns: 1fr;
  }

  .overview-pipeline {
    grid-template-columns: repeat(4, minmax(140px, 1fr));
    gap: 8px;
    overflow-x: auto;
  }

  .pipeline-connector {
    display: none;
  }

  .trend-supply,
  .trend-demand,
  .trend-pressure,
  .trend-latency {
    grid-column: span 6;
  }

  .sandbox-toolbar {
    grid-template-columns: 1fr 1fr auto;
  }

  .sandbox-toolbar .registry-copy {
    grid-column: 1 / -1;
  }
}

@media (max-width: 700px) {
  .command-grid .health-strip,
  .decision-brief,
  .capacity-card,
  .pipeline-card {
    padding: var(--space-4);
  }

  .metric-grid,
  .latency-strip,
  .fleet-signal-grid,
  .node-hero-stats,
  .registry-stat-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .trend-supply,
  .trend-demand,
  .trend-pressure,
  .trend-latency {
    grid-column: 1 / -1;
  }

  .decision-facts {
    grid-template-columns: 1fr;
  }

  .overview-pipeline {
    grid-template-columns: repeat(4, minmax(132px, 1fr));
  }

  .sandbox-toolbar {
    grid-template-columns: 1fr;
  }

  .sandbox-toolbar .registry-copy {
    grid-column: auto;
  }

  .node-table,
  .sandbox-table,
  .registry-table {
    min-width: 0;
  }

  .node-table th:nth-child(4),
  .node-table td:nth-child(4),
  .node-table th:nth-child(5),
  .node-table td:nth-child(5),
  .node-table th:nth-child(6),
  .node-table td:nth-child(6),
  .sandbox-table th:nth-child(3),
  .sandbox-table td:nth-child(3),
  .sandbox-table th:nth-child(5),
  .sandbox-table td:nth-child(5),
  .sandbox-table th:nth-child(6),
  .sandbox-table td:nth-child(6),
  .sandbox-table th:nth-child(7),
  .sandbox-table td:nth-child(7),
  .registry-table th:nth-child(4),
  .registry-table td:nth-child(4),
  .registry-table th:nth-child(5),
  .registry-table td:nth-child(5) {
    display: none;
  }
}

@media (max-width: 480px) {
  .metric-grid,
  .latency-strip,
  .fleet-signal-grid,
  .node-hero-stats,
  .registry-stat-grid {
    grid-template-columns: 1fr;
  }

  .metric-card {
    min-height: 126px;
  }

  .overview-heading {
    align-items: flex-start;
    flex-direction: column;
  }
}

@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    scroll-behavior: auto !important;
    transition-duration: 0.01ms !important;
    animation-duration: 0.01ms !important;
    animation-iteration-count: 1 !important;
  }
}

@media (forced-colors: active) {
  .health-icon,
  .state-dot,
  .nav-badge,
  .inline-badge,
  .sandbox-status {
    forced-color-adjust: none;
    border: 1px solid CanvasText;
  }

  .dual-meter span {
    background: Highlight;
  }
}

/* Operations ledger visual system. */
:root {
  color-scheme: light;
  --app-bar-height: 60px;
  --rail-width: 0px;
  --background: #f4f5f2;
  --surface: #fbfcf9;
  --surface-soft: #f0f2ee;
  --surface-raised: #ffffff;
  --line: #cfd4ce;
  --line-soft: #e1e5df;
  --text: #171b1d;
  --muted: #687176;
  --faint: #8d969a;
  --blue: #226a78;
  --green: #217a61;
  --orange: #ae641c;
  --purple: #6754a3;
  --red: #b13f50;
  --amber: #8d6a17;
  --app-bar: #171b1d;
  --app-bar-line: #303638;
  --shadow: none;
  --radius: 0px;
}

:root.dark,
:root.dark-charts {
  color-scheme: dark;
  --background: #0c0f10;
  --surface: #111516;
  --surface-soft: #161b1d;
  --surface-raised: #1a1f21;
  --line: #303638;
  --line-soft: #24292b;
  --text: #edf1ed;
  --muted: #929c9f;
  --faint: #6f797c;
  --blue: #5ab0bd;
  --green: #52b99a;
  --orange: #d99a58;
  --purple: #9c8ad8;
  --red: #e06d7d;
  --amber: #c7a552;
  --app-bar: #0c0f10;
  --app-bar-line: #303638;
}

html {
  background: var(--background);
  font-size: 15px;
}

body {
  min-width: 320px;
  background: var(--background);
  color: var(--text);
  font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  letter-spacing: -0.006em;
}

button,
input,
select {
  border-radius: 2px;
  box-shadow: none;
  font: inherit;
}

button:focus-visible,
input:focus-visible,
select:focus-visible,
[role="tab"]:focus-visible,
summary:focus-visible {
  outline: 2px solid var(--blue);
  outline-offset: 2px;
}

.app-bar {
  position: fixed;
  inset: 0 0 auto 0;
  z-index: 60;
  height: var(--app-bar-height);
  padding: 0 22px;
  background: var(--app-bar);
  border-bottom: 1px solid var(--app-bar-line);
  color: #f4f7f4;
  box-shadow: none;
}

.brand {
  min-width: 0;
  gap: 11px;
}

.brand-mark {
  width: 17px;
  height: 17px;
  padding: 0;
  gap: 3px;
  background: transparent;
  border: 0;
}

.brand-mark span {
  width: 7px;
  height: 7px;
  border-radius: 1px;
  background: #a9b2b1;
}

.brand-mark span:nth-child(1) { background: #57bda0; }
.brand-mark span:nth-child(2) { background: #7aa7ae; }
.brand-mark span:nth-child(3) { background: #d49b5b; }
.brand-mark span:nth-child(4) { background: #879195; }

.brand-copy strong {
  font-size: 0.88rem;
  font-weight: 640;
  letter-spacing: -0.01em;
}

.brand-copy small {
  display: none;
}

.top-controls {
  gap: 6px;
}

.status-pill,
.select-control,
.icon-button,
.title-actions button,
.auth-panel button,
.table-action {
  min-height: 34px;
  border: 1px solid #343b3e;
  border-radius: 2px;
  background: #171c1e;
  color: #dce2df;
  box-shadow: none;
}

.status-pill {
  padding: 0 11px;
  text-transform: uppercase;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.075em;
}

.status-pill::before {
  width: 6px;
  height: 6px;
  border-radius: 50%;
}

.select-control {
  padding: 0 6px 0 9px;
}

.select-control select {
  min-height: 32px;
  padding-right: 22px;
  background: transparent;
  color: inherit;
  font-size: 0.78rem;
  font-weight: 600;
}

.icon-button {
  width: 34px;
  min-width: 34px;
  padding: 0;
}

.page-shell {
  display: block;
  width: min(100%, 1720px);
  min-height: 100vh;
  margin: 0 auto;
  padding: calc(var(--app-bar-height) + 20px) 28px 72px;
}

.page-title {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  min-height: 52px;
  margin: 0;
  padding: 0 0 14px;
  border: 0;
}

.page-title h1 {
  margin: 0;
  color: var(--text);
  font-size: clamp(1.65rem, 2.3vw, 2.35rem);
  font-weight: 580;
  letter-spacing: -0.04em;
  line-height: 1;
}

.page-kicker,
.page-title p {
  position: absolute !important;
  width: 1px !important;
  height: 1px !important;
  padding: 0 !important;
  margin: -1px !important;
  overflow: hidden !important;
  clip: rect(0, 0, 0, 0) !important;
  white-space: nowrap !important;
  border: 0 !important;
}

.title-actions {
  align-items: center;
  gap: 10px;
}

.last-updated {
  color: var(--faint);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.72rem;
  font-variant-numeric: tabular-nums;
}

.title-actions button,
.auth-panel button,
.table-action {
  padding: 0 12px;
  border-color: var(--line);
  background: transparent;
  color: var(--text);
  font-size: 0.76rem;
  font-weight: 650;
}

.title-actions button:hover,
.auth-panel button:hover,
.table-action:hover {
  background: var(--surface-soft);
}

.auth-panel {
  display: grid;
  grid-template-columns: minmax(260px, 1fr) auto auto;
  align-items: end;
  gap: 8px;
  margin: 0 0 14px;
  padding: 14px 0;
  border: 0;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
  border-radius: 0;
  background: transparent;
  box-shadow: none;
}

.auth-panel[hidden] {
  display: none;
}

.token-field span,
.sandbox-search > span,
.sandbox-filter > span,
.registry-search > span,
.registry-select > span {
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.token-field input,
.inline-search input,
.sandbox-search input,
.registry-search input,
.inline-select select,
.sandbox-filter select,
.registry-select select {
  min-height: 38px;
  border: 1px solid var(--line);
  border-radius: 2px;
  background: var(--surface);
  color: var(--text);
}

.page-tabs {
  position: sticky;
  top: var(--app-bar-height);
  left: auto;
  z-index: 50;
  display: flex;
  flex-direction: row;
  align-items: stretch;
  width: auto;
  height: 43px;
  margin: 0 -28px 26px;
  padding: 0 28px;
  gap: 26px;
  overflow-x: auto;
  overflow-y: hidden;
  background: color-mix(in srgb, var(--background) 94%, transparent);
  border: 0;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
  box-shadow: none;
  backdrop-filter: blur(10px);
}

.page-tabs::before,
.nav-section-label,
.nav-icon {
  display: none;
}

.page-tab {
  position: relative;
  display: flex;
  flex: 0 0 auto;
  align-items: center;
  min-height: 42px;
  margin: 0;
  padding: 0;
  gap: 8px;
  border: 0;
  border-radius: 0;
  background: transparent;
  color: var(--muted);
  box-shadow: none;
  font-size: 0.8rem;
  font-weight: 620;
}

.page-tab::after {
  content: "";
  position: absolute;
  right: 0;
  bottom: -1px;
  left: 0;
  height: 2px;
  background: transparent;
}

.page-tab:hover {
  background: transparent;
  color: var(--text);
}

.page-tab.is-active {
  border: 0;
  background: transparent;
  color: var(--text);
  box-shadow: none;
}

.page-tab.is-active::after {
  background: var(--text);
}

.nav-label {
  gap: 0;
}

.nav-badge,
.inline-badge,
.read-only-badge,
.sandbox-status,
.state-badge {
  min-width: 20px;
  padding: 2px 6px;
  border: 1px solid var(--line);
  border-radius: 999px;
  background: transparent;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.64rem;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  letter-spacing: 0;
}

.overview-page,
.workspace-page,
.sandboxes-page,
.registry-page {
  display: block;
  min-width: 0;
  margin: 0;
  padding: 0;
}

.overview-section,
.workspace-page > section,
.sandboxes-page > section,
.registry-page > section {
  margin-top: 0;
}

.command-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(400px, 0.75fr);
  gap: 0;
  margin-bottom: 34px;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.command-grid .health-strip,
.decision-brief,
.capacity-card,
.pipeline-card,
.metric-card,
.chart-panel,
.event-panel,
.workspace-card,
.ops-panel,
.flow-panel,
.decision-hero,
.workspace-hero,
.registry-hero,
.sandbox-hero,
.fleet-signal-grid article {
  border-radius: 0;
  background: transparent;
  box-shadow: none;
}

.command-grid .health-strip,
.decision-brief {
  min-height: 164px;
  padding: 22px 24px;
  border: 0;
}

.command-grid .health-strip {
  display: grid;
  grid-template-columns: minmax(260px, auto) 1fr auto;
  align-items: center;
  gap: 24px;
  border-right: 1px solid var(--line);
}

.health-strip::before,
.metric-card::before,
.resource-vector::before {
  display: none !important;
}

.health-primary {
  align-items: center;
  gap: 14px;
}

.health-primary > div {
  gap: 5px;
}

.health-icon {
  width: 10px;
  height: 10px;
  border: 1px solid currentColor;
  border-radius: 50%;
  box-shadow: none;
}

.health-primary strong {
  font-size: clamp(1.45rem, 2vw, 2rem);
  font-weight: 560;
  letter-spacing: -0.035em;
}

.health-primary #healthDetail {
  color: var(--muted);
  font-size: 0.76rem;
}

.health-signals {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  gap: 7px;
  flex-wrap: wrap;
}

.health-actions {
  align-self: center;
}

.eyebrow,
.metric-label,
.headroom-label > span,
.pipeline-stage > span,
.stat-box > span,
.resource-vector > span,
th {
  color: var(--muted);
  font-size: 0.66rem;
  font-weight: 720;
  letter-spacing: 0.065em;
  text-transform: uppercase;
}

.decision-brief {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.decision-brief h2,
.panel-header h2,
.section-heading h2,
.workspace-hero h2,
.registry-hero h2,
.sandbox-hero h2,
.decision-hero h2 {
  margin: 0;
  color: var(--text);
  font-size: 1rem;
  font-weight: 630;
  letter-spacing: -0.02em;
}

.decision-summary,
.workspace-copy,
.registry-copy {
  margin: 8px 0 0;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.73rem;
  line-height: 1.35;
}

.decision-facts {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  margin: 18px 0 -22px;
  border-top: 1px solid var(--line-soft);
  background: transparent;
}

.decision-facts > div {
  min-width: 0;
  padding: 13px 12px 14px;
  border-right: 1px solid var(--line-soft);
}

.decision-brief .decision-summary {
  display: none;
}

.decision-facts > div:first-child { padding-left: 0; }
.decision-facts > div:last-child { border-right: 0; }

.decision-facts span {
  display: block;
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 0.63rem;
  font-weight: 680;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.decision-facts strong,
.metric-card strong,
.stat-box strong,
.fleet-signal-grid strong,
.pipeline-stage strong,
.latency-strip strong,
.headroom-label strong,
.resource-vector strong {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-variant-numeric: tabular-nums slashed-zero;
}

.section-heading {
  display: flex;
  align-items: end;
  justify-content: space-between;
  margin: 0;
  padding: 0;
}

.overview-heading {
  min-height: 38px;
  margin: 0;
  padding-bottom: 10px;
  border-bottom: 1px solid var(--line);
}

.overview-heading .eyebrow {
  display: none;
}

.overview-heading h2 {
  font-size: 0.78rem;
  letter-spacing: 0.055em;
  text-transform: uppercase;
}

.section-summary {
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.7rem;
  font-variant-numeric: tabular-nums;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr));
  gap: 0;
  margin: 0 0 34px;
  border-bottom: 1px solid var(--line);
}

.metric-card {
  min-height: 112px;
  padding: 17px 18px 15px;
  border: 0;
  border-right: 1px solid var(--line);
}

.metric-card:last-child {
  border-right: 0;
}

.metric-card strong {
  display: block;
  margin: 10px 0 9px;
  color: var(--text);
  font-size: clamp(1.45rem, 2vw, 2.2rem);
  font-weight: 540;
  letter-spacing: -0.045em;
  line-height: 1;
}

.metric-detail {
  display: block;
  overflow: hidden;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.66rem;
  min-height: 0;
  margin: 0;
  line-height: 1.2;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.metric-context {
  display: none;
}

.overview-workbench {
  display: grid;
  grid-template-columns: minmax(360px, 0.8fr) minmax(600px, 1.2fr);
  gap: 0;
  margin: 0 0 34px;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.capacity-card,
.pipeline-card {
  padding: 22px 24px;
  border: 0;
}

.capacity-card {
  border-right: 1px solid var(--line);
}

.capacity-card .workspace-copy {
  min-height: 18px;
}

.headroom-list {
  gap: 0;
  margin: 11px 0 0;
}

.headroom-row {
  display: grid;
  grid-template-columns: 116px 1fr 132px;
  align-items: center;
  gap: 12px;
  min-height: 43px;
  border-top: 1px solid var(--line-soft);
}

.headroom-row:last-child {
  border-bottom: 1px solid var(--line-soft);
}

.headroom-label {
  display: flex;
  grid-column: 1;
  align-items: baseline;
  justify-content: space-between;
  gap: 8px;
}

.headroom-label strong {
  font-size: 0.78rem;
  font-weight: 600;
}

.dual-meter {
  position: relative;
  grid-column: 2;
  height: 8px;
  overflow: hidden;
  border: 1px solid var(--line);
  border-radius: 0;
  background: transparent;
}

.dual-meter span {
  border-radius: 0;
}

.headroom-detail {
  grid-column: 3;
  overflow: hidden;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.64rem;
  text-align: right;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.capacity-rule {
  display: none;
}

.overview-pipeline {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0;
  margin-top: 18px;
  overflow: hidden;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.pipeline-stage {
  min-width: 0;
  min-height: 112px;
  padding: 15px 16px;
  border: 0;
  border-right: 1px solid var(--line);
  border-radius: 0;
  background: transparent !important;
}

.pipeline-stage:last-of-type {
  border-right: 0;
}

.pipeline-stage strong {
  display: block;
  margin: 14px 0 10px;
  color: var(--text) !important;
  font-size: 1.8rem;
  font-weight: 520;
  letter-spacing: -0.04em;
}

.pipeline-stage small {
  display: block;
  overflow: hidden;
  color: var(--muted) !important;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.64rem;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.pipeline-connector {
  display: none;
}

.latency-strip,
.stat-strip {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 0;
  border-bottom: 1px solid var(--line-soft);
}

.latency-strip > div,
.stat-strip .stat-box {
  padding: 12px 12px 13px;
  border: 0;
  border-right: 1px solid var(--line-soft);
  background: transparent;
}

.latency-strip > div:first-child,
.stat-strip .stat-box:first-child { padding-left: 0; }
.latency-strip > div:last-child,
.stat-strip .stat-box:last-child { border-right: 0; }

.latency-strip span {
  display: block;
  margin-bottom: 5px;
  color: var(--muted);
  font-size: 0.61rem;
  font-weight: 680;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

.trend-grid {
  display: grid;
  grid-template-columns: repeat(12, minmax(0, 1fr));
  gap: 0;
  margin: 0 0 34px;
  border-bottom: 1px solid var(--line);
}

.trend-grid .chart-panel {
  min-width: 0;
  padding: 17px 18px 12px;
  border: 0;
  border-right: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.trend-supply,
.trend-demand {
  grid-column: span 6;
}

.trend-pressure {
  grid-column: span 3;
}

.trend-latency {
  grid-column: span 6;
}

.trend-grid .chart-panel:nth-child(2),
.trend-grid .chart-panel:last-child {
  border-right: 0;
}

.trend-grid .panel-header {
  min-height: 26px;
  margin-bottom: 4px;
}

.trend-grid .panel-header h2 {
  font-size: 0.76rem;
  font-weight: 680;
  letter-spacing: 0.045em;
  text-transform: uppercase;
}

.trend-grid .panel-header > div > span,
.info-dot {
  display: none;
}

.chart-canvas,
.trend-grid .chart-canvas,
.trend-grid .chart-canvas.small {
  min-height: 178px;
  max-height: 178px;
  margin-top: 0;
}

.legend {
  display: flex;
  gap: 15px;
  min-height: 18px;
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.62rem;
}

.swatch {
  width: 13px;
  height: 2px;
  border-radius: 0;
}

.activity-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(260px, 0.42fr);
  gap: 0;
  border-bottom: 1px solid var(--line);
}

.activity-event-panel {
  grid-row: span 2;
  border-right: 1px solid var(--line) !important;
}

.activity-grid .event-panel,
.event-panel,
.workspace-card,
.ops-panel {
  overflow: hidden;
  border: 0;
  border-radius: 0;
  background: transparent;
  box-shadow: none;
}

.activity-grid .diagnostic-disclosure {
  border-bottom: 1px solid var(--line-soft);
}

.panel-header,
.table-header,
.diagnostic-disclosure summary {
  min-height: 48px;
  padding: 0 16px;
  border-bottom: 1px solid var(--line-soft);
}

.diagnostic-disclosure summary {
  display: flex;
  align-items: center;
  justify-content: space-between;
  cursor: pointer;
  font-size: 0.76rem;
  font-weight: 650;
}

.diagnostic-disclosure summary::marker {
  color: var(--muted);
}

.table-wrap {
  border: 0;
  border-radius: 0;
  background: transparent;
}

table {
  width: 100%;
  border-collapse: collapse;
  background: transparent;
  font-variant-numeric: tabular-nums;
}

th,
td {
  height: 42px;
  padding: 8px 13px;
  border-bottom: 1px solid var(--line-soft);
  text-align: left;
}

th {
  background: var(--surface-soft);
}

td {
  color: var(--text);
  font-size: 0.75rem;
}

tbody tr:hover {
  background: color-mix(in srgb, var(--blue) 7%, transparent);
}

.empty-cell {
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  text-align: center;
}

.decision-hero,
.workspace-hero,
.registry-hero,
.sandbox-hero {
  display: grid;
  grid-template-columns: minmax(300px, 0.7fr) minmax(520px, 1.3fr);
  align-items: stretch;
  gap: 0;
  margin: 0 0 28px;
  padding: 0;
  border: 0;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.decision-copy,
.workspace-hero > div:first-child,
.registry-hero-main,
.sandbox-hero-main {
  min-width: 0;
  padding: 22px 24px;
  border-right: 1px solid var(--line);
}

.decision-stats,
.node-hero-stats,
.registry-stat-grid,
.sandbox-stat-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 0;
}

.node-hero-stats,
.registry-stat-grid {
  grid-template-columns: repeat(5, minmax(0, 1fr));
}

.stat-box {
  min-width: 0;
  padding: 18px 16px;
  border: 0;
  border-right: 1px solid var(--line-soft);
  border-radius: 0;
  background: transparent;
}

.stat-box:last-child {
  border-right: 0;
}

.stat-box strong {
  display: block;
  margin-top: 12px;
  color: var(--text);
  font-size: 1.45rem;
  font-weight: 520;
  letter-spacing: -0.04em;
}

.flow-panel,
.capacity-equation-card,
.policy-card,
.queue-panel,
.image-supply-grid > *,
.registry-full-grid > *,
.registry-builds-panel,
.sandbox-list-panel,
.nodes-page .event-panel,
#nodesPage > .event-panel {
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.flow-panel {
  padding: 20px 0 0;
  margin-bottom: 28px;
  border-right: 0;
  border-left: 0;
}

.flow-panel > .section-heading,
.capacity-equation-card > .section-heading,
.policy-card > .section-heading {
  padding: 0 18px 16px;
}

.program-flow {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 0;
  border-top: 1px solid var(--line-soft);
}

.flow-stage {
  min-height: 92px;
  padding: 14px 16px;
  border: 0;
  border-right: 1px solid var(--line-soft);
  border-radius: 0;
  background: transparent;
  color: var(--text);
}

.flow-stage:last-of-type {
  border-right: 0;
}

.flow-stage.is-selected {
  background: var(--surface-soft);
  box-shadow: inset 0 -2px var(--text);
}

.flow-arrow {
  display: none;
}

.scheduler-grid,
.image-supply-grid,
.registry-full-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.55fr);
  gap: 0;
  margin-bottom: 28px;
}

.scheduler-grid > :first-child,
.image-supply-grid > :first-child,
.registry-full-grid > :first-child {
  border-right: 1px solid var(--line);
}

.capacity-equation-card,
.policy-card,
.builder-service,
.image-queue-summary {
  padding: 20px 0 0;
}

.policy-values,
.resource-vector-list {
  margin: 0;
  border-top: 1px solid var(--line-soft);
}

.policy-values > div,
.resource-vector {
  min-height: 43px;
  padding: 9px 16px;
  border-bottom: 1px solid var(--line-soft);
  background: transparent;
}

.policy-note {
  display: none;
}

.equation-footnote {
  display: flex;
  justify-content: flex-end;
  padding: 10px 14px;
  border-top: 1px solid var(--line-soft);
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.65rem;
}

.queue-toolbar,
.node-toolbar,
.sandbox-toolbar,
.registry-toolbar {
  display: grid;
  align-items: end;
  gap: 8px;
  padding: 12px 0;
  border: 0;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
  border-radius: 0;
  background: transparent;
  box-shadow: none;
}

.queue-toolbar,
.node-toolbar {
  grid-template-columns: minmax(220px, 1fr) minmax(200px, 0.7fr) minmax(170px, 0.45fr) auto;
}

.sandbox-toolbar,
.registry-toolbar {
  grid-template-columns: minmax(280px, 1fr) minmax(180px, 0.4fr) auto auto;
}

.fleet-signal-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 0;
  margin-bottom: 28px;
  border-top: 1px solid var(--line);
  border-bottom: 1px solid var(--line);
}

.fleet-signal-grid article {
  min-height: 105px;
  padding: 16px;
  border: 0;
  border-right: 1px solid var(--line);
}

.fleet-signal-grid article:last-child {
  border-right: 0;
}

.fleet-signal-grid strong {
  display: block;
  margin: 11px 0 7px;
  font-size: 1.2rem;
  font-weight: 540;
}

.fleet-signal-grid small {
  color: var(--muted);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.63rem;
}

.builder-service > .panel-header,
.image-queue-summary > .section-heading {
  padding: 0 18px 16px;
}

.builder-service .stat-strip {
  grid-template-columns: repeat(5, 1fr);
  border-top: 1px solid var(--line-soft);
}

.builder-service .chart-canvas {
  padding: 0 12px;
}

.toast-region {
  right: 18px;
  bottom: 18px;
}

.toast {
  border: 1px solid var(--line);
  border-radius: 2px;
  background: var(--surface-raised);
  box-shadow: none;
}

@media (max-width: 1180px) {
  .metric-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .metric-card:nth-child(3) {
    border-right: 0;
  }

  .metric-card:nth-child(-n + 3) {
    border-bottom: 1px solid var(--line);
  }

  .overview-workbench,
  .command-grid {
    grid-template-columns: minmax(0, 1fr);
  }

  .command-grid .health-strip,
  .capacity-card {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .trend-pressure {
    grid-column: span 6;
  }

  .trend-grid .chart-panel:nth-child(3) {
    border-right: 1px solid var(--line);
  }

  .decision-hero,
  .workspace-hero,
  .registry-hero,
  .sandbox-hero {
    grid-template-columns: 1fr;
  }

  .decision-copy,
  .workspace-hero > div:first-child,
  .registry-hero-main,
  .sandbox-hero-main {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }
}

@media (max-width: 820px) {
  .app-bar {
    padding: 0 14px;
  }

  .status-pill {
    display: none;
  }

  .page-shell {
    padding-right: 16px;
    padding-left: 16px;
  }

  .page-tabs {
    margin-right: -16px;
    margin-left: -16px;
    padding: 0 16px;
    gap: 22px;
  }

  .command-grid .health-strip {
    grid-template-columns: 1fr auto;
  }

  .health-signals {
    grid-column: 1 / -1;
    grid-row: 2;
  }

  .overview-pipeline {
    grid-template-columns: repeat(4, minmax(140px, 1fr));
    overflow-x: auto;
  }

  .activity-grid,
  .scheduler-grid,
  .image-supply-grid,
  .registry-full-grid {
    grid-template-columns: 1fr;
  }

  .activity-event-panel,
  .scheduler-grid > :first-child,
  .image-supply-grid > :first-child,
  .registry-full-grid > :first-child {
    border-right: 0 !important;
    border-bottom: 1px solid var(--line) !important;
  }

  .fleet-signal-grid,
  .node-hero-stats,
  .registry-stat-grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .decision-stats,
  .sandbox-stat-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .queue-toolbar,
  .node-toolbar,
  .sandbox-toolbar,
  .registry-toolbar {
    grid-template-columns: 1fr 1fr;
  }
}

@media (max-width: 560px) {
  .brand-copy strong {
    font-size: 0.78rem;
  }

  .select-control:first-of-type,
  #pauseButton {
    display: none;
  }

  .page-title {
    align-items: flex-start;
    gap: 10px;
  }

  .last-updated {
    display: none;
  }

  .metric-grid,
  .fleet-signal-grid,
  .node-hero-stats,
  .registry-stat-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .command-grid .health-strip {
    grid-template-columns: minmax(0, 1fr);
  }

  .health-actions,
  .health-signals {
    grid-column: 1;
    grid-row: auto;
  }

  .metric-card:nth-child(3) {
    border-right: 1px solid var(--line);
  }

  .metric-card:nth-child(2n) {
    border-right: 0;
  }

  .metric-card:nth-child(-n + 4) {
    border-bottom: 1px solid var(--line);
  }

  .headroom-row {
    grid-template-columns: 92px 1fr;
  }

  .headroom-detail {
    display: none;
  }

  .latency-strip {
    grid-template-columns: repeat(2, 1fr);
  }

  .trend-supply,
  .trend-demand,
  .trend-pressure,
  .trend-latency {
    grid-column: 1 / -1;
  }

  .trend-grid .chart-panel {
    border-right: 0 !important;
  }

  .queue-toolbar,
  .node-toolbar,
  .sandbox-toolbar,
  .registry-toolbar,
  .auth-panel {
    grid-template-columns: 1fr;
  }

  .program-flow {
    grid-template-columns: repeat(5, minmax(112px, 1fr));
    overflow-x: auto;
  }

  .overview-workbench,
  .pipeline-card,
  .capacity-card {
    min-width: 0;
  }

  .overview-pipeline {
    max-width: 100%;
    overflow-x: auto;
  }
}

.app-bar .brand {
  width: auto;
  min-width: max-content;
}

.app-bar .brand-copy {
  display: block;
  width: auto;
}

.page-tabs .page-tab {
  width: auto;
  min-width: 0;
  justify-content: flex-start;
}

.page-tabs .nav-icon {
  display: none;
}

.page-tabs .nav-badge {
  margin-left: 0;
}

.capacity-equation-table thead th,
.capacity-equation-table tbody th,
.capacity-equation-table td,
.node-table th,
.sandbox-table th,
.registry-table th,
.program-table th,
.event-panel th {
  background: transparent;
}

.capacity-equation-table thead th,
.node-table thead th,
.sandbox-table thead th,
.registry-table thead th,
.program-table thead th,
.event-panel thead th {
  background: var(--surface-soft);
}

.decision-facts > div {
  border-radius: 0;
  background: transparent;
}

#healthDetail,
.capacity-card > .workspace-copy,
.decision-copy > .workspace-copy,
.workspace-hero > div:first-child > .workspace-copy,
.registry-hero-main > .registry-copy,
.sandbox-hero-main > .registry-copy {
  display: none;
}

/* Desktop density pass. */
:root {
  --app-bar-height: 50px;
}

.app-bar {
  padding: 0 18px;
}

.app-context {
  display: flex;
  align-items: center;
  align-self: stretch;
  min-width: 0;
  margin: 0 auto 0 18px;
  padding-left: 18px;
  border-left: 1px solid var(--app-bar-line);
}

.app-context h1 {
  margin: 0;
  color: #f4f7f4;
  font-size: 0.9rem;
  font-weight: 620;
  letter-spacing: -0.015em;
}

.top-controls #authToggleButton {
  min-height: 32px;
  padding: 0 10px;
  border: 1px solid #343b3e;
  border-radius: 2px;
  background: #171c1e;
  color: #dce2df;
  font-size: 0.72rem;
  font-weight: 650;
}

.top-controls .last-updated {
  max-width: 150px;
  overflow: hidden;
  color: #8e989a;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.page-shell {
  padding: var(--app-bar-height) 20px 48px;
}

.page-tabs {
  height: 38px;
  margin: 0 -20px 16px;
  padding: 0 20px;
  gap: 24px;
}

.page-tab {
  min-height: 37px;
}

.command-grid {
  margin-bottom: 20px;
}

.command-grid .health-strip,
.decision-brief {
  min-height: 126px;
  padding: 16px 18px;
}

.health-primary strong {
  font-size: clamp(1.3rem, 1.7vw, 1.7rem);
}

.decision-facts {
  margin: 11px 0 -16px;
}

.decision-facts > div {
  padding-top: 9px;
  padding-bottom: 9px;
}

.overview-heading {
  min-height: 30px;
  padding-bottom: 7px;
}

.metric-grid {
  margin-bottom: 20px;
}

.metric-card {
  min-height: 88px;
  padding: 12px 14px 11px;
}

.metric-card strong {
  margin: 7px 0 7px;
  font-size: clamp(1.35rem, 1.75vw, 1.85rem);
}

.metric-detail {
  display: -webkit-box;
  overflow: hidden;
  line-height: 1.25;
  text-overflow: clip;
  white-space: normal;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 2;
}

.overview-workbench {
  margin-bottom: 20px;
}

.capacity-card,
.pipeline-card {
  padding: 15px 18px;
}

.headroom-list,
.overview-pipeline {
  margin-top: 11px;
}

.headroom-row {
  grid-template-columns: 145px minmax(110px, 1fr) 175px;
  gap: 10px;
  min-height: 35px;
}

.headroom-label span,
.headroom-label strong {
  white-space: nowrap;
}

.hard-limit .headroom-label span::after {
  display: none;
}

.pipeline-stage {
  min-height: 86px;
  padding: 11px 13px;
}

.pipeline-stage strong {
  margin: 9px 0 7px;
  font-size: 1.55rem;
}

.pipeline-stage > span {
  min-height: 0;
  line-height: 1.15;
}

.latency-strip {
  margin-top: 8px;
  border: 0;
  border-top: 1px solid var(--line-soft);
  border-bottom: 1px solid var(--line-soft);
  border-radius: 0;
  background: transparent;
}

.latency-strip > div,
.stat-strip .stat-box {
  padding-top: 8px;
  padding-bottom: 9px;
  background: transparent;
}

.trend-grid {
  margin-bottom: 20px;
}

.trend-grid .chart-panel {
  padding: 13px 14px 9px;
}

.chart-canvas,
.trend-grid .chart-canvas,
.trend-grid .chart-canvas.small {
  min-height: 148px;
  max-height: 148px;
}

@media (max-width: 820px) {
  .app-bar {
    padding: 0 12px;
  }

  .brand-copy,
  .top-controls .last-updated {
    display: none;
  }

  .app-context {
    margin-left: 8px;
    padding-left: 8px;
  }

  .page-shell {
    padding-right: 14px;
    padding-left: 14px;
  }

  .page-tabs {
    margin-right: -14px;
    margin-left: -14px;
    padding-right: 14px;
    padding-left: 14px;
  }
}

/* Coherent visual system: calm hierarchy, exact alignment, semantic color. */
:root {
  color-scheme: light;
  --app-bar-height: 56px;
  --background: #f3f5f8;
  --surface: #ffffff;
  --surface-soft: #f7f8fa;
  --surface-raised: #ffffff;
  --line: #d9dee7;
  --line-soft: #e8ebf0;
  --text: #171a21;
  --muted: #697386;
  --faint: #929bab;
  --blue: #4d6ee8;
  --green: #23866f;
  --orange: #a96424;
  --purple: #7058b3;
  --red: #c94b5d;
  --amber: #9a721e;
  --app-bar: #11141b;
  --app-bar-line: #282d38;
  --focus-ring: rgba(77, 110, 232, 0.32);
  --selection: #4d6ee8;
}

:root.dark,
:root.dark-charts {
  color-scheme: dark;
  --background: #0c0f15;
  --surface: #11151d;
  --surface-soft: #151a24;
  --surface-raised: #191f2a;
  --line: #29313e;
  --line-soft: #202733;
  --text: #f0f2f6;
  --muted: #9aa5b5;
  --faint: #707b8d;
  --blue: #809bff;
  --green: #5cc7a7;
  --orange: #dfa568;
  --purple: #ad99ec;
  --red: #f07d8c;
  --amber: #d6b462;
  --app-bar: #0a0d12;
  --app-bar-line: #242b36;
  --focus-ring: rgba(128, 155, 255, 0.34);
  --selection: #809bff;
}

html {
  background: var(--background);
  font-size: 15px;
}

body {
  background: var(--background);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  font-size: 0.875rem;
  font-weight: 430;
  letter-spacing: 0;
  line-height: 1.45;
}

button,
input,
select {
  font-family: inherit;
  letter-spacing: 0;
}

button:focus-visible,
input:focus-visible,
select:focus-visible,
[role="tab"]:focus-visible,
summary:focus-visible {
  outline: 0;
  box-shadow: 0 0 0 3px var(--focus-ring);
}

h1,
h2,
h3,
strong,
th {
  letter-spacing: -0.012em;
}

.app-bar {
  height: var(--app-bar-height);
  padding: 0 24px;
  border-bottom: 1px solid var(--app-bar-line);
  background: var(--app-bar);
}

.brand {
  gap: 10px;
}

.brand-mark {
  width: 18px;
  height: 18px;
  gap: 2px;
}

.brand-mark span {
  width: 8px;
  height: 8px;
  border-radius: 2px;
  opacity: 0.95;
}

.brand-mark span:nth-child(1) { background: #69c5aa; }
.brand-mark span:nth-child(2) { background: #8da2ff; }
.brand-mark span:nth-child(3) { background: #e3ab69; }
.brand-mark span:nth-child(4) { background: #748092; }

.brand-copy strong {
  color: #f5f6f8;
  font-size: 0.88rem;
  font-weight: 650;
}

.app-context {
  margin-left: 20px;
  padding-left: 20px;
  border-color: var(--app-bar-line);
}

.app-context h1 {
  color: #c9d0da;
  font-size: 0.82rem;
  font-weight: 520;
}

.top-controls {
  gap: 8px;
}

.top-controls .last-updated {
  max-width: none;
  color: #8d98a8;
  font-size: 0.73rem;
  font-variant-numeric: tabular-nums;
}

.status-pill,
.select-control,
.icon-button,
.top-controls #authToggleButton {
  min-height: 34px;
  border: 1px solid #303745;
  border-radius: 6px;
  background: #141922;
  color: #d6dbe3;
}

.status-pill {
  min-width: 80px;
  padding: 0 12px;
  font-size: 0.66rem;
  font-weight: 720;
  letter-spacing: 0.08em;
}

.status-pill::before {
  width: 6px;
  height: 6px;
}

.select-control {
  gap: 8px;
  padding: 0 8px 0 10px;
}

.select-control select {
  color: #d6dbe3;
  font-size: 0.75rem;
}

.select-icon {
  color: #8490a2;
}

.icon-button,
.top-controls #authToggleButton {
  padding: 0 10px;
  font-size: 0.74rem;
  white-space: nowrap;
}

.icon-button:hover,
.top-controls #authToggleButton:hover {
  border-color: #465165;
  background: #1a202b;
}

.page-shell {
  padding: var(--app-bar-height) 24px 64px;
}

.page-tabs {
  height: 46px;
  margin: 0 -24px 18px;
  padding: 0 24px;
  gap: 28px;
  border-bottom: 1px solid var(--line);
  background: color-mix(in srgb, var(--background) 94%, transparent);
}

.page-tab {
  position: relative;
  min-height: 45px;
  padding: 0;
  border: 0;
  color: var(--muted);
  font-size: 0.78rem;
  font-weight: 560;
}

.page-tab::after {
  position: absolute;
  inset: auto 0 -1px;
  height: 2px;
  border-radius: 2px 2px 0 0;
  background: transparent;
  content: "";
}

.page-tab:hover {
  color: var(--text);
}

.page-tab.is-active {
  color: var(--text);
  font-weight: 660;
}

.page-tab.is-active::after {
  background: var(--blue);
}

.nav-badge {
  min-width: 18px;
  height: 18px;
  padding: 0 5px;
  border: 0;
  border-radius: 5px;
  background: var(--surface-raised);
  color: var(--muted);
  font-size: 0.64rem;
  font-weight: 700;
}

.nav-badge-bad,
.nav-badge-warn {
  background: color-mix(in srgb, var(--red) 13%, var(--surface));
  color: var(--red);
}

.auth-panel {
  margin: 0 0 18px;
  padding: 14px 16px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--surface);
}

.auth-panel input,
.inline-search input,
.inline-select select,
.registry-toolbar input,
.registry-toolbar select,
.sandbox-toolbar input,
.sandbox-toolbar select,
.node-toolbar input,
.node-toolbar select,
.queue-toolbar input,
.queue-toolbar select {
  min-height: 38px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface-soft);
  color: var(--text);
}

.auth-panel button,
.title-actions button,
.table-action,
.health-actions button,
.sandbox-toolbar button {
  min-height: 34px;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface-raised);
  color: var(--text);
  font-size: 0.74rem;
  font-weight: 600;
}

.auth-panel button:hover,
.title-actions button:hover,
.health-actions button:hover,
.sandbox-toolbar button:hover {
  border-color: color-mix(in srgb, var(--blue) 45%, var(--line));
  background: color-mix(in srgb, var(--blue) 8%, var(--surface));
}

.eyebrow,
.metric-label,
.stat-box > span,
.fleet-signal-grid span,
.latency-strip span,
.decision-facts span,
.pipeline-stage > span,
.flow-index,
th {
  color: var(--muted);
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.075em;
  line-height: 1.2;
  text-transform: uppercase;
}

.section-heading h2,
.panel-header h2,
.workspace-card h2,
.event-panel h2 {
  color: var(--text);
  font-size: 0.92rem;
  font-weight: 650;
  letter-spacing: -0.015em;
}

.section-summary,
.workspace-copy,
.registry-copy,
.last-updated,
.metric-detail,
.headroom-detail,
.pipeline-stage small,
.latency-strip strong,
.decision-facts strong,
.stat-box strong {
  font-family: inherit;
  font-variant-numeric: tabular-nums;
}

small,
.legend,
.fleet-signal-grid small,
.stat-box small,
.meter-label,
.equation-footnote,
.policy-values,
.registry-page-url {
  font-family: inherit;
}

.section-summary {
  color: var(--muted);
  font-size: 0.72rem;
}

.command-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(430px, 0.9fr);
  gap: 0;
  margin: 0 0 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.command-grid .health-strip,
.decision-brief {
  min-height: 144px;
  padding: 20px 22px;
  border: 0;
  border-radius: 0;
  background: transparent;
}

.command-grid .health-strip {
  grid-template-columns: minmax(320px, 1.1fr) minmax(260px, 0.9fr) auto;
  gap: 18px;
}

.decision-brief {
  border-left: 1px solid var(--line);
  background: var(--surface-soft);
}

.health-primary {
  gap: 12px;
}

.health-icon {
  width: 9px;
  height: 9px;
  border: 0;
  border-radius: 50%;
  box-shadow: 0 0 0 5px color-mix(in srgb, currentColor 10%, transparent);
}

.health-primary strong {
  color: var(--text);
  font-size: clamp(1.3rem, 1.7vw, 1.65rem);
  font-weight: 620;
  letter-spacing: -0.025em;
}

.health-signals {
  gap: 6px;
}

.signal-chip,
.reason-chip,
.read-only-badge,
.inline-badge {
  min-height: 24px;
  padding: 3px 8px;
  border: 1px solid var(--line);
  border-radius: 5px;
  background: transparent;
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 620;
}

.signal-chip.warn,
.reason-chip.warn,
.badge-warn {
  border-color: color-mix(in srgb, var(--amber) 30%, var(--line));
  background: color-mix(in srgb, var(--amber) 8%, transparent);
  color: var(--amber);
}

.signal-chip.bad,
.badge-bad {
  border-color: color-mix(in srgb, var(--red) 32%, var(--line));
  background: color-mix(in srgb, var(--red) 8%, transparent);
  color: var(--red);
}

.badge-ok {
  border-color: color-mix(in srgb, var(--green) 30%, var(--line));
  background: color-mix(in srgb, var(--green) 8%, transparent);
  color: var(--green);
}

.decision-brief h2,
.decision-copy h2 {
  margin-top: 4px;
  font-size: 1.05rem;
  font-weight: 650;
}

.reason-stack,
.decision-reasons {
  gap: 5px;
  margin-top: 9px;
}

.reason-stack > span,
.decision-reasons > span {
  color: var(--muted);
  font-size: 0.73rem;
  line-height: 1.35;
}

.reason-stack > span::before,
.decision-reasons > span::before {
  width: 4px;
  height: 4px;
  margin-right: 8px;
  border-radius: 50%;
  background: var(--blue);
}

.decision-facts {
  margin: 13px -22px -20px;
  border-top: 1px solid var(--line);
}

.decision-facts > div {
  padding: 10px 12px;
  border-right: 1px solid var(--line);
}

.decision-facts strong {
  margin-top: 5px;
  color: var(--text);
  font-size: 0.84rem;
  font-weight: 630;
}

.overview-heading {
  min-height: 32px;
  padding: 0 0 8px;
  border: 0;
}

.overview-heading h2 {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.metric-grid {
  margin: 0 0 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.metric-card {
  min-height: 102px;
  padding: 15px 16px 14px;
  border-right: 1px solid var(--line);
  background: transparent;
}

.metric-card strong {
  margin: 8px 0 7px;
  color: var(--text);
  font-family: inherit;
  font-size: clamp(1.5rem, 1.9vw, 1.9rem);
  font-weight: 630;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.035em;
}

.metric-detail {
  color: var(--muted);
  font-size: 0.71rem;
  line-height: 1.3;
}

.overview-workbench {
  grid-template-columns: minmax(430px, 0.82fr) minmax(650px, 1.18fr);
  margin: 0 0 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.capacity-card,
.pipeline-card {
  padding: 18px 20px;
  background: transparent;
}

.capacity-card {
  border-right: 1px solid var(--line);
}

.headroom-list {
  margin-top: 13px;
}

.headroom-row {
  grid-template-columns: 142px minmax(100px, 1fr) 184px;
  min-height: 39px;
  gap: 12px;
  border-top: 1px solid var(--line-soft);
}

.headroom-label span {
  color: var(--muted);
  font-size: 0.68rem;
  font-weight: 650;
}

.headroom-label strong {
  color: var(--text);
  font-family: inherit;
  font-size: 0.76rem;
  font-weight: 640;
  font-variant-numeric: tabular-nums;
}

.dual-meter {
  height: 7px;
  border: 0;
  border-radius: 4px;
  background: var(--line-soft);
}

.dual-meter span {
  border-radius: 4px;
}

.meter-reserved {
  background: color-mix(in srgb, var(--blue) 30%, transparent);
}

.meter-actual {
  height: 5px;
  margin-top: 1px;
  background: var(--blue);
}

.meter-disk {
  background: var(--purple);
}

.headroom-detail {
  color: var(--muted);
  font-size: 0.66rem;
}

.overview-pipeline {
  margin-top: 13px;
  border-color: var(--line-soft);
}

.pipeline-stage {
  min-height: 88px;
  padding: 12px 14px;
  border-color: var(--line-soft);
}

.pipeline-stage strong {
  margin: 9px 0 6px;
  font-family: inherit;
  font-size: 1.55rem;
  font-weight: 630;
  font-variant-numeric: tabular-nums;
}

.pipeline-stage small {
  color: var(--muted) !important;
  font-size: 0.68rem;
}

.latency-strip {
  margin-top: 10px;
  border-color: var(--line-soft);
}

.latency-strip > div,
.stat-strip .stat-box {
  padding: 9px 12px 10px;
  border-color: var(--line-soft);
}

.latency-strip strong,
.stat-strip strong {
  color: var(--text);
  font-size: 0.84rem;
  font-weight: 630;
}

.trend-grid,
.activity-grid,
.scheduler-grid,
.image-supply-grid,
.registry-full-grid {
  gap: 0;
  margin-bottom: 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.trend-grid .chart-panel,
.activity-grid > *,
.scheduler-grid > *,
.image-supply-grid > *,
.registry-full-grid > * {
  border-color: var(--line);
  background: transparent;
}

.trend-grid .chart-panel {
  padding: 15px 16px 10px;
}

.panel-header,
.table-header {
  margin-bottom: 10px;
}

.chart-canvas,
.trend-grid .chart-canvas,
.trend-grid .chart-canvas.small {
  min-height: 148px;
  max-height: 148px;
}

.legend {
  gap: 16px;
  color: var(--muted);
  font-size: 0.68rem;
}

.legend span::before {
  width: 14px;
  height: 2px;
  border-radius: 1px;
}

.activity-event-panel,
.event-panel,
.ops-panel,
.workspace-card,
.chart-panel {
  border-radius: 0;
  box-shadow: none;
}

.activity-event-panel table {
  min-width: 720px;
}

.decision-hero,
.workspace-hero,
.registry-hero,
.sandbox-hero {
  margin-bottom: 20px;
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.decision-copy,
.workspace-hero > div:first-child,
.registry-hero-main,
.sandbox-hero-main {
  padding: 19px 22px;
  border-color: var(--line);
}

.decision-stats,
.node-hero-stats,
.registry-stat-grid,
.sandbox-stat-grid {
  background: var(--surface-soft);
}

.decision-stats > div,
.node-hero-stats > div,
.registry-stat-grid > div,
.sandbox-stat-grid > div,
.fleet-signal-grid > div {
  padding: 16px 18px;
  border-color: var(--line);
}

.decision-stats strong,
.node-hero-stats strong,
.registry-stat-grid strong,
.sandbox-stat-grid strong,
.fleet-signal-grid strong {
  color: var(--text);
  font-family: inherit;
  font-size: 1.35rem;
  font-weight: 630;
  font-variant-numeric: tabular-nums;
}

.fleet-signal-grid,
.stat-strip,
.compact-strip {
  border: 1px solid var(--line);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}

.program-flow {
  border: 1px solid var(--line);
  border-radius: 8px;
  overflow: hidden;
}

.flow-stage {
  min-height: 96px;
  padding: 14px 16px;
  border: 0;
  border-right: 1px solid var(--line);
  border-radius: 0;
  background: transparent;
  color: var(--text);
}

.flow-stage:hover {
  background: color-mix(in srgb, var(--blue) 5%, transparent);
}

.flow-stage.is-selected {
  background: color-mix(in srgb, var(--blue) 9%, transparent);
  box-shadow: inset 0 -2px var(--blue);
}

.flow-stage strong {
  margin: 9px 0 7px;
  font-family: inherit;
  font-size: 1.65rem;
  font-weight: 630;
}

.policy-values dt,
.policy-values dd,
.resource-vector,
.equation-footnote {
  font-family: inherit;
}

.policy-values > div {
  border-color: var(--line-soft);
}

.queue-toolbar,
.node-toolbar,
.sandbox-toolbar,
.registry-toolbar {
  padding: 13px 0 11px;
  border-color: var(--line);
  background: transparent;
}

.event-panel,
.sandbox-list-panel,
.registry-list-panel {
  border-color: var(--line);
  background: var(--surface);
}

.table-wrap {
  background: transparent;
  scrollbar-color: color-mix(in srgb, var(--muted) 38%, transparent) transparent;
  scrollbar-width: thin;
}

.table-wrap::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.table-wrap::-webkit-scrollbar-thumb {
  border: 2px solid transparent;
  border-radius: 8px;
  background: color-mix(in srgb, var(--muted) 36%, transparent);
  background-clip: padding-box;
}

table {
  color: var(--text);
  font-family: inherit;
}

th,
td {
  height: 42px;
  padding: 8px 12px;
  border-color: var(--line-soft);
}

th,
.capacity-equation-table thead th,
.node-table thead th,
.sandbox-table thead th,
.registry-table thead th,
.program-table thead th,
.event-panel thead th {
  background: var(--surface-soft);
  color: var(--muted);
}

td {
  color: color-mix(in srgb, var(--text) 92%, var(--muted));
  font-size: 0.74rem;
}

tbody tr:hover {
  background: color-mix(in srgb, var(--blue) 5%, transparent);
}

.sandbox-id,
.sandbox-image,
.sandbox-node,
.node-table td:nth-child(2),
.program-table td:nth-child(3),
.program-table td:nth-child(4) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 0.71rem;
  letter-spacing: -0.01em;
}

.sandbox-resources,
.sandbox-labels {
  font-family: inherit;
  font-size: 0.72rem;
}

.meter-label,
.meter-stack {
  font-family: inherit;
}

.meter {
  height: 6px;
  border: 0;
  border-radius: 4px;
  background: var(--line-soft);
}

.meter span {
  border-radius: 4px;
  background: var(--green);
}

.meter.warn span { background: var(--amber); }
.meter.bad span { background: var(--red); }

.state-dot {
  width: 7px;
  height: 7px;
  box-shadow: none;
}

.sandbox-status,
.build-status,
.severity-badge {
  min-width: 64px;
  height: 22px;
  border: 1px solid var(--line);
  border-radius: 5px;
  background: transparent;
  font-size: 0.66rem;
  font-weight: 650;
}

.sandbox-status.running,
.build-status.succeeded {
  border-color: color-mix(in srgb, var(--green) 35%, var(--line));
  background: color-mix(in srgb, var(--green) 9%, transparent);
  color: var(--green);
}

.sandbox-status.parked,
.sandbox-status.pending,
.build-status.queued {
  border-color: color-mix(in srgb, var(--blue) 30%, var(--line));
  background: color-mix(in srgb, var(--blue) 8%, transparent);
  color: var(--blue);
}

.sandbox-status.waking,
.sandbox-status.transitioning,
.sandbox-status.migrating,
.build-status.running,
.severity-warn {
  border-color: color-mix(in srgb, var(--amber) 32%, var(--line));
  background: color-mix(in srgb, var(--amber) 8%, transparent);
  color: var(--amber);
}

.sandbox-status.failed,
.sandbox-status.stale,
.build-status.failed,
.severity-alert {
  border-color: color-mix(in srgb, var(--red) 35%, var(--line));
  background: color-mix(in srgb, var(--red) 8%, transparent);
  color: var(--red);
}

.severity-info {
  border-color: color-mix(in srgb, var(--blue) 28%, var(--line));
  background: color-mix(in srgb, var(--blue) 7%, transparent);
  color: var(--blue);
}

.table-action.danger {
  border-color: color-mix(in srgb, var(--red) 35%, var(--line));
  background: transparent;
  color: var(--red);
}

.table-action.danger:hover:not(:disabled) {
  background: color-mix(in srgb, var(--red) 9%, transparent);
}

.tag-chip {
  border: 1px solid var(--line);
  border-radius: 5px;
  background: var(--surface-soft);
  color: var(--muted);
  font-family: inherit;
  font-size: 0.67rem;
}

@media (max-width: 1180px) {
  .command-grid {
    grid-template-columns: 1fr;
  }

  .decision-brief {
    border-top: 1px solid var(--line);
    border-left: 0;
  }

  .overview-workbench {
    grid-template-columns: 1fr;
  }

  .capacity-card {
    border-right: 0;
    border-bottom: 1px solid var(--line);
  }

  .trend-latency {
    grid-column: 1 / -1;
  }
}

@media (max-width: 1000px) {
  .scheduler-grid,
  .image-supply-grid,
  .registry-full-grid {
    grid-template-columns: 1fr;
  }

  .scheduler-grid > :first-child,
  .image-supply-grid > :first-child,
  .registry-full-grid > :first-child {
    border-right: 0 !important;
    border-bottom: 1px solid var(--line) !important;
  }
}

@media (max-width: 900px) {
  .app-context,
  .top-controls .last-updated {
    display: none;
  }

  .headroom-row {
    grid-template-columns: 142px minmax(100px, 1fr);
  }

  .headroom-detail {
    display: none;
  }
}

@media (max-width: 820px) {
  .app-bar {
    padding: 0 14px;
  }

  .page-shell {
    padding-right: 14px;
    padding-left: 14px;
  }

  .page-tabs {
    margin-right: -14px;
    margin-left: -14px;
    padding-right: 14px;
    padding-left: 14px;
    gap: 22px;
  }

  .command-grid .health-strip {
    grid-template-columns: 1fr;
  }

  .metric-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .metric-card:nth-child(2n) {
    border-right: 0;
  }

  .headroom-row {
    grid-template-columns: 130px minmax(100px, 1fr);
  }

  .headroom-detail {
    display: none;
  }
}
"""


DASHBOARD_JS = """
const MAX_HISTORY = 4320;
const DEFAULT_REFRESH_INTERVAL_MS = 2000;
const MAX_PROGRAM_ROWS = 200;
const MAX_NODE_ROWS = 250;
const MAX_REGISTRY_REPOSITORY_ROWS = 250;

const state = {
  timer: null,
  currentPage: "overview",
  history: [],
  lastSnapshot: null,
  lastSandboxes: [],
  sandboxFetchInFlight: false,
  sandboxActionInFlight: false,
  terminatingSandboxIds: new Set(),
  metricsRequest: null,
  requestSequence: 0,
  appliedSequence: 0,
  resizeFrame: null,
  lastSandboxRefreshAt: 0,
  programStateFilter: "all",
};

let palette = {
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
    "toastRegion",
    "pageKicker",
    "pageHeading",
    "pageDescription",
    "overviewNavBadge",
    "schedulerNavBadge",
    "nodesNavBadge",
    "sandboxesNavBadge",
    "registryNavBadge",
    "lastUpdated",
    "timeRangeSelect",
    "refreshNowButton",
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
    "readyWakeValue",
    "readyWakeDetail",
    "diskCommitValue",
    "diskCommitDetail",
    "modelWaitValue",
    "modelWaitDetail",
    "wakeLatencyValue",
    "wakeLatencyDetail",
    "overviewDecisionTitle",
    "overviewDecisionBadge",
    "overviewDecisionReasons",
    "overviewSupplyValue",
    "overviewProjectedValue",
    "overviewDeficitValue",
    "capacityFitBadge",
    "capacitySummary",
    "capacityCpuValue",
    "capacityCpuActualMeter",
    "capacityCpuReservedMeter",
    "capacityCpuDetail",
    "capacityMemoryValue",
    "capacityMemoryActualMeter",
    "capacityMemoryReservedMeter",
    "capacityMemoryDetail",
    "capacityDiskValue",
    "capacityDiskMeter",
    "capacityDiskDetail",
    "overviewModelWaitAge",
    "overviewWakingValue",
    "overviewActingValue",
    "overviewModelLatency",
    "builderSummary",
    "builderReadyValue",
    "builderPreparedValue",
    "builderActiveBuildsValue",
    "builderCpuValue",
    "builderMemoryValue",
    "autoscalerSummary",
    "autoscalerProvisioningValue",
    "autoscalerIdleGraceValue",
    "programSummary",
    "programModelWaitValue",
    "programReadyValue",
    "programOldestReadyValue",
    "programWakeLatencyValue",
    "healthBadge",
    "healthTitle",
    "healthDetail",
    "healthSignals",
    "copyDiagnosticsButton",
    "downloadSnapshotButton",
    "schedulerPage",
    "schedulerDecisionTitle",
    "schedulerModeBadge",
    "schedulerDecisionDetail",
    "schedulerReasons",
    "schedulerReadyNodesValue",
    "schedulerProvisioningValue",
    "schedulerWakePlanValue",
    "schedulerUnplacedValue",
    "programFlowSummary",
    "flowAllValue",
    "flowModelWaitValue",
    "flowModelWaitDetail",
    "flowReadyValue",
    "flowReadyDetail",
    "flowWakingValue",
    "flowActingValue",
    "decisionPressureValue",
    "decisionIdleGraceValue",
    "equationImmediateCpu",
    "equationImmediateMemory",
    "equationImmediateDisk",
    "equationReadyCpu",
    "equationReadyMemory",
    "equationReadyDisk",
    "equationPredictiveCpu",
    "equationPredictiveMemory",
    "equationPredictiveDisk",
    "equationPreparedCpu",
    "equationPreparedMemory",
    "equationPreparedDisk",
    "equationFreeCpu",
    "equationFreeMemory",
    "equationFreeDisk",
    "equationDeficitCpu",
    "equationDeficitMemory",
    "equationDeficitDisk",
    "policyValues",
    "programSearchInput",
    "programResultFilter",
    "programQueueSummary",
    "programQueueRows",
    "nodesPage",
    "nodesPageDetail",
    "nodesReadyValue",
    "nodesProvisioningValue",
    "nodesDrainingValue",
    "nodesIncompatibleValue",
    "nodesDiskFreeValue",
    "nodesCpuPressureValue",
    "nodesCpuPressureDetail",
    "nodesMemoryPressureValue",
    "nodesMemoryPressureDetail",
    "nodesPsiValue",
    "nodesStorageQueueValue",
    "nodesStorageQueueDetail",
    "nodesVolumeErrorsValue",
    "nodeSearchInput",
    "nodeStateFilter",
    "nodeTableSummary",
    "nodeRows",
    "sandboxesPage",
    "sandboxesPageStatusBadge",
    "sandboxesPageDetail",
    "sandboxesPageRowsValue",
    "sandboxesPageTerminableValue",
    "sandboxesPagePendingValue",
    "sandboxesPageRoutesValue",
    "sandboxSearchInput",
    "sandboxStateFilter",
    "refreshSandboxesButton",
    "sandboxesPageSummary",
    "sandboxListSummary",
    "sandboxRows",
    "registryPage",
    "registryPageStatusBadge",
    "registryPageUrl",
    "registryPageHealthDetail",
    "registryPageReposValue",
    "registryPageTagsValue",
    "registryPageVisibleTagsValue",
    "registryPageCoverageValue",
    "registryActiveBuildsValue",
    "registryFailedBuildsValue",
    "registryPendingBuildsValue",
    "registryOldestBuildValue",
    "registryActiveBuildsSummaryValue",
    "registryFailedBuildsSummaryValue",
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
  restoreSelectPreference(els.sandboxStateFilter, "ucloud.dashboard.sandboxState");
  restoreSelectPreference(els.nodeStateFilter, "ucloud.dashboard.nodeState");
  restoreSelectPreference(els.programResultFilter, "ucloud.dashboard.programResult");
  restoreSelectPreference(els.registryFilterSelect, "ucloud.dashboard.registryFilter");
  applyTheme(localStorage.getItem("ucloud.dashboard.theme") || preferredTheme());
  document.querySelectorAll("canvas").forEach((canvas) => {
    const heading = canvas.closest("article")?.querySelector("h2, .metric-label");
    const label = `${heading?.textContent?.trim() || "Metric"} session chart`;
    canvas.setAttribute("role", "img");
    canvas.setAttribute("aria-label", label);
    canvas.textContent = label;
  });
  document.querySelectorAll(".info-dot[title]").forEach((dot) => {
    dot.setAttribute("role", "note");
    dot.setAttribute("aria-label", dot.title);
  });
  syncAuthPanel(!savedToken);
  els.authToggleButton.addEventListener("click", () => syncAuthPanel(els.authPanel.hidden));
  els.saveTokenButton.addEventListener("click", saveToken);
  els.clearTokenButton.addEventListener("click", clearToken);
  els.timeRangeSelect.addEventListener("change", () => {
    trimHistory();
    redrawCharts();
  });
  els.refreshNowButton.addEventListener("click", () => refreshNow({ supersede: true }));
  els.themeButton.addEventListener("click", toggleTheme);
  els.copyDiagnosticsButton.addEventListener("click", copyDiagnostics);
  els.downloadSnapshotButton.addEventListener("click", downloadSnapshot);
  document.querySelectorAll("[data-page-target]").forEach((button) => {
    button.addEventListener("click", () => setPage(button.dataset.pageTarget || "overview"));
    button.addEventListener("keydown", handleTabKeydown);
  });
  window.addEventListener("hashchange", () => setPage(pageFromHash(), { updateHash: false }));
  els.sandboxSearchInput.addEventListener("input", renderSandboxesPage);
  els.sandboxStateFilter.addEventListener("change", () => {
    persistSelectPreference(els.sandboxStateFilter, "ucloud.dashboard.sandboxState");
    renderSandboxesPage();
  });
  els.refreshSandboxesButton.addEventListener("click", () => refreshSandboxes({ force: true }));
  els.registrySearchInput.addEventListener("input", () => renderRegistryPage(state.lastSnapshot || {}));
  els.registryFilterSelect.addEventListener("change", () => {
    persistSelectPreference(els.registryFilterSelect, "ucloud.dashboard.registryFilter");
    renderRegistryPage(state.lastSnapshot || {});
  });
  els.programSearchInput.addEventListener("input", renderProgramQueue);
  els.programResultFilter.addEventListener("change", () => {
    persistSelectPreference(els.programResultFilter, "ucloud.dashboard.programResult");
    renderProgramQueue();
  });
  els.nodeSearchInput.addEventListener("input", renderNodesPage);
  els.nodeStateFilter.addEventListener("change", () => {
    persistSelectPreference(els.nodeStateFilter, "ucloud.dashboard.nodeState");
    renderNodesPage();
  });
  document.querySelectorAll("[data-program-state]").forEach((button) => {
    button.addEventListener("click", () => {
      state.programStateFilter = button.dataset.programState || "all";
      document.querySelectorAll("[data-program-state]").forEach((item) => {
        const selected = item === button;
        item.classList.toggle("is-selected", selected);
        item.setAttribute("aria-pressed", String(selected));
      });
      renderProgramQueue();
    });
  });
  window.addEventListener("resize", scheduleChartRedraw);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      clearRefreshTimer();
      return;
    }
    refreshNow({ supersede: true });
    scheduleNextRefresh();
  });
  setPage(pageFromHash(), { updateHash: false });
  refreshNow();
});

function saveToken() {
  const token = els.tokenInput.value.trim();
  if (token) {
    sessionStorage.setItem("ucloud.dashboard.token", token);
    syncAuthPanel(false);
    setStatus("Saved", "ok");
    refreshNow({ supersede: true });
    return;
  }
  clearToken();
}

function restoreSelectPreference(select, key) {
  if (!select) return;
  const saved = localStorage.getItem(key);
  if (saved !== null && [...select.options].some((option) => option.value === saved)) {
    select.value = saved;
  }
}

function persistSelectPreference(select, key) {
  if (select) localStorage.setItem(key, select.value);
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

function preferredTheme() {
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function toggleTheme() {
  applyTheme(document.documentElement.classList.contains("dark") ? "light" : "dark");
  redrawCharts();
}

function applyTheme(theme) {
  const dark = theme === "dark";
  document.documentElement.classList.toggle("dark", dark);
  localStorage.setItem("ucloud.dashboard.theme", dark ? "dark" : "light");
  els.themeButton.title = dark ? "Use light theme" : "Use dark theme";
  els.themeButton.setAttribute("aria-label", els.themeButton.title);
  palette = dark
    ? {
        ...palette,
        blue: "#809bff",
        blueSoft: "rgba(128, 155, 255, 0.10)",
        green: "#5cc7a7",
        greenSoft: "rgba(92, 199, 167, 0.10)",
        orange: "#dfa568",
        orangeSoft: "rgba(223, 165, 104, 0.10)",
        purple: "#ad99ec",
        purpleSoft: "rgba(173, 153, 236, 0.10)",
        red: "#f07d8c",
        redSoft: "rgba(240, 125, 140, 0.10)",
        grid: "#29313e",
        text: "#f0f2f6",
        muted: "#9aa5b5",
        plotBg: "#11151d",
      }
    : {
        ...palette,
        blue: "#4d6ee8",
        blueSoft: "rgba(77, 110, 232, 0.08)",
        green: "#23866f",
        greenSoft: "rgba(35, 134, 111, 0.08)",
        orange: "#a96424",
        orangeSoft: "rgba(169, 100, 36, 0.08)",
        purple: "#7058b3",
        purpleSoft: "rgba(112, 88, 179, 0.08)",
        red: "#c94b5d",
        redSoft: "rgba(201, 75, 93, 0.08)",
        grid: "#d9dee7",
        text: "#171a21",
        muted: "#697386",
        plotBg: "#ffffff",
      };
}

function pageFromHash() {
  const page = window.location.hash.replace(/^#/, "");
  return ["overview", "scheduler", "nodes", "sandboxes", "registry"].includes(page) ? page : "overview";
}

function setPage(page, options = {}) {
  const next = ["overview", "scheduler", "nodes", "sandboxes", "registry"].includes(page) ? page : "overview";
  const pageCopy = {
    overview: ["Control plane", "Overview", "Overview"],
    scheduler: ["Control plane", "Demand & scaling", "Demand & scaling"],
    nodes: ["Control plane", "Fleet", "Fleet"],
    sandboxes: ["Control plane", "Sandboxes", "Sandboxes"],
    registry: ["Control plane", "Images", "Images"],
  };
  state.currentPage = next;
  const [kicker, heading, description] = pageCopy[next];
  setText("pageKicker", kicker);
  setText("pageHeading", heading);
  setText("pageDescription", description);
  const overviewPage = document.getElementById("overviewPage");
  if (overviewPage) overviewPage.hidden = next !== "overview";
  if (els.schedulerPage) els.schedulerPage.hidden = next !== "scheduler";
  if (els.nodesPage) els.nodesPage.hidden = next !== "nodes";
  if (els.sandboxesPage) {
    els.sandboxesPage.hidden = next !== "sandboxes";
  }
  if (els.registryPage) {
    els.registryPage.hidden = next !== "registry";
  }
  document.querySelectorAll("[data-page-target]").forEach((button) => {
    const active = button.dataset.pageTarget === next;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", String(active));
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
    button.tabIndex = active ? 0 : -1;
  });
  if (options.updateHash !== false) {
    const hash = `#${next}`;
    if (window.location.hash !== hash) {
      window.history.replaceState(null, "", hash);
    }
  }
  if (next === "overview") {
    redrawCharts();
    renderOverviewDetail(state.lastSnapshot || {});
  } else if (next === "scheduler") {
    renderSchedulerPage(state.lastSnapshot || {});
  } else if (next === "nodes") {
    renderNodesPage();
  } else if (next === "registry") {
    renderRegistryPage(state.lastSnapshot || {});
  } else {
    renderSandboxesPage();
    refreshSandboxes({ force: true, quiet: true });
  }
}

function handleTabKeydown(event) {
  if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)) return;
  const tabs = [...document.querySelectorAll("[data-page-target]")];
  const current = tabs.indexOf(event.currentTarget);
  let index = current;
  if (event.key === "Home") index = 0;
  else if (event.key === "End") index = tabs.length - 1;
  else index = (current + (["ArrowRight", "ArrowDown"].includes(event.key) ? 1 : -1) + tabs.length) % tabs.length;
  event.preventDefault();
  tabs[index].focus();
  setPage(tabs[index].dataset.pageTarget || "overview");
}

function clearRefreshTimer() {
  if (state.timer !== null) {
    window.clearTimeout(state.timer);
    state.timer = null;
  }
}

function scheduleNextRefresh() {
  clearRefreshTimer();
  if (document.hidden) return;
  state.timer = window.setTimeout(() => refreshNow(), DEFAULT_REFRESH_INTERVAL_MS);
}

async function refreshNow(options = {}) {
  if (state.metricsRequest) {
    if (!options.supersede) return;
    state.metricsRequest.abort();
  }
  clearRefreshTimer();
  const controller = new AbortController();
  const sequence = ++state.requestSequence;
  state.metricsRequest = controller;
  els.refreshNowButton.disabled = true;
  if (!state.lastSnapshot) setStatus("Connecting", "warn");
  const token = sessionStorage.getItem("ucloud.dashboard.token") || els.tokenInput.value.trim();
  const headers = token ? { "X-UCloud-Sandbox-Token": token } : {};
  try {
    const response = await fetch("/v1/metrics", {
      headers,
      cache: "no-store",
      signal: controller.signal,
    });
    if (sequence < state.requestSequence) return;
    if (response.status === 401) {
      setStatus("Auth required", "warn");
      els.lastUpdated.textContent = "Enter the gateway bearer token";
      syncAuthPanel(true);
      redrawCharts();
      return;
    }
    if (!response.ok) {
      setStatus(`HTTP ${response.status}`, "bad");
      els.lastUpdated.textContent = state.lastSnapshot
        ? `Last data ${formatTime(state.lastSnapshot.generated_at)}, refresh failed`
        : "Metrics request failed";
      return;
    }
    const snapshot = await response.json();
    if (sequence < state.requestSequence) return;
    state.appliedSequence = sequence;
    setStatus("Live", "ok");
    syncAuthPanel(false);
    renderSnapshot(snapshot);
    if (state.currentPage === "sandboxes") {
      const now = Date.now();
      if (now - state.lastSandboxRefreshAt >= 30000) {
        await refreshSandboxes({ quiet: true });
      }
    }
  } catch (error) {
    if (error && error.name === "AbortError") return;
    setStatus("Offline", "bad");
    els.lastUpdated.textContent = state.lastSnapshot
      ? `Last data ${formatTime(state.lastSnapshot.generated_at)}, connection lost`
      : String(error && error.message ? error.message : error);
    redrawCharts();
  } finally {
    if (state.metricsRequest === controller) {
      state.metricsRequest = null;
      els.refreshNowButton.disabled = false;
      scheduleNextRefresh();
    }
  }
}

function dashboardAuthHeaders() {
  const token = sessionStorage.getItem("ucloud.dashboard.token") || els.tokenInput.value.trim();
  return token ? { "X-UCloud-Sandbox-Token": token } : {};
}

async function dashboardJsonRequest(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    cache: "no-store",
    headers: {
      ...dashboardAuthHeaders(),
      ...(options.headers || {}),
    },
  });
  const contentType = response.headers.get("Content-Type") || "";
  let payload = {};
  if (contentType.includes("application/json")) {
    payload = await response.json();
  } else {
    payload = { error: summarizeResponseText(await response.text()) };
  }
  if (response.status === 401) {
    syncAuthPanel(true);
    throw new Error("Auth required");
  }
  if (!response.ok) {
    const detail = payload && payload.error ? String(payload.error) : `HTTP ${response.status}`;
    throw new Error(detail);
  }
  return payload;
}

function summarizeResponseText(text) {
  const normalized = String(text || "").replace(/\\s+/g, " ").trim();
  const titleMatch = normalized.match(/<title>(.*?)<\\/title>/i);
  const selected = titleMatch ? titleMatch[1] : normalized;
  if (!selected) return "non-JSON response";
  return selected.length > 300 ? `${selected.slice(0, 300)}...` : selected;
}

function errorMessage(error) {
  return String(error && error.message ? error.message : error);
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
  renderHealth(snapshot);
  if (state.currentPage === "overview") {
    renderOverviewDetail(snapshot);
  } else if (state.currentPage === "scheduler") {
    renderSchedulerPage(snapshot);
  } else if (state.currentPage === "nodes") {
    renderNodesPage();
  } else if (state.currentPage === "registry") {
    renderRegistryPage(snapshot);
  } else {
    renderSandboxesPage();
  }
}

function renderOverviewDetail(snapshot) {
  if (!snapshot || !snapshot.generated_at) return;
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
  const programs = snapshot.programs || {};
  const programStates = programs.states || {};
  const scale = snapshot.scale_up || {};
  const recentEvents = ((snapshot.events || {}).recent || []);
  const cpuActual = nullableNumber(actual.cpu_percent_avg);
  const memoryActual = nullableNumber(actual.memory_percent);
  const cpuReserved = ratioToPercent(load.vcpu);
  const memoryReserved = ratioToPercent(load.memory);
  return {
    at: Date.parse(snapshot.generated_at) || Date.now(),
    activeNodes: firstNumber(nodes.sandbox_ready, nodes.sandbox) || 0,
    freshNodes: asNumber(nodes.fresh),
    activeSandboxes: asNumber(sandboxes.active_routes),
    builderNodes: asNumber(nodes.builder),
    readyWake: asNumber(programStates.ready_to_wake),
    modelWait: asNumber(programStates.model_wait),
    wakeP95Seconds: msToSeconds(programs.response_to_wake_p95_ms),
    wakeP50Seconds: msToSeconds(programs.response_to_wake_p50_ms),
    modelWaitP95Seconds: msToSeconds(programs.model_wait_p95_ms),
    hardDiskUtilization: resourcePercent(
      sandboxResources.used || {},
      sandboxResources.effective || {},
      "disk_mb"
    ),
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
  const autoscaler = snapshot.autoscaler || {};
  const liveSignals = autoscaler.live_signals || {};
  const programs = snapshot.programs || {};
  const programStates = programs.states || {};
  const sandboxStates = sandboxes.states || {};
  const wakePlan = autoscaler.program_wake_plan || {};

  const blockedWakes = asNumber(wakePlan.unplaced_count);
  const staleNodes = (Array.isArray(nodes.items) ? nodes.items : []).filter(
    (node) => (node.capabilities || []).includes("sandbox") && (!node.fresh || !node.agent_version_compatible)
  ).length;
  const fleetAttention = staleNodes + asNumber(nodes.sandbox_draining) + asNumber(nodes.incompatible);
  const sandboxAttention = asNumber(sandboxes.pending) + asNumber(sandboxes.stale_routes)
    + asNumber(sandboxStates.failed) + asNumber(sandboxStates.error) + asNumber(sandboxStates.migrating);
  const imageAttention = asNumber(images.failed_builds) + asNumber(images.active_builds);
  setNavBadge("schedulerNavBadge", blockedWakes || asNumber(programStates.ready_to_wake), blockedWakes ? "bad" : "");
  setNavBadge("nodesNavBadge", fleetAttention, fleetAttention ? "warn" : "");
  setNavBadge("sandboxesNavBadge", sandboxAttention, sandboxAttention ? "warn" : "");
  setNavBadge("registryNavBadge", registry.configured && !registry.ok ? "!" : imageAttention, registry.configured && !registry.ok || asNumber(images.failed_builds) ? "bad" : "");

  setText("activeNodesValue", formatInteger(latest.activeNodes));
  setText(
    "activeNodesDetail",
    `${asNumber(autoscaler.ready_nodes)} ready, ${asNumber(autoscaler.provisioning_nodes)} booting, ${asNumber(autoscaler.unreachable_nodes)} unreachable, ${asNumber(autoscaler.total_nodes || nodes.sandbox)} total`
  );

  setText("runningSandboxesValue", formatInteger(sandboxes.active_routes));
  const staleRouteText = asNumber(sandboxes.stale_routes) > 0
    ? `, ${asNumber(sandboxes.stale_routes)} stale routes`
    : "";
  setText(
    "runningSandboxesDetail",
    `${asNumber(sandboxStates.running)} running, ${asNumber(sandboxStates.parked)} parked, ${asNumber(sandboxStates.waking)} waking${staleRouteText}`
  );

  setText("readyWakeValue", formatInteger(programStates.ready_to_wake));
  setText(
    "readyWakeDetail",
    asNumber(programStates.ready_to_wake) > 0
      ? `${formatAge(programs.oldest_ready_to_wake_seconds)} oldest, ${asNumber(wakePlan.unplaced_count)} unplaced`
      : "no response-ready work"
  );

  setText("diskCommitValue", formatPercentPoint(latest.hardDiskUtilization));
  setText(
    "diskCommitDetail",
    `${formatMemory((sandboxResources.free || {}).disk_mb)} free of ${formatMemory((sandboxResources.effective || {}).disk_mb)}`
  );

  setText("modelWaitValue", formatInteger(programStates.model_wait));
  setText(
    "modelWaitDetail",
    `${formatResources((programs.resources || {}).model_wait || {})}, ${formatAge(programs.oldest_model_wait_seconds)} oldest`
  );

  setText(
    "wakeLatencyValue",
    latest.wakeP95Seconds === null ? "-" : `${latest.wakeP95Seconds.toFixed(2)}s`
  );
  setText(
    "wakeLatencyDetail",
    `p50 ${formatDurationMs(programs.response_to_wake_p50_ms)}, ${formatInteger(programs.requests)} requests`
  );

  const actions = Array.isArray(autoscaler.actions) ? autoscaler.actions : [];
  const actionText = actionSummary(actions);
  setText(
    "autoscalerSummary",
    autoscaler.timestamp ? `${actionText}, cycle ${formatTime(autoscaler.timestamp)}` : "No autoscaler cycle loaded"
  );
  setText("autoscalerPressureValue", formatInteger(liveSignals.pressure_samples));
  setText(
    "autoscalerUtilizationValue",
    `${formatPercentPoint(ratioToPercent(liveSignals.cpu_utilization))} / ${formatPercentPoint(ratioToPercent(liveSignals.memory_utilization))}`
  );
  setText(
    "autoscalerProvisioningValue",
    nullableNumber(liveSignals.provisioning_p95_seconds) === null
      ? "-"
      : `${Number(liveSignals.provisioning_p95_seconds).toFixed(1)}s`
  );
  setText(
    "autoscalerIdleGraceValue",
    nullableNumber(autoscaler.effective_scale_down_idle_seconds) === null
      ? "-"
      : formatAge(autoscaler.effective_scale_down_idle_seconds)
  );
  setText(
    "programSummary",
    `${asNumber(programs.rollouts)} rollouts, ${asNumber(programs.requests)} requests`
  );
  setText("programModelWaitValue", formatInteger(programStates.model_wait));
  setText("programReadyValue", formatInteger(programStates.ready_to_wake));
  setText(
    "programOldestReadyValue",
    asNumber(programStates.ready_to_wake) > 0
      ? formatAge(programs.oldest_ready_to_wake_seconds)
      : "-"
  );
  setText(
    "programWakeLatencyValue",
    asNumber(programs.response_to_wake_p95_ms) > 0
      ? `${(asNumber(programs.response_to_wake_p95_ms) / 1000).toFixed(2)}s`
      : "-"
  );

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
  renderOverviewOperational(snapshot);
}

function renderOverviewOperational(snapshot) {
  const nodes = snapshot.nodes || {};
  const sandboxes = snapshot.sandboxes || {};
  const resources = ((snapshot.resources || {}).sandbox || {});
  const actual = resources.actual_usage || {};
  const load = resources.load || {};
  const free = resources.free || {};
  const used = resources.used || {};
  const effective = resources.effective || {};
  const autoscaler = snapshot.autoscaler || {};
  const policy = autoscaler.effective_policy || {};
  const programs = snapshot.programs || {};
  const states = programs.states || {};
  const actions = Array.isArray(autoscaler.actions) ? autoscaler.actions : [];
  const reasons = Array.isArray(autoscaler.reasons) ? autoscaler.reasons : [];
  const creates = actionCount(actions, "create");
  const stops = actionCount(actions, "stop");
  const decision = creates > 0 ? `Create ${formatInteger(creates)} node${creates === 1 ? "" : "s"}`
    : stops > 0 ? `Stop ${formatInteger(stops)} node${stops === 1 ? "" : "s"}`
      : "Hold current capacity";
  const actionEnabled = Boolean((autoscaler.program_signals || {}).action_enabled || policy.program_aware_autoscaling_enabled);

  setText("overviewDecisionTitle", decision);
  els.overviewDecisionBadge.textContent = actionEnabled ? "Active policy" : "Shadow policy";
  els.overviewDecisionBadge.className = `inline-badge ${actionEnabled ? "badge-ok" : "badge-muted"}`;
  setText("overviewSupplyValue", `${formatInteger(autoscaler.ready_nodes)} / ${formatInteger(autoscaler.provisioning_nodes)} / ${formatInteger(autoscaler.unreachable_nodes)}`);
  setText("overviewProjectedValue", formatResources(autoscaler.projected_free_resources || {}));
  setText("overviewDeficitValue", formatResources(autoscaler.resource_deficit || {}));
  els.overviewDecisionReasons.replaceChildren(...(reasons.length ? reasons : ["No additional scale action is required."]).slice(0, 3).map((reason) => {
    const item = document.createElement("span");
    item.textContent = String(reason);
    return item;
  }));

  const cpuActual = nullableNumber(actual.cpu_percent_avg);
  const cpuReserved = ratioToPercent(load.vcpu);
  const memoryActual = nullableNumber(actual.memory_percent);
  const memoryReserved = ratioToPercent(load.memory);
  const diskPercent = resourcePercent(used, effective, "disk_mb");
  const hasReadySupply = asNumber(nodes.sandbox_ready) > 0;
  const hasDeficit = resourceHasPositiveValue(autoscaler.resource_deficit);
  els.capacityFitBadge.textContent = hasDeficit ? "Does not fit" : hasReadySupply ? "Fits current supply" : "No ready nodes";
  els.capacityFitBadge.className = `inline-badge ${hasDeficit ? "badge-bad" : hasReadySupply ? "badge-ok" : "badge-muted"}`;
  setText(
    "capacitySummary",
    hasDeficit
      ? `Uncovered immediate demand: ${formatResources(autoscaler.resource_deficit || {})}.`
      : `${formatResources(free)} remains across ${formatInteger(nodes.sandbox_ready)} schedulable node(s).`
  );
  setText("capacityCpuValue", `${formatNumber(free.vcpu)} vCPU free`);
  setText("capacityCpuDetail", `Actual ${formatPercentPoint(cpuActual)} / reserved ${formatPercentPoint(cpuReserved)} / target ${formatPercentPoint(ratioToPercent(policy.target_cpu_utilization))}`);
  setMeterWidth("capacityCpuActualMeter", cpuActual);
  setMeterWidth("capacityCpuReservedMeter", cpuReserved);
  setText("capacityMemoryValue", `${formatMemory(free.memory_mb)} free`);
  setText("capacityMemoryDetail", `Actual ${formatPercentPoint(memoryActual)} / reserved ${formatPercentPoint(memoryReserved)} / target ${formatPercentPoint(ratioToPercent(policy.target_memory_utilization))}`);
  setMeterWidth("capacityMemoryActualMeter", memoryActual);
  setMeterWidth("capacityMemoryReservedMeter", memoryReserved);
  setText("capacityDiskValue", `${formatMemory(free.disk_mb)} free`);
  setText("capacityDiskDetail", `${formatMemory(used.disk_mb)} committed of ${formatMemory(effective.disk_mb)} hard capacity`);
  setMeterWidth("capacityDiskMeter", diskPercent);

  setText("overviewModelWaitAge", asNumber(states.model_wait) ? `oldest ${formatAge(programs.oldest_model_wait_seconds)}` : "No active wait");
  setText("overviewWakingValue", formatInteger(states.waking));
  setText("overviewActingValue", formatInteger(states.acting));
  setText("overviewModelLatency", formatDurationMs(programs.model_wait_p95_ms));
}

function renderHealth(snapshot) {
  const nodes = snapshot.nodes || {};
  const sandboxes = snapshot.sandboxes || {};
  const images = snapshot.images || {};
  const programs = snapshot.programs || {};
  const states = programs.states || {};
  const autoscaler = snapshot.autoscaler || {};
  const registry = snapshot.registry || {};
  const wakePlan = autoscaler.program_wake_plan || {};
  const recent = ((snapshot.events || {}).recent || []);
  const volumeErrors = (Array.isArray(nodes.items) ? nodes.items : []).reduce(
    (total, node) => total + asNumber((node.actual_usage || {}).storage_error_volumes),
    0
  );
  const signals = [];
  let severity = "ok";
  let title = "Service is healthy";
  let detail = "Current hard demand fits projected ready capacity.";

  const projectionErrors = recent.filter((event) =>
    ["program_state_projection_error", "program_wake_shadow_plan_error"].includes(event.kind)
  ).length;
  if (projectionErrors > 0) {
    severity = "bad";
    title = "Program scheduling telemetry is degraded";
    detail = `${projectionErrors} recent projection or shadow-plan error(s) need attention.`;
  } else if (asNumber(wakePlan.unplaced_count) > 0) {
    severity = "bad";
    title = "Ready work cannot be placed";
    detail = `${formatInteger(wakePlan.unplaced_count)} wake request(s) have no current hard fit.`;
  } else if (resourceHasPositiveValue(autoscaler.resource_deficit)) {
    severity = "warn";
    title = "Capacity is catching up";
    detail = `Autoscaler deficit: ${formatResources(autoscaler.resource_deficit)}.`;
  } else if (asNumber(sandboxes.stale_routes) > 0 || asNumber(nodes.incompatible) > 0) {
    severity = "warn";
    title = "Some supply is unavailable";
    detail = `${formatInteger(sandboxes.stale_routes)} stale route(s), ${formatInteger(nodes.incompatible)} incompatible node(s).`;
  } else if (volumeErrors > 0 || asNumber(images.failed_builds) > 0 || (registry.configured && !registry.ok)) {
    severity = "warn";
    title = "A supporting subsystem needs attention";
    detail = `${formatInteger(volumeErrors)} volume error(s), ${formatInteger(images.failed_builds)} failed build(s)${registry.configured && !registry.ok ? ", registry unavailable" : ""}.`;
  } else if (Boolean(autoscaler.create_pressure_scale_up)) {
    severity = "warn";
    title = "Create pipeline is saturated";
    const live = autoscaler.live_signals || {};
    detail = `${formatInteger(live.sandbox_create_limit)} create slots occupied; ${formatInteger(live.sandbox_create_rejections)} recent rejection(s).`;
  } else if (Boolean(autoscaler.pressure_scale_up)) {
    severity = "warn";
    title = "Live pressure is above policy";
    detail = "The autoscaler is adding or evaluating capacity from live node pressure.";
  } else if (actionCount(autoscaler.actions, "create") > 0 || actionCount(autoscaler.actions, "stop") > 0) {
    title = "Capacity is changing";
    detail = actionSummary(autoscaler.actions);
  }

  if (asNumber(states.ready_to_wake) > 0) {
    signals.push({ text: `${formatInteger(states.ready_to_wake)} ready`, mode: asNumber(wakePlan.unplaced_count) ? "bad" : "", page: "scheduler" });
  }
  if (asNumber(states.model_wait) > 0) {
    signals.push({ text: `${formatInteger(states.model_wait)} model wait`, mode: "", page: "scheduler" });
  }
  if (asNumber(nodes.sandbox_draining) > 0) {
    signals.push({ text: `${formatInteger(nodes.sandbox_draining)} draining`, mode: "warn", page: "nodes" });
  }
  if (asNumber(images.failed_builds) > 0) {
    signals.push({ text: `${formatInteger(images.failed_builds)} failed builds`, mode: "warn", page: "registry" });
  }
  if (volumeErrors > 0) {
    signals.push({ text: `${formatInteger(volumeErrors)} volume errors`, mode: "bad", page: "nodes" });
  }
  if (signals.length === 0) signals.push({ text: "No active warnings", mode: "" });

  els.healthBadge.className = `health-icon health-${severity}`;
  els.overviewNavBadge.textContent = severity === "bad" ? "Fix" : severity === "warn" ? "Watch" : "Live";
  els.overviewNavBadge.className = `nav-badge nav-badge-${severity}`;
  setText("healthTitle", title);
  setText("healthDetail", detail);
  els.healthSignals.replaceChildren(...signals.map((signal) => {
    const element = document.createElement(signal.page ? "button" : "span");
    element.className = `signal-chip ${signal.mode}`.trim();
    element.textContent = signal.text;
    if (signal.page) {
      element.type = "button";
      element.title = `Open ${signal.page}`;
      element.addEventListener("click", () => setPage(signal.page));
    }
    return element;
  }));
}

function renderSchedulerPage(snapshot) {
  if (!snapshot || !snapshot.generated_at) return;
  const autoscaler = snapshot.autoscaler || {};
  const programs = snapshot.programs || {};
  const states = programs.states || {};
  const programSignals = autoscaler.program_signals || {};
  const wakePlan = autoscaler.program_wake_plan || {};
  const policy = autoscaler.effective_policy || {};
  const actions = Array.isArray(autoscaler.actions) ? autoscaler.actions : [];
  const reasons = Array.isArray(autoscaler.reasons) ? autoscaler.reasons : [];
  const enabled = Boolean(programSignals.action_enabled || policy.program_aware_autoscaling_enabled);
  const creates = actionCount(actions, "create");
  const stops = actionCount(actions, "stop");
  const decisionTitle = creates > 0
    ? `Create ${formatInteger(creates)} node${creates === 1 ? "" : "s"}`
    : stops > 0
      ? `Stop ${formatInteger(stops)} node${stops === 1 ? "" : "s"}`
      : "Hold current capacity";

  setText("schedulerDecisionTitle", decisionTitle);
  els.schedulerModeBadge.textContent = enabled ? "Action enabled" : "Shadow only";
  els.schedulerModeBadge.className = `inline-badge ${enabled ? "badge-ok" : "badge-muted"}`;
  setText(
    "schedulerDecisionDetail",
    autoscaler.timestamp
      ? `${actionSummary(actions)}, cycle ${formatTime(autoscaler.timestamp)}`
      : "No autoscaler cycle loaded."
  );
  els.schedulerReasons.replaceChildren(...(reasons.length ? reasons : ["No scale action is required."]).slice(0, 6).map((reason) => {
    const span = document.createElement("span");
    span.className = `reason-chip ${resourceHasPositiveValue(autoscaler.resource_deficit) ? "warn" : ""}`.trim();
    span.textContent = String(reason);
    return span;
  }));
  setText("schedulerReadyNodesValue", formatInteger(autoscaler.ready_nodes));
  setText("schedulerProvisioningValue", formatInteger(autoscaler.provisioning_nodes));
  setText("schedulerWakePlanValue", `${formatInteger(wakePlan.placed)}/${formatInteger(wakePlan.queued)}`);
  setText("schedulerUnplacedValue", formatInteger(wakePlan.unplaced_count));

  setText("flowAllValue", formatInteger(programs.requests));
  setText("flowModelWaitValue", formatInteger(states.model_wait));
  setText("flowModelWaitDetail", `p95 ${formatDurationMs(programs.model_wait_p95_ms)}`);
  setText("flowReadyValue", formatInteger(states.ready_to_wake));
  setText("flowReadyDetail", `oldest ${formatAge(programs.oldest_ready_to_wake_seconds)}`);
  setText("flowWakingValue", formatInteger(states.waking));
  setText("flowActingValue", formatInteger(states.acting));
  setText(
    "programFlowSummary",
    `${formatInteger(programs.rollouts)} rollouts / ${formatInteger(programs.sandboxes)} sandboxes`
  );

  renderCapacityEquation({
    immediate: autoscaler.pending_resources || {},
    ready: programSignals.ready_to_wake_resources || {},
    predictive: programSignals.weighted_model_wait_resources || {},
    prepared: autoscaler.prepared_resources || {},
    free: autoscaler.projected_free_resources || {},
    deficit: autoscaler.resource_deficit || {},
  });
  const liveSignals = autoscaler.live_signals || {};
  const pressureParts = [
    `${formatInteger(liveSignals.pressure_samples)} host`,
    `${formatInteger(liveSignals.create_pressure_samples)} create`,
  ];
  if (nullableNumber(liveSignals.rootfs_export_queue_utilization) !== null) {
    pressureParts.push(`${formatPercentPoint(ratioToPercent(liveSignals.rootfs_export_queue_utilization))} rootfs`);
  }
  setText("decisionPressureValue", pressureParts.join(" / "));
  setText("decisionIdleGraceValue", `Idle grace ${formatAge(autoscaler.effective_scale_down_idle_seconds)}`);
  renderPolicy(policy);
  renderProgramQueue();
}

function renderCapacityEquation(rows) {
  const mapping = [
    ["Immediate", rows.immediate],
    ["Ready", rows.ready],
    ["Predictive", rows.predictive],
    ["Prepared", rows.prepared],
    ["Free", rows.free],
    ["Deficit", rows.deficit],
  ];
  for (const [prefix, resources] of mapping) {
    const values = resources || {};
    setText(`equation${prefix}Cpu`, `${formatNumber(values.vcpu)} vCPU`);
    setText(`equation${prefix}Memory`, formatMemory(values.memory_mb));
    setText(`equation${prefix}Disk`, formatMemory(values.disk_mb));
  }
}

function renderPolicy(policy) {
  const rows = [
    ["Program action", policy.program_aware_autoscaling_enabled ? "Enabled" : "Shadow"],
    ["Model-wait weight", formatPercentPoint(ratioToPercent(policy.model_wait_capacity_weight))],
    ["Leading headroom", `${formatInteger(policy.model_wait_max_headroom_nodes)} node max`],
    ["Node range", `${formatInteger(policy.min_nodes)}–${formatInteger(policy.max_nodes)}`],
    ["CPU target", formatPercentPoint(ratioToPercent(policy.target_cpu_utilization))],
    ["Memory target", formatPercentPoint(ratioToPercent(policy.target_memory_utilization))],
    ["Storage queue", formatPercentPoint(ratioToPercent(policy.target_storage_queue_utilization))],
    ["Create target", `${formatInteger(policy.create_target_concurrency_per_node)} per node`],
    ["Create burst", `${formatInteger(policy.create_pressure_max_headroom_nodes)} node max`],
    ["Idle grace", formatAge(policy.scale_down_idle_seconds)],
  ];
  els.policyValues.replaceChildren(...rows.map(([name, value]) => {
    const wrapper = document.createElement("div");
    const dt = document.createElement("dt");
    const dd = document.createElement("dd");
    dt.textContent = name;
    dd.textContent = value;
    wrapper.append(dt, dd);
    return wrapper;
  }));
}

function renderProgramQueue() {
  if (!els.programQueueRows) return;
  const snapshot = state.lastSnapshot || {};
  const programs = snapshot.programs || {};
  const autoscaler = snapshot.autoscaler || {};
  const plan = autoscaler.program_wake_plan || {};
  const queue = Array.isArray(programs.shadow_wake_queue) ? programs.shadow_wake_queue : [];
  const placements = Array.isArray(plan.placements) ? plan.placements : [];
  const unplaced = Array.isArray(plan.unplaced) ? plan.unplaced : [];
  const byRequest = new Map();
  for (const item of queue) byRequest.set(String(item.request_id || ""), { ...item });
  for (const item of placements) {
    const key = String(item.request_id || "");
    byRequest.set(key, { ...(byRequest.get(key) || {}), ...item, result: "placed" });
  }
  for (const item of unplaced) {
    const key = String(item.request_id || "");
    byRequest.set(key, { ...(byRequest.get(key) || {}), ...item, result: "unplaced" });
  }
  let rows = [...byRequest.values()];
  const query = String(els.programSearchInput.value || "").trim().toLowerCase();
  const resultFilter = String(els.programResultFilter.value || "all");
  if (!["all", "ready_to_wake"].includes(state.programStateFilter)) rows = [];
  if (query) {
    rows = rows.filter((item) => [
      item.rollout_id,
      item.request_id,
      item.sandbox_id,
      item.node_id,
      item.job_id,
      item.reason,
    ].join(" ").toLowerCase().includes(query));
  }
  if (resultFilter === "local") rows = rows.filter((item) => item.local === true);
  if (resultFilter === "migration") rows = rows.filter((item) => item.local === false && item.result === "placed");
  if (resultFilter === "unplaced") {
    rows.sort((a, b) => Number(b.result === "unplaced") - Number(a.result === "unplaced") || asNumber(a.position) - asNumber(b.position));
  } else {
    rows.sort((a, b) => asNumber(a.position) - asNumber(b.position));
  }
  const shown = rows.slice(0, MAX_PROGRAM_ROWS);
  const truncated = asNumber(plan.placements_truncated) + asNumber(plan.unplaced_truncated);
  setText(
    "programQueueSummary",
    `${formatInteger(shown.length)} shown, ${formatInteger(plan.queued)} queued, ${formatInteger(plan.unplaced_count)} unplaced${truncated ? `, ${formatInteger(truncated)} sampled out` : ""}`
  );
  if (shown.length === 0) {
    renderEmptyRow(
      els.programQueueRows,
      8,
      state.programStateFilter === "all" || state.programStateFilter === "ready_to_wake"
        ? "No ready wake requests match the current filter"
        : "Per-request rows are available for the ready-to-wake phase"
    );
    return;
  }
  els.programQueueRows.replaceChildren(...shown.map(programQueueRow));
}

function programQueueRow(item) {
  const tr = document.createElement("tr");
  if (item.result === "unplaced") tr.className = "row-alert";
  appendCell(tr, formatInteger(item.position));
  appendCell(tr, formatAge(firstNumber(item.ready_age_seconds, item.age_seconds)));
  appendClassCell(
    tr,
    `${item.rollout_id || "-"} / ${item.request_id || "-"}`,
    "",
    `${item.rollout_id || ""}\\n${item.request_id || ""}`
  );
  appendClassCell(tr, `${item.sandbox_id || "-"} / g${formatInteger(item.sandbox_generation)}`, "");
  appendCell(tr, formatResources(item.resources || {}));
  appendClassCell(tr, item.node_id || item.job_id || "-", "");
  appendCell(tr, item.result === "placed" ? (item.local ? "local" : "migration") : "-");
  const result = item.result === "unplaced" ? `blocked: ${item.reason || "no hard fit"}` : item.result || "queued";
  appendCell(tr, result);
  return tr;
}

function renderNodesPage() {
  if (!els.nodeRows) return;
  const snapshot = state.lastSnapshot || {};
  const nodes = snapshot.nodes || {};
  const resources = (snapshot.resources || {}).sandbox || {};
  const autoscaler = snapshot.autoscaler || {};
  const policy = autoscaler.effective_policy || {};
  const plan = autoscaler.program_wake_plan || {};
  const placements = Array.isArray(plan.placements) ? plan.placements : [];
  const plannedByNode = placements.reduce((counts, item) => {
    const key = String(item.node_id || "");
    if (key) counts.set(key, (counts.get(key) || 0) + 1);
    return counts;
  }, new Map());
  const query = String(els.nodeSearchInput.value || "").trim().toLowerCase();
  const filter = String(els.nodeStateFilter.value || "all");
  let items = Array.isArray(nodes.items) ? nodes.items.filter((item) => (item.capabilities || []).includes("sandbox")) : [];
  items = items.filter((item) => {
    const stateName = nodeState(item);
    if (query && !`${item.node_id || ""} ${item.job_id || ""}`.toLowerCase().includes(query)) return false;
    if (filter === "all") return true;
    if (filter === "ready") return stateName.mode === "ok";
    if (filter === "constrained") return nodeConstrained(item);
    if (filter === "draining") return Boolean(item.draining || item.admission_open === false);
    if (filter === "stale") return !item.fresh || !item.agent_version_compatible;
    return true;
  });
  items.sort((a, b) => nodeStateRank(a) - nodeStateRank(b) || asNumber(b.active_workloads) - asNumber(a.active_workloads) || String(a.node_id).localeCompare(String(b.node_id)));
  const shown = items.slice(0, MAX_NODE_ROWS);
  const allSandboxNodes = Array.isArray(nodes.items) ? nodes.items.filter((item) => (item.capabilities || []).includes("sandbox")) : [];
  const freshSandboxNodes = allSandboxNodes.filter((item) => item.fresh && item.agent_version_compatible);
  const aggregateActual = resources.actual_usage || {};
  const aggregateLoad = resources.load || {};
  const storage = freshSandboxNodes.reduce((summary, item) => {
    const actual = item.actual_usage || {};
    summary.active += asNumber(actual.storage_active_operations);
    summary.waiting += asNumber(actual.storage_waiting_operations);
    summary.limit += asNumber(actual.storage_max_concurrent_operations);
    summary.errors += asNumber(actual.storage_error_volumes);
    summary.rootfsActive += asNumber(actual.rootfs_export_active_operations);
    summary.rootfsWaiting += asNumber(actual.rootfs_export_waiting_operations);
    summary.rootfsLimit += asNumber(actual.rootfs_export_max_concurrent_operations);
    summary.psi = Math.max(summary.psi, asNumber(actual.memory_psi_full_avg10));
    return summary;
  }, { active: 0, waiting: 0, limit: 0, errors: 0, rootfsActive: 0, rootfsWaiting: 0, rootfsLimit: 0, psi: 0 });
  const staleOrIncompatible = allSandboxNodes.filter((item) => !item.fresh || !item.agent_version_compatible).length;
  setText("nodesReadyValue", formatInteger(nodes.sandbox_ready));
  setText("nodesProvisioningValue", formatInteger(autoscaler.provisioning_nodes));
  setText("nodesDrainingValue", formatInteger(nodes.sandbox_draining));
  setText("nodesIncompatibleValue", formatInteger(staleOrIncompatible));
  setText("nodesDiskFreeValue", formatMemory((resources.free || {}).disk_mb));
  setText("nodesCpuPressureValue", `${formatPercentPoint(aggregateActual.cpu_percent_avg)} / ${formatPercentPoint(ratioToPercent(aggregateLoad.vcpu))}`);
  setText("nodesCpuPressureDetail", `target ${formatPercentPoint(ratioToPercent(policy.target_cpu_utilization))}`);
  setText("nodesMemoryPressureValue", `${formatPercentPoint(aggregateActual.memory_percent)} / ${formatPercentPoint(ratioToPercent(aggregateLoad.memory))}`);
  setText("nodesMemoryPressureDetail", `target ${formatPercentPoint(ratioToPercent(policy.target_memory_utilization))}`);
  setText("nodesPsiValue", `${formatNumber(storage.psi)}%`);
  setText("nodesStorageQueueValue", `${formatInteger(storage.active + storage.rootfsActive)} / ${formatInteger(storage.waiting + storage.rootfsWaiting)}`);
  setText("nodesStorageQueueDetail", `active / waiting; storage ${formatInteger(storage.limit)}, rootfs ${formatInteger(storage.rootfsLimit)} max`);
  setText("nodesVolumeErrorsValue", formatInteger(storage.errors));
  setText(
    "nodesPageDetail",
    `${formatInteger(nodes.sandbox_ready)} ready of ${formatInteger(nodes.sandbox)} fresh sandbox nodes; ${formatInteger(plan.placed)} shadow wake placement(s).`
  );
  setText("nodeTableSummary", `${formatInteger(shown.length)} shown of ${formatInteger(items.length)} matching`);
  if (shown.length === 0) {
    renderEmptyRow(els.nodeRows, 8, "No nodes match the current filter");
    return;
  }
  els.nodeRows.replaceChildren(...shown.map((item) => nodeRow(item, plannedByNode.get(String(item.node_id || "")) || 0)));
}

function nodeRow(item, plannedWakes) {
  const tr = document.createElement("tr");
  const stateInfo = nodeState(item);
  const stateCell = document.createElement("td");
  const stateWrapper = document.createElement("span");
  const dot = document.createElement("i");
  dot.className = `state-dot ${stateInfo.mode}`;
  stateWrapper.className = "state-cell";
  stateWrapper.append(dot, document.createTextNode(stateInfo.label));
  stateCell.append(stateWrapper);
  tr.append(stateCell);
  appendClassCell(tr, `${item.node_id || "-"}\\n${item.job_id || "-"}`, "", item.node_url || "");
  appendCell(tr, `${formatInteger(item.active_sandboxes)} sandboxes${plannedWakes ? `, +${plannedWakes} planned` : ""}`);
  tr.append(resourceMeterCell(item, "vcpu", "cpu_percent"));
  tr.append(resourceMeterCell(item, "memory_mb", "memory_percent"));
  const free = item.free_resources || {};
  const effective = item.effective_resources || {};
  appendCell(tr, `${formatMemory(free.disk_mb)} / ${formatMemory(effective.disk_mb)}`);
  appendCell(tr, nodePressureText(item));
  appendCell(tr, item.fresh ? `${formatAge(item.age_seconds)} ago` : `stale ${formatAge(item.age_seconds)}`);
  return tr;
}

function resourceMeterCell(item, resource, actualKey) {
  const td = document.createElement("td");
  const load = ratioToPercent((item.load || {})[resource === "vcpu" ? "vcpu" : "memory"]);
  const actual = nullableNumber((item.actual_usage || {})[actualKey]);
  const value = firstNumber(actual, load, 0);
  const wrapper = document.createElement("div");
  const label = document.createElement("div");
  const meter = document.createElement("div");
  const fill = document.createElement("span");
  const actualLabel = document.createElement("span");
  const reservedLabel = document.createElement("span");
  wrapper.className = "meter-stack";
  label.className = "meter-label";
  actualLabel.textContent = `${formatPercentPoint(actual)} actual`;
  reservedLabel.textContent = `${formatPercentPoint(load)} reserved`;
  label.append(actualLabel, reservedLabel);
  meter.className = `meter ${value >= 90 ? "bad" : value >= 75 ? "warn" : ""}`.trim();
  fill.style.width = `${Math.max(0, Math.min(100, value))}%`;
  meter.append(fill);
  wrapper.append(label, meter);
  td.append(wrapper);
  return td;
}

function nodeState(item) {
  if (!item.fresh) return { label: "Stale", mode: "bad" };
  if (!item.agent_version_compatible) return { label: "Incompatible", mode: "bad" };
  if (item.draining) return { label: "Draining", mode: "warn" };
  if (item.admission_open === false) return { label: "Closed", mode: "warn" };
  if (nodeConstrained(item)) return { label: "Constrained", mode: "warn" };
  return { label: "Ready", mode: "ok" };
}

function nodeStateRank(item) {
  const mode = nodeState(item).mode;
  return mode === "bad" ? 0 : mode === "warn" ? 1 : 2;
}

function nodeConstrained(item) {
  const actual = item.actual_usage || {};
  const load = item.load || {};
  const storageLimit = asNumber(actual.storage_max_concurrent_operations);
  const storageQueue = storageLimit > 0
    ? (asNumber(actual.storage_active_operations) + asNumber(actual.storage_waiting_operations)) / storageLimit
    : 0;
  const rootfsLimit = asNumber(actual.rootfs_export_max_concurrent_operations);
  const rootfsQueue = rootfsLimit > 0
    ? (asNumber(actual.rootfs_export_active_operations) + asNumber(actual.rootfs_export_waiting_operations)) / rootfsLimit
    : 0;
  return asNumber(load.vcpu) >= 0.8
    || asNumber(load.memory) >= 0.85
    || asNumber(actual.memory_psi_full_avg10) >= 5
    || storageQueue >= 0.75
    || rootfsQueue >= 0.75
    || asNumber(actual.storage_error_volumes) > 0;
}

function nodePressureText(item) {
  const actual = item.actual_usage || {};
  const parts = [];
  if (nullableNumber(actual.memory_psi_full_avg10) !== null) parts.push(`PSI ${formatNumber(actual.memory_psi_full_avg10)}`);
  const limit = asNumber(actual.storage_max_concurrent_operations);
  if (limit > 0) parts.push(`storage ${asNumber(actual.storage_active_operations) + asNumber(actual.storage_waiting_operations)}/${limit}`);
  const rootfsLimit = asNumber(actual.rootfs_export_max_concurrent_operations);
  if (rootfsLimit > 0) parts.push(`rootfs ${asNumber(actual.rootfs_export_active_operations) + asNumber(actual.rootfs_export_waiting_operations)}/${rootfsLimit}`);
  if (asNumber(actual.storage_error_volumes) > 0) parts.push(`${formatInteger(actual.storage_error_volumes)} volume errors`);
  return parts.join(", ") || "normal";
}

function actionCount(actions, kind) {
  return (Array.isArray(actions) ? actions : []).reduce(
    (total, action) => total + (actionKind(action) === kind ? Math.max(0, asNumber((action || {}).count || ((action || {}).job_ids || []).length || 1)) : 0),
    0
  );
}

function actionSummary(actions) {
  if (!Array.isArray(actions) || actions.length === 0) return "Hold";
  return actions.map((action) => {
    const kind = actionKind(action);
    const count = Math.max(0, asNumber((action || {}).count || ((action || {}).job_ids || []).length));
    const label = kind === "create" || kind === "scale_up" ? "Create"
      : kind === "stop" || kind === "scale_down" ? "Stop"
        : kind === "scale_up_builder" ? "Create builder"
          : String(kind || "Action").replaceAll("_", " ");
    return `${label}${count ? ` ${formatInteger(count)}` : ""}`;
  }).join(", ");
}

function actionKind(action) {
  if (typeof action === "string") return action;
  return action && typeof action === "object" ? String(action.kind || "") : "";
}

async function copyDiagnostics() {
  if (!state.lastSnapshot) return;
  const snapshot = state.lastSnapshot;
  const autoscaler = snapshot.autoscaler || {};
  const programs = snapshot.programs || {};
  const text = [
    `Generated: ${snapshot.generated_at || "-"}`,
    `Health: ${els.healthTitle.textContent}`,
    `Nodes: ${asNumber((snapshot.nodes || {}).sandbox_ready)} ready / ${asNumber(autoscaler.provisioning_nodes)} provisioning`,
    `Programs: ${asNumber(programs.requests)} requests, ${asNumber((programs.states || {}).ready_to_wake)} ready`,
    `Wake plan: ${asNumber((autoscaler.program_wake_plan || {}).placed)} placed / ${asNumber((autoscaler.program_wake_plan || {}).unplaced_count)} unplaced`,
    `Decision: ${actionSummary(autoscaler.actions)}`,
    `Reasons: ${(autoscaler.reasons || []).join("; ") || "none"}`,
  ].join("\\n");
  try {
    await navigator.clipboard.writeText(text);
    showToast("Operational summary copied");
  } catch (_error) {
    showToast("Could not copy the operational summary", "bad");
  }
}

function downloadSnapshot() {
  if (!state.lastSnapshot) return;
  const blob = new Blob([JSON.stringify(state.lastSnapshot, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `ucloud-sandbox-metrics-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  link.click();
  URL.revokeObjectURL(url);
  showToast("Metrics snapshot downloaded");
}

function showToast(message, mode = "ok") {
  if (!els.toastRegion) return;
  els.toastRegion.textContent = message;
  els.toastRegion.className = `toast-region is-visible ${mode === "bad" ? "bad" : ""}`.trim();
  window.setTimeout(() => {
    els.toastRegion.className = "toast-region";
  }, 2400);
}

function renderEvents(snapshot) {
  const allEvents = ((snapshot.events || {}).recent || []).slice().reverse();
  const events = allEvents.filter(isMeaningfulEvent).slice(0, 12);
  els.eventSummary.textContent = events.length ? `${events.length} decisions or exceptions` : "No decisions or exceptions";
  if (events.length === 0) {
    els.eventRows.innerHTML = '<tr><td colspan="4" class="empty-cell">No recent events</td></tr>';
    return;
  }
  els.eventRows.replaceChildren(...events.map(eventRow));
}

function isMeaningfulEvent(event) {
  if (!event || event.kind === "node_heartbeat") return false;
  if (severityForEvent(event) !== "INFO") return true;
  if (event.kind !== "autoscaler_cycle") return true;
  const data = event.data || {};
  return actionSummary([...(data.actions || []), ...(data.builder_actions || [])]) !== "Hold"
    || resourceHasPositiveValue(data.resource_deficit);
}

async function refreshSandboxes(options = {}) {
  if (state.sandboxFetchInFlight) return;
  state.sandboxFetchInFlight = true;
  renderSandboxesPage();
  if (!options.quiet) {
    setSandboxPageStatus("Loading", "warn");
  }
  try {
    const payload = await dashboardJsonRequest("/v1/sandboxes?refresh=true");
    const rows = Array.isArray(payload.sandboxes) ? payload.sandboxes : [];
    state.lastSandboxes = rows.map(normalizeSandboxRecord).sort(compareSandboxesNewestFirst);
    state.lastSandboxRefreshAt = Date.now();
    setSandboxPageStatus(payload.cached ? "Cached" : "Live", "ok");
    setText(
      "sandboxesPageDetail",
      state.lastSandboxes.length
        ? `${formatInteger(state.lastSandboxes.length)} latest sandbox records loaded`
        : "No sandboxes are currently listed"
    );
    renderSandboxesPage();
  } catch (error) {
    const message = errorMessage(error);
    setSandboxPageStatus("Failed", "bad");
    setText("sandboxesPageDetail", message);
    setText("sandboxesPageSummary", message);
  } finally {
    state.sandboxFetchInFlight = false;
    renderSandboxesPage();
  }
}

function setSandboxPageStatus(text, mode) {
  if (!els.sandboxesPageStatusBadge) return;
  els.sandboxesPageStatusBadge.textContent = text;
  els.sandboxesPageStatusBadge.className = `inline-badge ${mode === "ok" ? "badge-ok" : mode === "bad" ? "badge-bad" : "badge-warn"}`;
}

function renderSandboxesPage() {
  if (!els.sandboxesPage) return;
  const snapshot = state.lastSnapshot || {};
  const metrics = snapshot.sandboxes || {};
  const query = String(els.sandboxSearchInput.value || "").trim().toLowerCase();
  const stateFilter = String(els.sandboxStateFilter.value || "all");
  const sandboxes = state.lastSandboxes || [];
  const filtered = sandboxes.filter((sandbox) => sandboxMatchesSearch(sandbox, query) && sandboxMatchesState(sandbox, stateFilter));
  const activeRoutes = asNumber(metrics.active_routes);
  const staleRoutes = asNumber(metrics.stale_routes);
  const stateCounts = sandboxes.reduce((counts, sandbox) => {
    const group = sandboxStateGroup(sandbox.state);
    counts[group] = (counts[group] || 0) + 1;
    return counts;
  }, {});

  setText("sandboxesPageRowsValue", formatInteger(sandboxes.length));
  setText("sandboxesPageTerminableValue", formatInteger(stateCounts.running));
  setText("sandboxesPagePendingValue", formatInteger(stateCounts.parked));
  setText("sandboxesPageRoutesValue", formatInteger((stateCounts.transitioning || 0) + (stateCounts.migrating || 0)));
  setText(
    "sandboxesPageSummary",
    `${formatInteger(filtered.length)} shown of ${formatInteger(sandboxes.length)} loaded, ${formatInteger(activeRoutes)} active routes${staleRoutes ? `, ${formatInteger(staleRoutes)} stale` : ""}`
  );
  setText(
    "sandboxListSummary",
    filtered.length
      ? `${formatInteger(filtered.length)} latest`
      : (sandboxes.length ? "No matching sandboxes" : "No sandboxes loaded")
  );

  els.refreshSandboxesButton.disabled = state.sandboxFetchInFlight;
  els.refreshSandboxesButton.textContent = state.sandboxFetchInFlight ? "Refreshing" : "Refresh";

  if (filtered.length === 0) {
    renderEmptyRow(
      els.sandboxRows,
      8,
      sandboxes.length ? "No sandboxes match the current search" : "No sandboxes loaded"
    );
    return;
  }
  els.sandboxRows.replaceChildren(...filtered.slice(0, 250).map(sandboxRow));
}

function normalizeSandboxRecord(record) {
  const raw = plainObject(record);
  const spec = plainObject(raw.spec);
  const node = plainObject(raw.node);
  const labels = plainObject(raw.labels);
  const specLabels = plainObject(spec.labels);
  const resources = plainObject(raw.resources);
  return {
    id: firstText(raw.id, raw.sandbox_id, spec.id),
    generation: firstNumber(raw.generation, raw.sandbox_generation, spec.generation),
    state: firstText(raw.state, raw.status, raw.cached_state, "unknown"),
    image: firstText(raw.image, spec.image, "-"),
    profile: firstText(raw.profile, spec.profile, "-"),
    node: firstText(node.node_id, node.job_id, raw.node_id, raw.job_id, "-"),
    nodeFresh: node.fresh,
    resources,
    spec,
    labels: Object.keys(labels).length ? labels : specLabels,
    createdAt: firstText(raw.created_at, raw.createdAt),
    updatedAt: firstText(raw.updated_at, raw.updatedAt),
    operationId: firstText(raw.operation_id, raw.operationId),
    checkpointId: firstText(raw.checkpoint_id, raw.checkpointId),
    creationKind: firstText(raw.creation_kind, raw.creationKind),
    routeOnly: Boolean(raw.route_only),
    cached: Boolean(raw.cached),
  };
}

function compareSandboxesNewestFirst(a, b) {
  const aTime = Date.parse(a.createdAt || a.updatedAt || "") || 0;
  const bTime = Date.parse(b.createdAt || b.updatedAt || "") || 0;
  if (aTime !== bTime) return bTime - aTime;
  return String(a.id || "").localeCompare(String(b.id || ""));
}

function sandboxMatchesSearch(sandbox, query) {
  if (!query) return true;
  return [
    sandbox.id,
    sandbox.state,
    sandbox.image,
    sandbox.profile,
    sandbox.node,
    labelsText(sandbox.labels),
  ].join(" ").toLowerCase().includes(query);
}

function sandboxMatchesState(sandbox, filter) {
  if (!filter || filter === "all") return true;
  const group = sandboxStateGroup(sandbox.state);
  if (filter === "attention") return ["failed", "stale", "migrating", "transitioning", "pending"].includes(group) || sandbox.nodeFresh === false;
  return group === filter;
}

function sandboxStateGroup(state) {
  const status = String(state || "unknown").toLowerCase();
  if (["running", "acting", "ready"].includes(status)) return "running";
  if (["parked", "paused"].includes(status)) return "parked";
  if (["parking", "waking", "restoring", "moving"].includes(status)) return "transitioning";
  if (["migrating", "migration_pending"].includes(status)) return "migrating";
  if (["creating", "pending", "queued"].includes(status)) return "pending";
  if (["failed", "error", "terminated", "terminating"].includes(status)) return "failed";
  if (["stale", "orphaned"].includes(status)) return "stale";
  return "unknown";
}

function canTerminateSandbox(sandbox) {
  return Boolean(sandbox && sandbox.id);
}

function sandboxRow(sandbox) {
  const tr = document.createElement("tr");
  const statusCell = document.createElement("td");
  const badge = document.createElement("span");
  const status = String(sandbox.state || "unknown").toLowerCase();
  badge.className = `sandbox-status ${sandboxStatusClass(status)}`;
  badge.textContent = status || "unknown";
  statusCell.append(badge);
  tr.append(statusCell);

  appendClassCell(
    tr,
    sandbox.generation === null ? (sandbox.id || "-") : `${sandbox.id || "-"} / g${formatInteger(sandbox.generation)}`,
    "sandbox-id",
    [sandbox.id, sandbox.operationId, sandbox.checkpointId, sandbox.creationKind].filter(Boolean).join("\\n")
  );
  appendClassCell(tr, sandbox.image || "-", "sandbox-image", sandbox.image || "");
  appendClassCell(tr, sandboxNodeText(sandbox), "sandbox-node", sandboxNodeTitle(sandbox));
  appendClassCell(tr, sandboxResourcesText(sandbox), "sandbox-resources", sandboxResourcesText(sandbox));
  appendCell(tr, sandboxAge(sandbox));
  appendClassCell(tr, labelsText(sandbox.labels), "sandbox-labels", labelsTitle(sandbox.labels));

  const actionCell = document.createElement("td");
  const button = document.createElement("button");
  const terminating = sandbox.id && state.terminatingSandboxIds.has(sandbox.id);
  button.className = "table-action danger";
  button.type = "button";
  button.textContent = terminating ? "Terminating" : "Terminate";
  button.disabled = !sandbox.id || state.sandboxActionInFlight || Boolean(terminating);
  button.title = sandbox.id ? `Terminate ${sandbox.id}` : "No sandbox id available";
  button.addEventListener("click", () => terminateSandbox(sandbox.id));
  actionCell.append(button);
  tr.append(actionCell);
  return tr;
}

function sandboxStatusClass(status) {
  const group = sandboxStateGroup(status);
  if (["running", "parked", "transitioning", "migrating", "pending", "failed", "stale"].includes(group)) return group;
  return "unknown";
}

function sandboxNodeText(sandbox) {
  const suffix = sandbox.nodeFresh === false ? " stale" : "";
  return `${sandbox.node || "-"}${suffix}`;
}

function sandboxNodeTitle(sandbox) {
  const parts = [];
  if (sandbox.node) parts.push(sandbox.node);
  if (sandbox.cached) parts.push("cached route");
  if (sandbox.routeOnly) parts.push("route only");
  if (sandbox.nodeFresh === false) parts.push("stale heartbeat");
  return parts.join(", ");
}

function sandboxResourcesText(sandbox) {
  const resources = sandbox.resources || {};
  const spec = sandbox.spec || {};
  const cpu = firstNumber(resources.vcpu, resources.cpu, resources.cpus, spec.cpus);
  const memory = firstNumber(resources.memory_mb, resources.memory, spec.memory_mb);
  const disk = firstNumber(resources.disk_mb, resources.disk, spec.disk_mb);
  const parts = [];
  if (cpu !== null && cpu > 0) parts.push(`${formatNumber(cpu)} vCPU`);
  if (memory !== null && memory > 0) parts.push(formatMemory(memory));
  if (disk !== null && disk > 0) parts.push(formatMemory(disk));
  return parts.join(" / ") || "-";
}

function sandboxAge(sandbox) {
  const created = Date.parse(sandbox.createdAt || "");
  if (!Number.isFinite(created)) return "-";
  return formatAge((Date.now() - created) / 1000);
}

function labelsText(labels) {
  const entries = Object.entries(plainObject(labels)).sort(([a], [b]) => a.localeCompare(b));
  if (!entries.length) return "-";
  return entries.slice(0, 4).map(([key, value]) => `${key}=${value}`).join(", ");
}

function labelsTitle(labels) {
  const entries = Object.entries(plainObject(labels)).sort(([a], [b]) => a.localeCompare(b));
  return entries.map(([key, value]) => `${key}=${value}`).join("\\n");
}

async function terminateSandbox(sandboxId) {
  const id = String(sandboxId || "").trim();
  if (!id) return;
  if (!window.confirm(`Terminate sandbox ${id}?`)) return;
  state.terminatingSandboxIds.add(id);
  renderSandboxesPage();
  try {
    await deleteSandboxById(id);
    setSandboxPageStatus("Terminated", "ok");
    setText("sandboxesPageDetail", `${id} terminated`);
  } catch (error) {
    setSandboxPageStatus("Failed", "bad");
    setText("sandboxesPageDetail", errorMessage(error));
  } finally {
    state.terminatingSandboxIds.delete(id);
    await refreshSandboxes({ force: true, quiet: true });
  }
}

async function terminateAllSandboxes() {
  const candidates = (state.lastSandboxes || []).filter(canTerminateSandbox);
  if (!candidates.length) return;
  if (!window.confirm(`Terminate all ${candidates.length} listed sandboxes?`)) return;
  state.sandboxActionInFlight = true;
  renderSandboxesPage();
  const failures = [];
  for (const [index, sandbox] of candidates.entries()) {
    state.terminatingSandboxIds.add(sandbox.id);
    setText(
      "sandboxesPageDetail",
      `Terminating ${formatInteger(index + 1)} of ${formatInteger(candidates.length)}: ${sandbox.id}`
    );
    try {
      await deleteSandboxById(sandbox.id);
    } catch (error) {
      failures.push(`${sandbox.id}: ${errorMessage(error)}`);
    } finally {
      state.terminatingSandboxIds.delete(sandbox.id);
    }
  }
  state.sandboxActionInFlight = false;
  await refreshSandboxes({ force: true, quiet: true });
  if (failures.length) {
    setSandboxPageStatus("Partial", "bad");
    setText("sandboxesPageDetail", `${failures.length} terminate request(s) failed: ${failures.slice(0, 3).join("; ")}`);
  } else {
    setSandboxPageStatus("Terminated", "ok");
    setText("sandboxesPageDetail", `${candidates.length} sandbox terminate request(s) completed`);
  }
}

async function deleteSandboxById(sandboxId) {
  return dashboardJsonRequest(`/v1/sandboxes/${encodeURIComponent(sandboxId)}`, {
    method: "DELETE",
  });
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

  setText("registryActiveBuildsValue", formatInteger(images.active_builds));
  setText("registryFailedBuildsValue", formatInteger(images.failed_builds));
  setText("registryPendingBuildsValue", formatInteger(images.pending_builds));
  setText("registryOldestBuildValue", asNumber(images.pending_builds) ? formatAge(images.oldest_pending_build_seconds) : "-");
  setText("registryActiveBuildsSummaryValue", formatInteger(images.active_builds));
  setText("registryFailedBuildsSummaryValue", formatInteger(images.failed_builds));
  drawBuilderChart();

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
  const unavailable = asNumber(registry.unavailable_repository_count);
  const partial = unavailable > 0 ? `, ${formatInteger(unavailable)} missing tag lists` : "";
  setText(
    "registryPageHealthDetail",
    `${formatInteger(scanned)} repositories scanned, ${formatInteger(registry.scanned_tag_count)} tags observed${truncated}${partial}`
  );

  const filteredRepos = repos.filter((repo) => registryRepoMatches(repo, buildByTag, filter, query));
  const shownRepos = filteredRepos.slice(0, MAX_REGISTRY_REPOSITORY_ROWS);
  const flattenedTags = flattenRegistryTags(filteredRepos, buildByTag)
    .filter((item) => !query || matchesRegistrySearch(item.searchText, query));
  const summaryParts = [
    `${formatInteger(filteredRepos.length)} repositories`,
    `${formatInteger(flattenedTags.length)} visible tags`,
    `${formatInteger(registryBuilds.length)} pushed builds`,
  ];
  if (unavailable > 0) summaryParts.push(`${formatInteger(unavailable)} missing tag lists`);
  if (query) summaryParts.push(`matching "${query}"`);
  setText("registryPageSummary", summaryParts.join(", "));
  setText(
    "registryRepoSummary",
    `${formatInteger(shownRepos.length)} shown of ${formatInteger(filteredRepos.length)} matching`
  );
  setText(
    "registryTagSummary",
    `${formatInteger(Math.min(flattenedTags.length, 200))} shown of ${formatInteger(flattenedTags.length)} matching`
  );

  if (shownRepos.length === 0) {
    renderEmptyRow(els.registryRepoRows, 5, "No repositories match the current filter");
  } else {
    els.registryRepoRows.replaceChildren(...shownRepos.map((repo) => registryRepoRow(repo, buildByTag)));
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
  appendCell(tr, repo.available === false ? "Unavailable" : (repo.latest_tag || "-"));
  appendCell(tr, formatInteger(buildCount));
  const tagCell = document.createElement("td");
  if (repo.available === false) {
    tagCell.textContent = repo.error || "Tags unavailable";
  } else {
    appendTagChips(tagCell, tags, repo.tags_truncated, repo.tag_count);
  }
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
  const unavailable = repo.available === false;
  item.textContent = unavailable
    ? `${repo.repository || "repository"} (tags unavailable)`
    : `${repo.repository || "repository"}${latest} (${asNumber(repo.tag_count)})`;
  item.title = unavailable ? (repo.error || "Tags unavailable") : tags.join(", ");
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

function appendClassCell(row, value, className, title = "") {
  const td = document.createElement("td");
  td.textContent = value;
  if (className) td.className = className;
  if (title) td.title = title;
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
    if (actions.some((action) => ["create", "scale_up"].includes(actionKind(action)))
      || builderActions.some((action) => actionKind(action) === "scale_up_builder")) return "INFO";
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
    const actionText = actionSummary(actions.concat(builderActions));
    const reasons = Array.isArray(data.reasons) && data.reasons.length ? `, because ${data.reasons.slice(0, 2).join("; ")}` : "";
    return `ready ${asNumber(data.ready_nodes)}, provisioning ${asNumber(data.provisioning_nodes)}, created ${created}, stopped ${stopped}, pending ${formatResources(pending)}, prepared ${formatResources(prepared)}, decision ${actionText}${reasons}`;
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
  if (document.hidden) return;
  if (state.currentPage === "registry") {
    drawBuilderChart();
    return;
  }
  if (state.currentPage !== "overview") return;
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

  drawLineChart("activeNodesChart", [
    { label: "Ready nodes", color: palette.blue, values: state.history.map((p) => p.activeNodes) },
    { label: "Sandbox routes", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.activeSandboxes) },
  ], { min: 0, ticks: 4 });
  drawLineChart("queueDepthChart", [
    { label: "Ready to wake", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.readyWake) },
    { label: "Sandbox creates", color: palette.purple, values: state.history.map((p) => p.pendingSandboxes) },
    { label: "Image builds", color: palette.orange, dashed: true, values: state.history.map((p) => p.pendingBuilds) },
  ], { min: 0, ticks: 4, integerAxis: true });
  drawLineChart("cpuPressureChart", [
    { label: "Actual", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.cpuUtilization) },
    { label: "Reserved", color: palette.blue, dashed: true, values: state.history.map((p) => p.cpuReserved) },
  ], { min: 0, max: 100, ticks: 4, suffix: "%" });
  drawLineChart("memoryPressureChart", [
    { label: "Actual", color: palette.orange, fill: palette.orangeSoft, values: state.history.map((p) => p.memoryUtilization) },
    { label: "Reserved", color: palette.blue, dashed: true, values: state.history.map((p) => p.memoryReserved) },
  ], { min: 0, max: 100, ticks: 4, suffix: "%" });
  drawLineChart("sandboxStartChart", [
    { label: "Model wait p95", color: palette.purple, values: state.history.map((p) => p.modelWaitP95Seconds) },
    { label: "Wake p95", color: palette.green, fill: palette.greenSoft, values: state.history.map((p) => p.wakeP95Seconds) },
  ], { min: 0, ticks: 4 });
}

function drawBuilderChart() {
  if (state.history.length === 0) {
    clearPlot("builderBuildsChart", "Waiting for metrics");
    return;
  }
  drawLineChart("builderBuildsChart", [
    { label: "Active builds", color: palette.orange, fill: palette.orangeSoft, values: state.history.map((p) => p.activeBuilds) },
    { label: "Ready builders", color: palette.blue, dashed: true, values: state.history.map((p) => p.builderNodes) },
  ], { min: 0, ticks: 4, integerAxis: true });
}

function scheduleChartRedraw() {
  if (state.resizeFrame !== null) return;
  state.resizeFrame = window.requestAnimationFrame(() => {
    state.resizeFrame = null;
    redrawCharts();
  });
}

function clearPlot(id, label) {
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const prepared = prepareCanvas(canvas);
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
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const prepared = prepareCanvas(canvas);
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
  const canvas = document.getElementById(id);
  if (!canvas) return;
  const prepared = prepareCanvas(canvas);
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
  const summaries = series.map((line) => {
    const numeric = line.values.filter((value) => value !== null && Number.isFinite(value));
    const latest = [...line.values].reverse().find((value) => value !== null && Number.isFinite(value));
    if (!numeric.length) return `${line.label}: unavailable`;
    return `${line.label}: latest ${formatAxisValue(latest, options)}, range ${formatAxisValue(Math.min(...numeric), options)} to ${formatAxisValue(Math.max(...numeric), options)}`;
  });
  canvas.setAttribute("aria-label", `${summaries.join(". ")}. ${state.history.length} session sample${state.history.length === 1 ? "" : "s"}.`);
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
  const element = els[id] || document.getElementById(id);
  if (!element) return;
  els[id] = element;
  element.textContent = value;
}

function setNavBadge(id, value, mode = "") {
  const element = els[id] || document.getElementById(id);
  if (!element) return;
  element.textContent = typeof value === "number" ? formatInteger(value) : String(value);
  element.className = `nav-badge ${mode ? `nav-badge-${mode}` : ""}`.trim();
}

function setMeterWidth(id, value) {
  const element = els[id] || document.getElementById(id);
  if (!element) return;
  const percent = nullableNumber(value);
  element.style.width = `${Math.max(0, Math.min(100, percent === null ? 0 : percent))}%`;
}

function plainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function firstText(...values) {
  for (const value of values) {
    if (value === null || value === undefined) continue;
    const text = String(value).trim();
    if (text) return text;
  }
  return "";
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
  const disk = asNumber(value.disk_mb);
  return `${cpu} vCPU / ${memory}${disk > 0 ? ` / ${formatMemory(disk)} disk` : ""}`;
}

function resourcePercent(used, effective, key) {
  const numerator = nullableNumber((used || {})[key]);
  const denominator = nullableNumber((effective || {})[key]);
  if (numerator === null || denominator === null || denominator <= 0) return null;
  return Math.max(0, Math.min(100, (numerator / denominator) * 100));
}

function formatMemory(value) {
  const number = nullableNumber(value);
  if (number === null) return "-";
  if (number >= 1024 * 1024) return `${formatNumber(number / (1024 * 1024))} TiB`;
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
