const state = {
  runs: [],
  runDetails: new Map(),
  eventSources: new Map(),
  activeRunId: "",
  historySelection: new Set(),
  currentPage: "setup",
};

const colors = ["#38b2ac", "#f59e0b", "#60a5fa", "#f472b6", "#34d399", "#f87171"];

const chartDefinitions = [
  {
    key: "throughput",
    metrics: [
      { key: "writes_per_sec", label: "writes/s", format: formatOpsPerSecond },
      { key: "reads_per_sec", label: "reads/s", format: formatOpsPerSecond },
    ],
  },
  {
    key: "memory",
    metrics: [{ key: "rss_bytes", label: "memory", format: formatBytes }],
  },
  {
    key: "io",
    metrics: [
      { key: "disk_read_bytes_per_sec", label: "read B/s", format: formatBytesPerSecond },
      { key: "disk_write_bytes_per_sec", label: "write B/s", format: formatBytesPerSecond },
    ],
  },
  {
    key: "disk",
    metrics: [{ key: "disk_usage_bytes", label: "disk usage", format: formatBytes }],
  },
];

const durationPresets = {
  quick: { value: 30, unit: "seconds", help: "Quick checks are good for validating setup and catching obvious regressions fast." },
  benchmark: { value: 5, unit: "minutes", help: "Benchmark runs give enough time for graphs and averages to settle into a representative shape." },
  stability: { value: 30, unit: "minutes", help: "Stability runs are for longer soak testing so you can watch drift, memory growth, and sustained I/O behavior." },
};

const routeMap = {
  "/": "setup",
  "/setup": "setup",
  "/dashboard": "dashboard",
  "/history": "history",
};

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(payload.error || response.statusText);
  }
  return response.json();
}

function routeForPage(page) {
  return page === "setup" ? "/setup" : `/${page}`;
}

function syncPageFromLocation() {
  state.currentPage = routeMap[window.location.pathname] || "setup";
  renderPage();
}

function navigateTo(page) {
  const path = routeForPage(page);
  if (window.location.pathname !== path) {
    window.history.pushState({}, "", path);
  }
  state.currentPage = page;
  renderPage();
}

function renderPage() {
  document.querySelectorAll(".page").forEach((page) => {
    page.classList.toggle("is-active", page.id === `page-${state.currentPage}`);
  });
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    link.classList.toggle("is-active", link.dataset.pageLink === state.currentPage);
  });
}

function setupNavigation() {
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      navigateTo(link.dataset.pageLink);
    });
  });
  window.addEventListener("popstate", syncPageFromLocation);
}

function durationToSeconds(value, unit) {
  const numeric = Number(value);
  if (unit === "hours") {
    return numeric * 3600;
  }
  if (unit === "minutes") {
    return numeric * 60;
  }
  return numeric;
}

function humanizeDuration(seconds) {
  if (seconds % 3600 === 0) {
    return `${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`;
  }
  if (seconds % 60 === 0) {
    return `${seconds / 60} minute${seconds === 60 ? "" : "s"}`;
  }
  return `${seconds} second${seconds === 1 ? "" : "s"}`;
}

function applyDurationPreset(presetKey) {
  const preset = durationPresets[presetKey];
  if (!preset) {
    return;
  }
  const form = document.getElementById("run-form");
  form.duration_value.value = preset.value;
  form.duration_unit.value = preset.unit;
  form.test_profile.value = presetKey;
  document.getElementById("duration-help").textContent = preset.help;
  renderSetupSummary();
}

function setupDurationControls() {
  const form = document.getElementById("run-form");
  const profile = document.getElementById("test-profile");
  profile.addEventListener("change", () => {
    if (profile.value !== "custom") {
      applyDurationPreset(profile.value);
    } else {
      document.getElementById("duration-help").textContent =
        "Custom duration lets you set your own soak window for longer or shorter experiments.";
      renderSetupSummary();
    }
  });

  document.querySelectorAll("[data-duration-preset]").forEach((button) => {
    button.addEventListener("click", () => applyDurationPreset(button.dataset.durationPreset));
  });

  ["input", "change"].forEach((eventName) => {
    form.addEventListener(eventName, () => renderSetupSummary());
  });

  applyDurationPreset("quick");
}

function formConfig(form) {
  const rampValue = form.ramp_schedule.value.trim();
  return {
    run_name: form.run_name.value.trim(),
    engine: form.engine.value,
    durability: form.durability.value,
    scenario: {
      initial_rows: Number(form.initial_rows.value),
      payload_size_bytes: Number(form.payload_size_bytes.value),
      category_count: Number(form.category_count.value),
      range_scan_size: Number(form.range_scan_size.value),
    },
    load: {
      concurrency: Number(form.concurrency.value),
      batch_size: Number(form.batch_size.value),
      duration_secs: durationToSeconds(form.duration_value.value, form.duration_unit.value),
      sample_interval_ms: Number(form.sample_interval_ms.value),
      mix: {
        point_reads: Number(form.point_reads.value),
        range_scans: Number(form.range_scans.value),
        inserts: Number(form.inserts.value),
        updates: Number(form.updates.value),
      },
    },
    ramp_schedule: rampValue ? JSON.parse(rampValue) : [],
  };
}

function renderSetupSummary() {
  const form = document.getElementById("run-form");
  const config = formConfig(form);
  const durationText = humanizeDuration(config.load.duration_secs);
  document.getElementById("duration-preview").textContent = durationText;

  const totalMix =
    config.load.mix.point_reads +
    config.load.mix.range_scans +
    config.load.mix.inserts +
    config.load.mix.updates;

  document.getElementById("setup-summary").innerHTML = `
    <div class="summary-item">
      <span class="label">Engine</span>
      <strong>${config.engine}</strong>
    </div>
    <div class="summary-item">
      <span class="label">Duration</span>
      <strong>${durationText}</strong>
    </div>
    <div class="summary-item">
      <span class="label">Dataset</span>
      <strong>${formatInteger(config.scenario.initial_rows)} rows</strong>
    </div>
    <div class="summary-item">
      <span class="label">Concurrency</span>
      <strong>${formatInteger(config.load.concurrency)}</strong>
    </div>
    <div class="summary-item">
      <span class="label">Payload</span>
      <strong>${formatBytes(config.scenario.payload_size_bytes)}</strong>
    </div>
    <div class="summary-item">
      <span class="label">Mix total</span>
      <strong>${totalMix}%</strong>
    </div>
  `;
}

function renderRecentRuns() {
  const container = document.getElementById("recent-runs");
  container.innerHTML = "";
  const recent = state.runs.slice(0, 5);
  if (!recent.length) {
    container.innerHTML = '<div class="empty-state">No runs yet. Your latest runs will appear here.</div>';
    return;
  }

  recent.forEach((run) => {
    const item = document.createElement("div");
    item.className = "run-item";
    item.innerHTML = `
      <div>
        <strong>${run.run_name}</strong>
        <div class="run-meta">${run.engine} • ${new Date(run.started_at_ms).toLocaleString()}</div>
      </div>
      <span class="status-pill">${run.status}</span>
    `;
    container.appendChild(item);
  });
}

function updateRunCounts() {
  document.getElementById("run-count").textContent = String(state.runs.length);
  const activeCount = state.runs.filter((run) => run.status === "running" || run.status === "pending").length;
  document.getElementById("active-run-count").textContent = String(activeCount);
}

function chooseDefaultActiveRun() {
  const activeRuns = state.runs.filter((run) => run.status === "running" || run.status === "pending");
  if (activeRuns.some((run) => run.run_id === state.activeRunId)) {
    return;
  }
  state.activeRunId = activeRuns[0]?.run_id || "";
}

function chooseDefaultHistoryRuns() {
  const completedRuns = state.runs.filter((run) => run.status !== "running" && run.status !== "pending");
  if (state.historySelection.size || !completedRuns.length) {
    return;
  }
  state.historySelection.add(completedRuns[0].run_id);
}

function renderActiveRunSelect() {
  const select = document.getElementById("active-run-select");
  const activeRuns = state.runs.filter((run) => run.status === "running" || run.status === "pending");
  select.innerHTML = "";

  if (!activeRuns.length) {
    select.innerHTML = '<option value="">No active runs</option>';
    select.value = "";
    return;
  }

  activeRuns.forEach((run) => {
    const option = document.createElement("option");
    option.value = run.run_id;
    option.textContent = `${run.run_name} (${run.engine})`;
    select.appendChild(option);
  });
  select.value = state.activeRunId || activeRuns[0].run_id;
}

function syncLiveControlsFromDetail(detail) {
  if (!detail?.config?.load) {
    return;
  }
  document.getElementById("live-concurrency").value = detail.config.load.concurrency;
  document.getElementById("live-concurrency-value").textContent = detail.config.load.concurrency;
  document.getElementById("live-point").value = detail.config.load.mix.point_reads;
  document.getElementById("live-range").value = detail.config.load.mix.range_scans;
  document.getElementById("live-insert").value = detail.config.load.mix.inserts;
  document.getElementById("live-update").value = detail.config.load.mix.updates;
}

function renderHistoryList() {
  const container = document.getElementById("history-run-list");
  container.innerHTML = "";
  const pastRuns = state.runs.filter((run) => run.status !== "running" && run.status !== "pending");

  if (!pastRuns.length) {
    container.innerHTML = '<div class="empty-state">Completed and interrupted runs will appear here for later comparison.</div>';
    return;
  }

  pastRuns.forEach((run) => {
    const row = document.createElement("label");
    row.className = "run-item";
    row.innerHTML = `
      <input type="checkbox" ${state.historySelection.has(run.run_id) ? "checked" : ""} />
      <div>
        <strong>${run.run_name}</strong>
        <div class="run-meta">${run.engine} • ${new Date(run.started_at_ms).toLocaleString()}</div>
      </div>
      <span class="status-pill">${run.status}</span>
    `;
    row.querySelector("input").addEventListener("change", async (event) => {
      if (event.target.checked) {
        state.historySelection.add(run.run_id);
        await ensureRunDetail(run.run_id);
      } else {
        state.historySelection.delete(run.run_id);
      }
      renderHistorySummary();
      renderHistoryCharts();
    });
    container.appendChild(row);
  });
}

async function refreshRuns() {
  state.runs = await fetchJson("/api/runs");
  updateRunCounts();
  chooseDefaultActiveRun();
  chooseDefaultHistoryRuns();
  renderRecentRuns();
  renderActiveRunSelect();
  renderHistoryList();

  for (const run of state.runs) {
    if ((run.status === "running" || run.status === "pending") && !state.eventSources.has(run.run_id)) {
      attachStream(run.run_id);
    }
  }

  if (state.activeRunId) {
    const detail = await ensureRunDetail(state.activeRunId);
    syncLiveControlsFromDetail(detail);
  }
  for (const runId of state.historySelection) {
    await ensureRunDetail(runId);
  }

  renderDashboardSummary();
  renderDashboardCharts();
  renderHistorySummary();
  renderHistoryCharts();
}

async function ensureRunDetail(runId) {
  if (!runId) {
    return null;
  }
  const detail = await fetchJson(`/api/runs/${runId}`);
  state.runDetails.set(runId, detail);
  return detail;
}

function attachStream(runId) {
  const source = new EventSource(`/api/runs/${runId}/stream`);
  source.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    const detail = state.runDetails.get(runId) || { samples: [], warnings: [], config: null, run_id: runId };
    if (payload.kind === "sample") {
      detail.samples.push(payload.sample);
    }
    if (payload.kind === "finished") {
      detail.summary = payload.summary;
      source.close();
      state.eventSources.delete(runId);
      refreshRuns();
    }
    if (payload.kind === "ready") {
      detail.warnings = payload.warnings || [];
    }
    state.runDetails.set(runId, detail);
    if (runId === state.activeRunId) {
      renderDashboardSummary();
      renderDashboardCharts();
    }
    if (state.historySelection.has(runId)) {
      renderHistorySummary();
      renderHistoryCharts();
    }
  };
  source.onerror = () => {
    source.close();
    state.eventSources.delete(runId);
  };
  state.eventSources.set(runId, source);
}

function getRunById(runId) {
  return state.runs.find((run) => run.run_id === runId);
}

function renderDashboardSummary() {
  const empty = document.getElementById("dashboard-empty");
  const summary = document.getElementById("dashboard-summary");
  const warnings = document.getElementById("dashboard-warnings");
  const detail = state.runDetails.get(state.activeRunId);
  const run = getRunById(state.activeRunId);

  if (!run || !detail) {
    empty.classList.remove("hidden");
    summary.innerHTML = "";
    warnings.innerHTML = "";
    return;
  }

  empty.classList.add("hidden");
  const last = detail.samples.at(-1);
  const durationSecs = last && detail.samples[0]
    ? (last.timestamp_ms - detail.samples[0].timestamp_ms) / 1000
    : 0;
  const configuredDuration = detail.config?.load?.duration_secs || run.summary?.config?.load?.duration_secs || 0;
  summary.innerHTML = `
    <div class="summary-card"><span>Run</span><strong>${run.run_name}</strong></div>
    <div class="summary-card"><span>Status</span><strong>${run.status}</strong></div>
    <div class="summary-card"><span>Elapsed</span><strong>${humanizeDuration(Math.max(1, Math.round(durationSecs || configuredDuration)))}</strong></div>
    <div class="summary-card"><span>Writes/s</span><strong>${formatOpsPerSecond(last?.writes_per_sec || 0)}</strong></div>
    <div class="summary-card"><span>Reads/s</span><strong>${formatOpsPerSecond(last?.reads_per_sec || 0)}</strong></div>
    <div class="summary-card"><span>Memory</span><strong>${formatBytes(last?.rss_bytes || 0)}</strong></div>
    <div class="summary-card"><span>Disk I/O</span><strong>${formatBytesPerSecond((last?.disk_read_bytes_per_sec || 0) + (last?.disk_write_bytes_per_sec || 0))}</strong></div>
    <div class="summary-card"><span>Disk usage</span><strong>${formatBytes(last?.disk_usage_bytes || 0)}</strong></div>
  `;

  const warningItems = detail.warnings || run.summary?.warnings || [];
  warnings.innerHTML = warningItems
    .map((warning) => `<div class="warning-item">${warning}</div>`)
    .join("");
}

function renderHistorySummary() {
  const container = document.getElementById("history-summary");
  const details = [...state.historySelection]
    .map((runId) => ({ run: getRunById(runId), detail: state.runDetails.get(runId) }))
    .filter((entry) => entry.run && entry.detail);

  if (!details.length) {
    container.innerHTML = '<div class="empty-state">Select one or more past runs to compare their summaries and charts.</div>';
    return;
  }

  container.innerHTML = details.map(({ run, detail }) => {
    const summary = run.summary || detail.summary;
    return `
      <div class="summary-card">
        <span>${run.run_name} • ${run.engine}</span>
        <strong>${summary ? formatOpsPerSecond(summary.avg_writes_per_sec) : "No summary"}</strong>
        <div class="run-meta">avg writes/s</div>
        <div class="run-meta">Peak RSS: ${summary ? formatBytes(summary.peak_rss_bytes) : "n/a"}</div>
      </div>
    `;
  }).join("");
}

function renderDashboardCharts() {
  const detail = state.runDetails.get(state.activeRunId);
  const runs = detail ? [detail] : [];
  renderChartGroup("dashboard", runs);
}

function renderHistoryCharts() {
  const runs = [...state.historySelection]
    .map((runId) => state.runDetails.get(runId))
    .filter(Boolean);
  renderChartGroup("history", runs);
}

function renderChartGroup(prefix, runs) {
  chartDefinitions.forEach((definition) => {
    drawChart(`${prefix}-${definition.key}-chart`, runs, definition.metrics);
    renderSeriesStats(`${prefix}-${definition.key}-stats`, runs, definition.metrics);
  });
}

function drawChart(canvasId, runs, metrics) {
  const canvas = document.getElementById(canvasId);
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  const margin = { top: 28, right: 96, bottom: 38, left: 86 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#081015";
  ctx.fillRect(0, 0, width, height);
  ctx.font = "12px IBM Plex Sans, sans-serif";

  if (!runs.length || !runs.some((run) => run.samples?.length)) {
    ctx.fillStyle = "rgba(147, 173, 183, 0.9)";
    ctx.textAlign = "center";
    ctx.fillText("No data to plot yet.", width / 2, height / 2);
    return;
  }

  const maxElapsed = Math.max(
    ...runs.map((run) => {
      const samples = run.samples || [];
      if (samples.length < 2) {
        return 1;
      }
      return (samples.at(-1).timestamp_ms - samples[0].timestamp_ms) / 1000;
    }),
    1,
  );
  const maxValue = niceCeiling(
    Math.max(
      ...runs.flatMap((run) => (run.samples || []).flatMap((sample) =>
        metrics.map((metric) => Number(sample[metric.key] || 0)),
      )),
      1,
    ),
  );

  ctx.strokeStyle = "rgba(147, 173, 183, 0.18)";
  ctx.lineWidth = 1;
  for (let tick = 0; tick <= 4; tick += 1) {
    const y = margin.top + (tick / 4) * plotHeight;
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(width - margin.right, y);
    ctx.stroke();
  }

  for (let tick = 0; tick <= 5; tick += 1) {
    const x = margin.left + (tick / 5) * plotWidth;
    ctx.beginPath();
    ctx.moveTo(x, margin.top);
    ctx.lineTo(x, height - margin.bottom);
    ctx.stroke();
  }

  ctx.strokeStyle = "rgba(230, 241, 244, 0.85)";
  ctx.beginPath();
  ctx.moveTo(margin.left, margin.top);
  ctx.lineTo(margin.left, height - margin.bottom);
  ctx.lineTo(width - margin.right, height - margin.bottom);
  ctx.stroke();

  ctx.fillStyle = "rgba(147, 173, 183, 0.95)";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let tick = 0; tick <= 4; tick += 1) {
    const value = maxValue - (tick / 4) * maxValue;
    const y = margin.top + (tick / 4) * plotHeight;
    ctx.fillText(metrics[0].format(value), margin.left - 12, y);
  }

  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  for (let tick = 0; tick <= 5; tick += 1) {
    const elapsed = (tick / 5) * maxElapsed;
    const x = margin.left + (tick / 5) * plotWidth;
    ctx.fillText(`${elapsed.toFixed(0)}s`, x, height - margin.bottom + 8);
  }
  ctx.fillText("Elapsed time", width / 2, height - 18);

  let seriesIndex = 0;
  runs.forEach((run) => {
    const samples = run.samples || [];
    if (!samples.length) {
      return;
    }
    metrics.forEach((metric, metricIndex) => {
      const color = colors[seriesIndex % colors.length];
      const firstTimestamp = samples[0].timestamp_ms;
      ctx.strokeStyle = color;
      ctx.fillStyle = color;
      ctx.lineWidth = metricIndex === 0 ? 2.6 : 1.8;
      ctx.setLineDash(metricIndex === 0 ? [] : [8, 5]);
      ctx.beginPath();
      samples.forEach((sample, index) => {
        const x = margin.left + (((sample.timestamp_ms - firstTimestamp) / 1000) / maxElapsed) * plotWidth;
        const y = height - margin.bottom - ((Number(sample[metric.key] || 0) / maxValue) * plotHeight);
        if (index === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();
      ctx.setLineDash([]);

      const last = samples.at(-1);
      const lastX = margin.left + (((last.timestamp_ms - firstTimestamp) / 1000) / maxElapsed) * plotWidth;
      const lastY = height - margin.bottom - ((Number(last[metric.key] || 0) / maxValue) * plotHeight);
      ctx.beginPath();
      ctx.arc(lastX, lastY, 3, 0, Math.PI * 2);
      ctx.fill();
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      ctx.fillText(
        `${run.config?.run_name || run.run_id} ${metric.label}: ${metric.format(last[metric.key] || 0)}`,
        Math.min(lastX + 8, width - margin.right + 8),
        Math.max(margin.top + 12, Math.min(lastY, height - margin.bottom - 12)),
      );
      seriesIndex += 1;
    });
  });
}

function renderSeriesStats(containerId, runs, metrics) {
  const container = document.getElementById(containerId);
  if (!runs.length) {
    container.innerHTML = "";
    return;
  }

  const cards = [];
  runs.forEach((run) => {
    metrics.forEach((metric) => {
      const stats = computeStats(run.samples || [], metric.key);
      if (!stats) {
        return;
      }
      cards.push(`
        <div class="series-card">
          <div class="series-label">${run.config?.run_name || run.run_id} • ${metric.label}</div>
          <div class="metric-grid">
            <div><span>Current</span><strong>${metric.format(stats.current)}</strong></div>
            <div><span>Min</span><strong>${metric.format(stats.min)}</strong></div>
            <div><span>Max</span><strong>${metric.format(stats.max)}</strong></div>
            <div><span>Average</span><strong>${metric.format(stats.average)}</strong></div>
            <div><span>Median</span><strong>${metric.format(stats.median)}</strong></div>
            <div><span>Samples</span><strong>${formatInteger(stats.count)}</strong></div>
          </div>
        </div>
      `);
    });
  });
  container.innerHTML = cards.join("");
}

function computeStats(samples, key) {
  const values = samples
    .map((sample) => Number(sample[key] || 0))
    .filter((value) => Number.isFinite(value));
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const count = values.length;
  const median = count % 2 === 0
    ? (sorted[count / 2 - 1] + sorted[count / 2]) / 2
    : sorted[Math.floor(count / 2)];
  const sum = values.reduce((total, value) => total + value, 0);
  return {
    current: values.at(-1),
    min: sorted[0],
    max: sorted.at(-1),
    average: sum / count,
    median,
    count,
  };
}

function niceCeiling(value) {
  if (value <= 1) {
    return 1;
  }
  const exponent = 10 ** Math.floor(Math.log10(value));
  const fraction = value / exponent;
  if (fraction <= 1) return exponent;
  if (fraction <= 2) return 2 * exponent;
  if (fraction <= 5) return 5 * exponent;
  return 10 * exponent;
}

async function sendControl(runId, payload) {
  await fetchJson(`/api/runs/${runId}/control`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
}

function setupForm() {
  const form = document.getElementById("run-form");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const config = formConfig(form);
      const mixTotal =
        config.load.mix.point_reads +
        config.load.mix.range_scans +
        config.load.mix.inserts +
        config.load.mix.updates;
      if (mixTotal !== 100) {
        throw new Error("The workload mix must add up to 100.");
      }
      const created = await fetchJson("/api/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      state.activeRunId = created.run_id;
      await refreshRuns();
      navigateTo("dashboard");
    } catch (error) {
      alert(error.message);
    }
  });

  document.getElementById("jump-to-dashboard").addEventListener("click", () => navigateTo("dashboard"));
}

function setupControls() {
  const runSelect = document.getElementById("active-run-select");
  const concurrency = document.getElementById("live-concurrency");

  runSelect.addEventListener("change", async () => {
    state.activeRunId = runSelect.value;
    if (state.activeRunId) {
      const detail = await ensureRunDetail(state.activeRunId);
      syncLiveControlsFromDetail(detail);
    }
    renderDashboardSummary();
    renderDashboardCharts();
  });

  concurrency.addEventListener("input", () => {
    document.getElementById("live-concurrency-value").textContent = concurrency.value;
  });

  document.getElementById("pause-run").addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "pause" });
    }
  });
  document.getElementById("resume-run").addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "resume" });
    }
  });
  document.getElementById("stop-run").addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "stop" });
    }
  });
  document.getElementById("apply-live-controls").addEventListener("click", async () => {
    if (!state.activeRunId) {
      return;
    }
    const pointReads = Number(document.getElementById("live-point").value);
    const rangeScans = Number(document.getElementById("live-range").value);
    const inserts = Number(document.getElementById("live-insert").value);
    const updates = Number(document.getElementById("live-update").value);
    if (pointReads + rangeScans + inserts + updates !== 100) {
      alert("The live mix must add up to 100.");
      return;
    }
    await sendControl(state.activeRunId, {
      kind: "update_concurrency",
      concurrency: Number(concurrency.value),
    });
    await sendControl(state.activeRunId, {
      kind: "update_mix",
      point_reads: pointReads,
      range_scans: rangeScans,
      inserts,
      updates,
    });
  });
}

function formatInteger(value) {
  return new Intl.NumberFormat().format(Math.round(Number(value) || 0));
}

function formatDecimal(value, decimals = 2) {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: decimals,
    minimumFractionDigits: 0,
  }).format(Number(value) || 0);
}

function formatOpsPerSecond(value) {
  return `${formatDecimal(value, 1)}/s`;
}

function formatBytes(value) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let numeric = Number(value) || 0;
  let unitIndex = 0;
  while (numeric >= 1024 && unitIndex < units.length - 1) {
    numeric /= 1024;
    unitIndex += 1;
  }
  return `${formatDecimal(numeric, numeric < 10 && unitIndex > 0 ? 2 : 1)} ${units[unitIndex]}`;
}

function formatBytesPerSecond(value) {
  return `${formatBytes(value)}/s`;
}

async function boot() {
  setupNavigation();
  setupForm();
  setupControls();
  setupDurationControls();
  syncPageFromLocation();
  renderSetupSummary();
  await refreshRuns();
  setInterval(refreshRuns, 5000);
}

boot().catch((error) => {
  document.getElementById("server-status").textContent = error.message;
});
