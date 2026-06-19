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
    key: "latency",
    metrics: [
      { key: "p50_latency_ms", label: "p50 latency", format: formatLatencyMs },
      { key: "p95_latency_ms", label: "p95 latency", format: formatLatencyMs },
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

const profilePresets = {
  quick: {
    value: 30,
    unit: "seconds",
    help: "Quick checks are good for validating setup and catching obvious regressions fast.",
    initial_rows: 3000,
    concurrency: 1,
    payload_size_bytes: 256,
    category_count: 10,
    range_scan_size: 20,
    batch_size: 1,
    sample_interval_ms: 1000,
    point_reads: 40,
    range_scans: 10,
    inserts: 20,
    updates: 20,
    deletes: 5,
    aggregates: 5
  },
  benchmark: {
    value: 5,
    unit: "minutes",
    help: "Benchmark runs give enough time for graphs and averages to settle into a representative shape.",
    initial_rows: 10000,
    concurrency: 1,
    payload_size_bytes: 256,
    category_count: 10,
    range_scan_size: 20,
    batch_size: 1,
    sample_interval_ms: 1000,
    point_reads: 80,
    range_scans: 10,
    inserts: 5,
    updates: 5,
    deletes: 0,
    aggregates: 0
  },
  stability: {
    value: 30,
    unit: "minutes",
    help: "Stability runs are for longer soak testing so you can watch drift, memory growth, and sustained I/O behavior.",
    initial_rows: 50000,
    concurrency: 1,
    payload_size_bytes: 512,
    category_count: 20,
    range_scan_size: 50,
    batch_size: 5,
    sample_interval_ms: 2000,
    point_reads: 30,
    range_scans: 10,
    inserts: 30,
    updates: 20,
    deletes: 5,
    aggregates: 5
  }
};

const routeMap = {
  "/": "setup",
  "/setup": "setup",
  "/dashboard": "dashboard",
  "/history": "history",
};

function defaultStorageConfig() {
  return {
    sqlite: {
      journal_mode: "wal",
      synchronous: "normal",
    },
    hematite: {
      journal_mode: "wal",
    },
  };
}

function storageFromLegacyDurability(durability) {
  if (durability === "safe") {
    return {
      sqlite: {
        journal_mode: "wal",
        synchronous: "full",
      },
      hematite: {
        journal_mode: "wal",
      },
    };
  }
  if (durability === "fast") {
    return {
      sqlite: {
        journal_mode: "memory",
        synchronous: "off",
      },
      hematite: {
        journal_mode: "rollback",
      },
    };
  }
  return defaultStorageConfig();
}

function isDefaultStorageConfig(storage) {
  return (
    storage?.sqlite?.journal_mode === "wal" &&
    storage?.sqlite?.synchronous === "normal" &&
    storage?.hematite?.journal_mode === "wal"
  );
}

function resolveStorageConfig(config = {}) {
  const defaults = defaultStorageConfig();
  const storage = {
    sqlite: {
      ...defaults.sqlite,
      ...(config.storage?.sqlite || {}),
    },
    hematite: {
      ...defaults.hematite,
      ...(config.storage?.hematite || {}),
    },
  };
  if (config.durability && isDefaultStorageConfig(storage)) {
    return storageFromLegacyDurability(config.durability);
  }
  return storage;
}

function formatSettingValue(value) {
  return String(value || "").replaceAll("_", " ").toUpperCase();
}

function formatEngineSettings(config) {
  if (!config?.engine) {
    return "n/a";
  }
  const storage = resolveStorageConfig(config);
  if (config.engine === "sqlite") {
    return `journal_mode=${formatSettingValue(storage.sqlite.journal_mode)}, synchronous=${formatSettingValue(storage.sqlite.synchronous)}`;
  }
  return `journal_mode=${formatSettingValue(storage.hematite.journal_mode)}`;
}

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
  
  const titles = {
    setup: "Run Setup",
    dashboard: "Live Monitoring",
    history: "Session History"
  };
  document.getElementById("current-page-title").textContent = titles[state.currentPage] || "Hematite Lab";
}

function setupNavigation() {
  document.querySelectorAll("[data-page-link]").forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault();
      navigateTo(link.dataset.pageLink);
    });
  });

  // Tab switching logic
  document.querySelectorAll(".tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const group = btn.parentElement;
      const tabId = btn.dataset.tab;
      
      // Update buttons
      group.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("is-active"));
      btn.classList.add("is-active");
      
      // Update content
      const contentContainer = group.parentElement;
      contentContainer.querySelectorAll(".tab-content").forEach(c => {
        c.style.display = c.id === `tab-${tabId}` ? "block" : "none";
      });
    });
  });

  // Log filtering/search logic
  document.getElementById("log-search-input")?.addEventListener("input", renderDashboardLogs);
  document.getElementById("log-level-filter")?.addEventListener("change", renderDashboardLogs);

  // Modal closing
  document.getElementById("close-modal")?.addEventListener("click", () => {
    document.getElementById("artifact-modal").classList.remove("is-active");
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
  const preset = profilePresets[presetKey];
  if (!preset) {
    return;
  }
  const form = document.getElementById("run-form");
  if (!form) return;
  
  if (form.duration_value) form.duration_value.value = preset.value;
  if (form.duration_unit) form.duration_unit.value = preset.unit;
  if (form.test_profile) form.test_profile.value = presetKey;
  
  if (form.initial_rows) form.initial_rows.value = preset.initial_rows;
  if (form.concurrency) form.concurrency.value = preset.concurrency;
  if (form.payload_size_bytes) form.payload_size_bytes.value = preset.payload_size_bytes;
  if (form.category_count) form.category_count.value = preset.category_count;
  if (form.range_scan_size) form.range_scan_size.value = preset.range_scan_size;
  if (form.batch_size) form.batch_size.value = preset.batch_size;
  if (form.sample_interval_ms) form.sample_interval_ms.value = preset.sample_interval_ms;
  
  if (form.point_reads) form.point_reads.value = preset.point_reads;
  if (form.range_scans) form.range_scans.value = preset.range_scans;
  if (form.inserts) form.inserts.value = preset.inserts;
  if (form.updates) form.updates.value = preset.updates;
  if (form.deletes) form.deletes.value = preset.deletes;
  if (form.aggregates) form.aggregates.value = preset.aggregates;

  const help = document.getElementById("duration-help");
  if (help) help.textContent = preset.help;
  
  renderSetupSummary();
}

function setupDurationControls() {
  const form = document.getElementById("run-form");
  const profile = document.getElementById("test-profile");
  if (profile) {
    profile.addEventListener("change", () => {
      if (profile.value !== "custom") {
        applyDurationPreset(profile.value);
      } else {
        const help = document.getElementById("duration-help");
        if (help) help.textContent = "Custom duration lets you set your own soak window for longer or shorter experiments.";
        renderSetupSummary();
      }
    });
  }

  document.querySelectorAll("[data-duration-preset]").forEach((button) => {
    button.addEventListener("click", () => applyDurationPreset(button.dataset.durationPreset));
  });

  ["input", "change"].forEach((eventName) => {
    form?.addEventListener(eventName, (event) => {
      if (event.target && event.target.id !== "test-profile" && event.target.name !== "test_profile" && event.target.name) {
        const formInputs = [
          "duration_value", "duration_unit", "initial_rows", "payload_size_bytes",
          "concurrency", "category_count", "range_scan_size", "batch_size",
          "sample_interval_ms", "point_reads", "range_scans", "inserts",
          "updates", "deletes", "aggregates"
        ];
        if (formInputs.includes(event.target.name)) {
          const profile = document.getElementById("test-profile");
          if (profile) profile.value = "custom";
        }
      }
      renderSetupSummary();
    });
  });

  applyDurationPreset("quick");
}

function parseRampSchedule(raw, options = {}) {
  const trimmed = raw.trim();
  if (!trimmed) {
    return { value: [], error: "" };
  }
  try {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) {
      throw new Error("Ramp schedule must be a JSON array.");
    }
    return { value: parsed, error: "" };
  } catch (error) {
    if (options.strict) {
      throw new Error(`Ramp schedule must be valid JSON. ${error.message}`);
    }
    return { value: [], error: "Ramp schedule must be valid JSON before launch." };
  }
}

function readFormState(form, options = {}) {
  const rampResult = parseRampSchedule(form.ramp_schedule?.value || "", options);
  return {
    config: {
      run_name: form.run_name?.value.trim() || "unnamed",
      engine: form.engine?.value || "hematite",
      scenario: {
        initial_rows: Number(form.initial_rows?.value || 0),
        payload_size_bytes: Number(form.payload_size_bytes?.value || 0),
        category_count: Number(form.category_count?.value || 0),
        range_scan_size: Number(form.range_scan_size?.value || 0),
      },
      load: {
        concurrency: Number(form.concurrency?.value || 0),
        batch_size: Number(form.batch_size?.value || 0),
        duration_secs: durationToSeconds(form.duration_value?.value || 0, form.duration_unit?.value || "seconds"),
        sample_interval_ms: Number(form.sample_interval_ms?.value || 0),
        mix: {
          point_reads: Number(form.point_reads?.value || 0),
          range_scans: Number(form.range_scans?.value || 0),
          inserts: Number(form.inserts?.value || 0),
          updates: Number(form.updates?.value || 0),
          deletes: Number(form.deletes?.value || 0),
          aggregates: Number(form.aggregates?.value || 0),
        },
      },
      ramp_schedule: rampResult.value,
      storage: {
        sqlite: {
          journal_mode: form.sqlite_journal_mode?.value || "wal",
          synchronous: form.sqlite_synchronous?.value || "normal",
        },
        hematite: {
          journal_mode: form.hematite_journal_mode?.value || "wal",
        },
      },
      profiling: (function () {
        const perfEnabled = document.getElementById("run-worker-perf")?.checked;
        const straceEnabled = document.getElementById("run-worker-strace")?.checked;
        const perfOut = document.getElementById("run-perf-output")?.value || "";
        const perfFreq = document.getElementById("run-perf-freq")?.value || "";
        const perfGen = document.getElementById("run-perf-generate-flamegraph")?.checked;
        const straceOut = document.getElementById("run-worker-strace-output")?.value || "";
        const anySet = perfEnabled || straceEnabled || perfOut !== "" || perfFreq !== "" || straceOut !== "";
        if (!anySet) return null;
        return {
          worker_perf: perfEnabled ? true : null,
          worker_perf_generate_flamegraph: perfGen === undefined ? null : perfGen,
          worker_perf_freq_hz: perfFreq === "" ? null : Number(perfFreq),
          worker_perf_output: perfOut === "" ? null : perfOut,
          worker_strace: straceEnabled ? true : null,
          worker_strace_output: straceOut === "" ? null : straceOut,
        };
      })(),
    },
    rampError: rampResult.error,
  };
}

function syncEngineSettingPanels(engine) {
  document.querySelectorAll("[data-engine-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.enginePanel !== engine;
  });
}

function renderSetupSummary() {
  const form = document.getElementById("run-form");
  const validation = document.getElementById("setup-validation");
  const { config, rampError } = readFormState(form);
  syncEngineSettingPanels(config.engine);
  const durationText = humanizeDuration(config.load.duration_secs);
  document.getElementById("duration-preview").textContent = durationText;

  const totalMix =
    config.load.mix.point_reads +
    config.load.mix.range_scans +
    config.load.mix.inserts +
    config.load.mix.updates +
    config.load.mix.deletes +
    config.load.mix.aggregates;

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
      <span class="label">Settings</span>
      <strong>${formatEngineSettings(config)}</strong>
    </div>
    <div class="summary-item">
      <span class="label">Mix total</span>
      <strong>${totalMix}%</strong>
    </div>
  `;

  const setupMixStatusEl = document.getElementById("setup-mix-status");
  if (setupMixStatusEl) {
    setupMixStatusEl.textContent = `${totalMix}%`;
    if (totalMix === 100) {
      setupMixStatusEl.className = "mix-validation-status is-valid";
    } else {
      setupMixStatusEl.className = "mix-validation-status is-invalid";
    }
  }

  const submitBtn = form.querySelector("button[type='submit']");
  if (submitBtn) {
    const isInvalid = totalMix !== 100 || !!rampError;
    submitBtn.disabled = isInvalid;
    submitBtn.style.opacity = isInvalid ? "0.5" : "1";
    submitBtn.style.pointerEvents = isInvalid ? "none" : "auto";
  }

  const issues = [];
  if (rampError) {
    issues.push({ text: rampError, error: true });
  }
  if (totalMix !== 100) {
    issues.push({ text: `Workload mix currently adds up to ${totalMix}%, not 100%.`, error: true });
  }
  validation.innerHTML = issues
    .map((issue) => `<div class="warning-item${issue.error ? " is-error" : ""}">${issue.text}</div>`)
    .join("");
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
      <span class="${statusPillClass(run.status)}">${run.status}</span>
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
  const load = detail?.effective_config?.load || detail?.config?.load;
  if (!load) {
    return;
  }
  syncConcurrencyControls(load.concurrency);
  document.getElementById("live-point").value = load.mix.point_reads;
  document.getElementById("live-range").value = load.mix.range_scans;
  document.getElementById("live-insert").value = load.mix.inserts;
  document.getElementById("live-update").value = load.mix.updates;
  document.getElementById("live-delete").value = load.mix.deletes;
  document.getElementById("live-aggregate").value = load.mix.aggregates;
  updateLiveMixStatus();
}

function updateLiveMixStatus() {
  const pointReads = Number(document.getElementById("live-point")?.value || 0);
  const rangeScans = Number(document.getElementById("live-range")?.value || 0);
  const inserts = Number(document.getElementById("live-insert")?.value || 0);
  const updates = Number(document.getElementById("live-update")?.value || 0);
  const deletes = Number(document.getElementById("live-delete")?.value || 0);
  const aggregates = Number(document.getElementById("live-aggregate")?.value || 0);

  const total = pointReads + rangeScans + inserts + updates + deletes + aggregates;
  const statusEl = document.getElementById("live-mix-status");
  const applyBtn = document.getElementById("apply-live-controls");

  if (statusEl) {
    statusEl.textContent = `${total}%`;
    if (total === 100) {
      statusEl.className = "mix-validation-status is-valid";
    } else {
      statusEl.className = "mix-validation-status is-invalid";
    }
  }

  if (applyBtn) {
    applyBtn.disabled = total !== 100;
    applyBtn.style.opacity = total !== 100 ? "0.5" : "1";
    applyBtn.style.pointerEvents = total !== 100 ? "none" : "auto";
  }
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
      <span class="${statusPillClass(run.status)}">${run.status}</span>
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
      renderHistoryLogs();
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
    try {
      // Only fetch detail for active run if we don't already have an SSE stream
      // providing live samples. If the stream is attached, it's the live source;
      // avoid overwriting its in-memory samples with a potentially-stale HTTP fetch.
      if (!state.eventSources.has(state.activeRunId)) {
        const detail = await ensureRunDetail(state.activeRunId);
        if (detail) syncLiveControlsFromDetail(detail);
      } else {
        // Still sync the live controls from whatever we have in memory.
        const detail = state.runDetails.get(state.activeRunId);
        if (detail) syncLiveControlsFromDetail(detail);
      }
    } catch (e) {
      console.warn("failed to ensure active run detail", e);
    }
  }
  for (const runId of state.historySelection) {
    try {
      await ensureRunDetail(runId);
    } catch (e) {
      console.warn(`failed to ensure history run detail for ${runId}`, e);
    }
  }

  renderDashboardSummary();
  renderDashboardCharts();
  renderDashboardLogs();
  renderHistorySummary();
  renderHistoryCharts();
  renderHistoryLogs();
}

async function ensureRunDetail(runId) {
  if (!runId) {
    return null;
  }
  const fetched = await fetchJson(`/api/runs/${runId}`);
  const existing = state.runDetails.get(runId);
  if (existing && existing.samples && existing.samples.length > 0) {
    // Merge: keep in-memory SSE samples; append any server samples not yet in memory.
    // Server samples come from metrics.jsonl (flushed to disk) — some recent ones
    // may only be in memory via SSE. Deduplicate by timestamp_ms.
    const inMemoryTs = new Set(existing.samples.map((s) => s.timestamp_ms));
    const newFromDisk = (fetched.samples || []).filter((s) => !inMemoryTs.has(s.timestamp_ms));
    fetched.samples = [...newFromDisk, ...existing.samples].sort(
      (a, b) => a.timestamp_ms - b.timestamp_ms
    );
    // Preserve in-memory logs that may not be flushed to disk yet.
    const inMemoryLogTs = new Set((existing.logs || []).map((l) => l.timestamp_ms));
    const newLogsFromDisk = (fetched.logs || []).filter((l) => !inMemoryLogTs.has(l.timestamp_ms));
    fetched.logs = [...newLogsFromDisk, ...(existing.logs || [])].sort(
      (a, b) => a.timestamp_ms - b.timestamp_ms
    );
  }
  state.runDetails.set(runId, fetched);
  return fetched;
}

function attachStream(runId, retryDelayMs = 1000) {
  const source = new EventSource(`/api/runs/${runId}/stream`);
  source.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    const detail = state.runDetails.get(runId) || {
      samples: [],
      logs: [],
      warnings: [],
      error_messages: [],
      control_events: [],
      config: null,
      effective_config: null,
      run_id: runId,
    };
    if (payload.kind === "sample") {
      detail.samples.push(payload.sample);
    }
    if (payload.kind === "log") {
      detail.logs = [...(detail.logs || []), payload.entry];
    }
    if (payload.kind === "control_applied") {
      detail.control_events = [...(detail.control_events || []), payload.event];
      detail.effective_config = payload.effective_config;
      if (runId === state.activeRunId) {
        syncLiveControlsFromDetail(detail);
      }
    }
    if (payload.kind === "finished") {
      detail.summary = payload.summary;
      detail.effective_config = payload.summary.final_config || detail.effective_config;
      detail.control_events = payload.summary.control_events || detail.control_events || [];
      detail.error_messages = payload.summary.error_messages || [];
      source.close();
      state.eventSources.delete(runId);
      refreshRuns();
    }
    if (payload.kind === "ready") {
      detail.warnings = payload.warnings || [];
    }
    if (payload.kind === "failed") {
      detail.error_messages = [...(detail.error_messages || []), payload.message];
      source.close();
      state.eventSources.delete(runId);
      refreshRuns();
    }
    state.runDetails.set(runId, detail);
    if (runId === state.activeRunId) {
      renderDashboardSummary();
      renderDashboardCharts();
      renderDashboardLogs();
    }
    if (state.historySelection.has(runId)) {
      renderHistorySummary();
      renderHistoryCharts();
      renderHistoryLogs();
    }
  };
  source.onerror = () => {
    source.close();
    state.eventSources.delete(runId);
    // Reconnect with exponential backoff if the run is still active.
    const run = getRunById(runId);
    if (run && (run.status === "running" || run.status === "pending")) {
      const nextDelay = Math.min((retryDelayMs || 1000) * 2, 30_000);
      setTimeout(() => attachStream(runId, nextDelay), retryDelayMs || 1000);
    }
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
  const configuredDuration =
    detail.effective_config?.load?.duration_secs ||
    detail.config?.load?.duration_secs ||
    run.summary?.final_config?.load?.duration_secs ||
    run.summary?.config?.load?.duration_secs ||
    0;
  const logs = detail.logs || run.summary?.recent_logs || [];
  const latestLog = logs.at(-1);
  const logCount = detail.summary?.log_count || run.summary?.log_count || logs.length;
  summary.innerHTML = `
    <div class="summary-card"><span>Run</span><strong>${run.run_name}</strong></div>
    <div class="summary-card"><span>Status</span><strong>${run.status}</strong></div>
    <div class="summary-card"><span>Elapsed</span><strong>${humanizeDuration(Math.max(1, Math.round(durationSecs || configuredDuration)))}</strong></div>
    <div class="summary-card"><span>Settings</span><strong>${formatEngineSettings(detail.effective_config || detail.config || run.summary?.final_config || run.summary?.config)}</strong></div>
    <div class="summary-card"><span>Writes/s</span><strong>${formatOpsPerSecond(last?.writes_per_sec || 0)}</strong></div>
    <div class="summary-card"><span>Reads/s</span><strong>${formatOpsPerSecond(last?.reads_per_sec || 0)}</strong></div>
    <div class="summary-card"><span>P95 latency</span><strong>${formatLatencyMs(last?.p95_latency_ms || 0)}</strong></div>
    <div class="summary-card"><span>Memory</span><strong>${formatBytes(last?.rss_bytes || 0)}</strong></div>
    <div class="summary-card"><span>Disk I/O</span><strong>${formatBytesPerSecond((last?.disk_read_bytes_per_sec || 0) + (last?.disk_write_bytes_per_sec || 0))}</strong></div>
    <div class="summary-card"><span>Disk usage</span><strong>${formatBytes(last?.disk_usage_bytes || 0)}</strong></div>
    <div class="summary-card"><span>Total errors</span><strong>${formatInteger(totalSampleErrors(detail.samples || []))}</strong></div>
    <div class="summary-card"><span>Log events</span><strong>${formatInteger(logCount)}</strong></div>
  `;

  warnings.innerHTML = renderMessageItems(
    [...(detail.warnings || run.summary?.warnings || [])],
    [...(detail.error_messages || run.summary?.error_messages || [])],
  );
  const artifacts = detail?.summary?.artifact_paths || run.summary?.artifact_paths;
  if (artifacts) {
    const artifactContainer = document.getElementById("dashboard-artifacts");
    artifactContainer.innerHTML = "";
    
    Object.entries(artifacts).forEach(([key, path]) => {
      if (!path) return;
      
      if (key === "strace_paths" && Array.isArray(path)) {
        path.forEach(p => renderArtifactCard(artifactContainer, run.run_id, p, "Strace Output"));
      } else if (typeof path === "string") {
        const label = key.replace("_path", "").replace("_", " ");
        renderArtifactCard(artifactContainer, run.run_id, path, label);
      }
    });
  }

  if (latestLog) {
    warnings.innerHTML += `
      <div class="warning-item">
        <strong>Latest event</strong>
        <div class="run-meta">${formatLogTimestamp(latestLog.timestamp_ms)} • ${latestLog.level} • ${latestLog.source}</div>
        <div>${escapeHtml(latestLog.message)}</div>
      </div>
    `;
  }
}

function renderArtifactCard(container, runId, path, label) {
  const fname = path.split("/").pop();
  const card = document.createElement("div");
  card.className = "artifact-card";
  card.innerHTML = `
    <div class="run-meta">${label}</div>
    <strong>${escapeHtml(fname)}</strong>
    <div class="button-row" style="margin-top: 10px;">
      <button class="ghost small-btn" onclick="viewArtifact('${runId}', '${encodeURIComponent(fname)}', '${label}')">View Inline</button>
      <a href="/api/runs/${runId}/artifact?name=${encodeURIComponent(fname)}" target="_blank" class="nav-item" style="padding: 4px 8px; font-size: 0.8rem;">Download</a>
    </div>
  `;
  container.appendChild(card);
}

async function viewArtifact(runId, filename, label) {
  const modal = document.getElementById("artifact-modal");
  const content = document.getElementById("modal-content");
  const title = document.getElementById("modal-title");
  
  title.textContent = `${label}: ${decodeURIComponent(filename)}`;
  content.innerHTML = "<p>Loading artifact...</p>";
  modal.classList.add("is-active");
  
  try {
    const url = `/api/runs/${runId}/artifact?name=${filename}`;
    const res = await fetch(url);
    if (filename.endsWith(".svg")) {
      const svg = await res.text();
      content.innerHTML = svg;
    } else {
      const text = await res.text();
      content.innerHTML = `
        <div style="height: 100%; display: flex; flex-direction: column;">
          <pre style="flex: 1; margin: 0; padding: 20px; background: rgba(0,0,0,0.4); color: #e2e8f0; font-family: 'JetBrains Mono', monospace; font-size: 0.85rem; line-height: 1.5; overflow: auto; border-radius: 8px; border: 1px solid rgba(255,255,255,0.1); white-space: pre-wrap; word-break: break-all;">${escapeHtml(text)}</pre>
        </div>
      `;
    }
  } catch (e) {
    content.innerHTML = `<p class="error">Failed to load artifact: ${e.message}</p>`;
  }
}

function renderHistoryComparisonTable(details) {
  const container = document.getElementById("history-comparison-table-container");
  if (!container) return;

  if (details.length < 2) {
    container.innerHTML = "";
    return;
  }

  // Find max values for writes/s and reads/s to highlight the winner!
  let maxWrites = 0;
  let maxReads = 0;
  details.forEach(({ run, detail }) => {
    const summary = run.summary || detail.summary;
    if (summary) {
      if (summary.avg_writes_per_sec > maxWrites) maxWrites = summary.avg_writes_per_sec;
      if (summary.avg_reads_per_sec > maxReads) maxReads = summary.avg_reads_per_sec;
    }
  });

  let html = `
    <table class="comparison-table">
      <thead>
        <tr>
          <th>Metric</th>
          ${details.map(({ run }) => `
            <th>
              <div class="comp-th-title">${escapeHtml(run.run_name)}</div>
              <div class="comp-th-subtitle">${run.engine}</div>
            </th>
          `).join("")}
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>Status</td>
          ${details.map(({ run }) => `<td><span class="${statusPillClass(run.status)}">${run.status}</span></td>`).join("")}
        </tr>
        <tr>
          <td>Avg Writes/s</td>
          ${details.map(({ run, detail }) => {
            const summary = run.summary || detail.summary;
            if (!summary) return "<td>n/a</td>";
            const val = summary.avg_writes_per_sec;
            const isWinner = val === maxWrites && val > 0;
            return `<td class="${isWinner ? "winner-cell" : ""}">${formatOpsPerSecond(val)} ${isWinner ? "🏆" : ""}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>Avg Reads/s</td>
          ${details.map(({ run, detail }) => {
            const summary = run.summary || detail.summary;
            if (!summary) return "<td>n/a</td>";
            const val = summary.avg_reads_per_sec;
            const isWinner = val === maxReads && val > 0;
            return `<td class="${isWinner ? "winner-cell" : ""}">${formatOpsPerSecond(val)} ${isWinner ? "🏆" : ""}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>P95 Latency</td>
          ${details.map(({ detail }) => {
            const avgP95 = computeStats(detail.samples || [], "p95_latency_ms");
            return `<td>${avgP95 ? formatLatencyMs(avgP95.average) : "n/a"}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>Peak RSS Memory</td>
          ${details.map(({ run, detail }) => {
            const summary = run.summary || detail.summary;
            return `<td>${summary ? formatBytes(summary.peak_rss_bytes) : "n/a"}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>Peak Disk Space</td>
          ${details.map(({ run, detail }) => {
            const summary = run.summary || detail.summary;
            return `<td>${summary ? formatBytes(summary.peak_disk_usage_bytes) : "n/a"}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>Errors</td>
          ${details.map(({ detail }) => {
            const totalErrors = totalSampleErrors(detail.samples || []);
            return `<td>${formatInteger(totalErrors)}</td>`;
          }).join("")}
        </tr>
        <tr>
          <td>Concurrency</td>
          ${details.map(({ detail }) => {
            const effectiveConfig = detail.effective_config || detail.config;
            return `<td>${formatInteger(effectiveConfig?.load?.concurrency || 0)}</td>`;
          }).join("")}
        </tr>
      </tbody>
    </table>
  `;

  container.innerHTML = html;
}

function startEditRunName(runId) {
  state.editingRunId = runId;
  state.editingRunDraftName = null; // fresh edit, no draft yet
  renderHistorySummary();
  // Focus the newly rendered input
  const input = document.getElementById(`edit-name-input-${runId}`);
  if (input) {
    input.focus();
    input.select();
  }
}

function cancelEditRunName() {
  state.editingRunId = null;
  state.editingRunDraftName = null;
  renderHistorySummary();
}

async function saveRunName(runId) {
  const input = document.getElementById(`edit-name-input-${runId}`);
  if (!input) return;
  const newName = input.value.trim();
  if (!newName) {
    alert("Run name cannot be empty.");
    return;
  }

  try {
    const res = await fetch(`/api/runs/${runId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ run_name: newName }),
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    state.editingRunId = null;
    state.editingRunDraftName = null;
    await refreshRuns();
    renderHistorySummary();
  } catch (e) {
    alert(`Failed to save run name: ${e.message}`);
  }
}

async function confirmDeleteRun(runId, runName) {
  if (!confirm(`Are you sure you want to permanently delete run "${runName}"? This will delete all its logs and artifacts from disk.`)) {
    return;
  }

  try {
    const res = await fetch(`/api/runs/${runId}`, {
      method: "DELETE",
    });
    if (!res.ok) {
      throw new Error(await res.text());
    }
    state.historySelection.delete(runId);
    if (state.activeRunId === runId) {
      state.activeRunId = "";
    }
    state.editingRunId = null;
    await refreshRuns();
    renderHistorySummary();
  } catch (e) {
    alert(`Failed to delete run: ${e.message}`);
  }
}

window.startEditRunName = startEditRunName;
window.cancelEditRunName = cancelEditRunName;
window.saveRunName = saveRunName;
window.confirmDeleteRun = confirmDeleteRun;
window.viewArtifact = viewArtifact;


function renderHistorySummary() {
  const container = document.getElementById("history-summary");
  const details = [...state.historySelection]
    .map((runId) => ({ run: getRunById(runId), detail: state.runDetails.get(runId) }))
    .filter((entry) => entry.run && entry.detail);

  if (!details.length) {
    container.innerHTML = '<div class="empty-state">Select one or more past runs to compare their summaries and charts.</div>';
    const compTableContainer = document.getElementById("history-comparison-table-container");
    if (compTableContainer) compTableContainer.innerHTML = "";
    return;
  }

  // Render the side-by-side comparison table
  renderHistoryComparisonTable(details);

  // If an edit is in progress, snapshot the current typed value before
  // we blow away the DOM. We'll restore it after innerHTML is set.
  let draftValue = null;
  let draftSelStart = null;
  let draftSelEnd = null;
  if (state.editingRunId) {
    const existingInput = document.getElementById(`edit-name-input-${state.editingRunId}`);
    if (existingInput) {
      draftValue = existingInput.value;
      draftSelStart = existingInput.selectionStart;
      draftSelEnd = existingInput.selectionEnd;
    }
  }

  container.innerHTML = details.map(({ run, detail }) => {
    const summary = run.summary || detail.summary;
    const effectiveConfig = summary?.final_config || detail.effective_config || detail.config;
    const avgP95 = computeStats(detail.samples || [], "p95_latency_ms");
    const totalErrors = totalSampleErrors(detail.samples || []);
    const logs = detail.logs || summary?.recent_logs || [];
    const latestLog = logs.at(-1);
    const logCount = summary?.log_count || logs.length;
    const isEditing = state.editingRunId === run.run_id;

    return `
      <div class="summary-card">
        <div style="display: flex; justify-content: space-between; align-items: flex-start; width: 100%; margin-bottom: 12px;">
          ${isEditing ? `
            <div style="display: flex; gap: 8px; align-items: center; width: 100%;">
              <input type="text" id="edit-name-input-${run.run_id}" value="${escapeHtml(run.run_name)}" style="padding: 4px 8px; font-size: 0.9rem; flex: 1; min-width: 0;" />
              <button class="small-btn" onclick="saveRunName('${run.run_id}')">Save</button>
              <button class="ghost small-btn" onclick="cancelEditRunName()">Cancel</button>
            </div>
          ` : `
            <div style="display: flex; flex-direction: column; gap: 6px; min-width: 0; flex: 1;">
              <span>${escapeHtml(run.run_name)} • ${run.engine}</span>
              <strong>${summary ? formatOpsPerSecond(summary.avg_writes_per_sec) : "No summary yet"}</strong>
              <div class="run-meta">avg writes/s</div>
            </div>
            <div class="run-actions-row" style="display: flex; gap: 6px; margin-left: 8px;">
              <button class="ghost small-btn" onclick="startEditRunName('${run.run_id}')">Rename</button>
              <button class="danger small-btn" onclick="confirmDeleteRun('${run.run_id}', '${escapeHtml(run.run_name)}')">Delete</button>
            </div>
          `}
        </div>
        <span class="${statusPillClass(run.status)}">${run.status}</span>
        <div class="metric-grid full">
          <div><span>Avg reads</span><strong>${summary ? formatOpsPerSecond(summary.avg_reads_per_sec) : "n/a"}</strong></div>
          <div><span>Avg p95</span><strong>${avgP95 ? formatLatencyMs(avgP95.average) : "n/a"}</strong></div>
          <div><span>Errors</span><strong>${formatInteger(totalErrors)}</strong></div>
          <div><span>Peak RSS</span><strong>${summary ? formatBytes(summary.peak_rss_bytes) : "n/a"}</strong></div>
          <div><span>Peak disk</span><strong>${summary ? formatBytes(summary.peak_disk_usage_bytes) : "n/a"}</strong></div>
          <div><span>Final concurrency</span><strong>${formatInteger(effectiveConfig?.load?.concurrency || 0)}</strong></div>
          <div><span>Log events</span><strong>${formatInteger(logCount)}</strong></div>
        </div>
        <div class="run-meta">Storage: ${formatEngineSettings(effectiveConfig)}</div>
        <div class="run-meta">Final mix: ${formatMix(effectiveConfig?.load?.mix)}</div>
        ${latestLog ? `<div class="run-meta">Latest event: ${escapeHtml(latestLog.message)}</div>` : ""}
        ${(() => {
        try {
          const ap = summary?.artifact_paths || detail?.summary?.artifact_paths;
          if (!ap) return "";
          const parts = [];
          if (ap.flamegraph_path) {
            const fn = ap.flamegraph_path.split("/").pop();
            parts.push(`
              <div class="artifact-item" style="display: flex; gap: 8px; align-items: center; justify-content: space-between; background: rgba(255,255,255,0.02); padding: 8px 12px; border-radius: 8px; border: 1px solid var(--panel-border); width: 100%;">
                <span class="run-meta" style="font-weight: 500; font-size: 0.8rem; color: var(--text);">Flamegraph</span>
                <div style="display: flex; gap: 6px;">
                  <button class="ghost small-btn" style="padding: 4px 8px; font-size: 0.75rem;" onclick="viewArtifact('${run.run_id}', '${encodeURIComponent(fn)}', 'Flamegraph')">View Inline</button>
                  <a href="/api/runs/${run.run_id}/artifact?name=${encodeURIComponent(fn)}" target="_blank" class="small-btn" style="padding: 4px 8px; font-size: 0.75rem; background: rgba(255,255,255,0.06); border-radius: 8px; text-decoration: none; color: var(--text); display: inline-flex; align-items: center;">Download</a>
                </div>
              </div>
            `);
          }
          if (ap.perf_data_path) {
            const fn = ap.perf_data_path.split("/").pop();
            parts.push(`
              <div class="artifact-item" style="display: flex; gap: 8px; align-items: center; justify-content: space-between; background: rgba(255,255,255,0.02); padding: 8px 12px; border-radius: 8px; border: 1px solid var(--panel-border); width: 100%;">
                <span class="run-meta" style="font-weight: 500; font-size: 0.8rem; color: var(--text);">Perf Data</span>
                <div style="display: flex; gap: 6px;">
                  <a href="/api/runs/${run.run_id}/artifact?name=${encodeURIComponent(fn)}" target="_blank" class="small-btn" style="padding: 4px 8px; font-size: 0.75rem; background: rgba(255,255,255,0.06); border-radius: 8px; text-decoration: none; color: var(--text); display: inline-flex; align-items: center;">Download</a>
                </div>
              </div>
            `);
          }
          if (ap.strace_paths && ap.strace_paths.length) {
            ap.strace_paths.forEach((p) => {
              const fn = p.split("/").pop();
              parts.push(`
                <div class="artifact-item" style="display: flex; gap: 8px; align-items: center; justify-content: space-between; background: rgba(255,255,255,0.02); padding: 8px 12px; border-radius: 8px; border: 1px solid var(--panel-border); width: 100%;">
                  <span class="run-meta" style="font-weight: 500; font-size: 0.8rem; color: var(--text); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 180px;" title="${escapeHtml(fn)}">${escapeHtml(fn)}</span>
                  <div style="display: flex; gap: 6px;">
                    <button class="ghost small-btn" style="padding: 4px 8px; font-size: 0.75rem;" onclick="viewArtifact('${run.run_id}', '${encodeURIComponent(fn)}', 'Strace Output')">View Inline</button>
                    <a href="/api/runs/${run.run_id}/artifact?name=${encodeURIComponent(fn)}" target="_blank" class="small-btn" style="padding: 4px 8px; font-size: 0.75rem; background: rgba(255,255,255,0.06); border-radius: 8px; text-decoration: none; color: var(--text); display: inline-flex; align-items: center;">Download</a>
                  </div>
                </div>
              `);
            });
          }
          if (parts.length) {
            return `
                <div class="panel inset-panel" style="margin-top: 16px;">
                  <div class="section-head compact">
                    <div>
                      <p class="section-kicker">Artifacts</p>
                      <h3>Run Artifacts</h3>
                    </div>
                  </div>
                  <div class="artifact-list" style="display: flex; flex-direction: column; gap: 8px; width: 100%;">${parts.join("")}</div>
                  ${ap.flamegraph_path ? `<div class="flamegraph-embed" style="cursor: pointer;" onclick="viewArtifact('${run.run_id}', '${encodeURIComponent(ap.flamegraph_path.split("/").pop())}', 'Flamegraph')"><img src="/api/runs/${run.run_id}/artifact?name=${encodeURIComponent(ap.flamegraph_path.split("/").pop())}" alt="flamegraph" /></div>` : ""}
                </div>
              `;
          }
          return "";
        } catch (e) {
          console.warn("failed to render artifacts", e);
          return "";
        }
      })()}
      </div>
    `;
  }).join("");

  // Restore the user's in-progress draft after the DOM was rebuilt.
  if (state.editingRunId && draftValue !== null) {
    const restoredInput = document.getElementById(`edit-name-input-${state.editingRunId}`);
    if (restoredInput) {
      restoredInput.value = draftValue;
      // Restore caret so the cursor doesn't jump to the end.
      restoredInput.setSelectionRange(draftSelStart, draftSelEnd);
    }
  }
}

function renderDashboardCharts() {
  const detail = state.runDetails.get(state.activeRunId);
  const runs = detail ? [detail] : [];
  renderChartGroup("dashboard", runs);
}

function renderDashboardLogs() {
  const container = document.getElementById("dashboard-logs");
  const detail = state.runDetails.get(state.activeRunId);
  const logs = detail?.logs || detail?.summary?.recent_logs || [];
  
  const clearedTs = state.clearedLogsTimestamp || 0;
  const activeLogs = logs.filter(log => log.timestamp_ms > clearedTs);

  if (!activeLogs.length) {
    container.innerHTML = '<div class="empty-state">No run events yet.</div>';
    return;
  }

  const searchTerm = document.getElementById("log-search-input")?.value.toLowerCase() || "";
  const levelFilter = document.getElementById("log-level-filter")?.value || "all";

  const filteredLogs = activeLogs.filter(log => {
    const matchesSearch = log.message.toLowerCase().includes(searchTerm);
    const matchesLevel = levelFilter === "all" || log.level.toLowerCase() === levelFilter;
    return matchesSearch && matchesLevel;
  });

  if (filteredLogs.length === 0) {
    container.innerHTML = '<div class="empty-state">No logs match your filters.</div>';
    return;
  }

  container.innerHTML = filteredLogs.slice(-200).map(log => `
    <div class="log-line">
      <span class="log-ts">${formatLogTimestamp(log.timestamp_ms)}</span>
      <span class="log-lvl lvl-${log.level.toLowerCase()}">${log.level}</span>
      <span class="log-msg">${escapeHtml(log.message)}</span>
    </div>
  `).join("");
  
  // Auto-scroll to bottom if freeze is not checked
  const freezeCheckbox = document.getElementById("freeze-logs-checkbox");
  if (!freezeCheckbox || !freezeCheckbox.checked) {
    container.scrollTop = container.scrollHeight;
  }
}

function renderHistoryCharts() {
  const runs = [...state.historySelection]
    .map((runId) => state.runDetails.get(runId))
    .filter(Boolean);
  renderChartGroup("history", runs);
  renderHistoryErrors();
}

function renderHistoryErrors() {
  const container = document.getElementById("history-errors-container");
  if (!container) return;
  const details = [...state.historySelection]
    .map((runId) => ({ run: getRunById(runId), detail: state.runDetails.get(runId) }))
    .filter((entry) => entry.run && entry.detail);

  if (!details.length) {
    container.innerHTML = '<div class="empty-state">Select runs to analyze their specific error reports.</div>';
    return;
  }

  container.innerHTML = details.map(({ run, detail }) => {
    const summary = run.summary || detail.summary;
    const errors = summary?.error_messages || [];
    return `
      <div class="series-card" style="margin-bottom: 24px;">
        <div class="series-label" style="font-weight: 700; color: var(--danger); margin-bottom: 12px; display: flex; align-items: center; gap: 8px;">
          <span style="width: 8px; height: 8px; border-radius: 50%; background: var(--danger);"></span>
          ${run.run_name} • Error Analysis
        </div>
        ${errors.length ? `
          <div class="warning-list">
            ${errors.map(err => `<div class="warning-item is-error" style="font-family: 'JetBrains Mono', monospace; font-size: 0.85rem;">${escapeHtml(err)}</div>`).join("")}
          </div>
        ` : '<div class="empty-state">No specific error messages recorded for this run.</div>'}
      </div>
    `;
  }).join("");
}

function renderHistoryLogs() {
  const container = document.getElementById("history-logs");
  const details = [...state.historySelection]
    .map((runId) => ({ run: getRunById(runId), detail: state.runDetails.get(runId) }))
    .filter((entry) => entry.run && entry.detail);

  if (!details.length) {
    container.innerHTML = '<div class="empty-state">Select one or more runs to inspect their recent event logs.</div>';
    return;
  }

  container.innerHTML = details.map(({ run, detail }) => {
    const logs = detail?.logs || detail?.summary?.recent_logs || [];
    return `
      <div class="series-card" style="margin-bottom: 24px;">
        <div class="series-label" style="font-weight: 700; color: var(--accent); margin-bottom: 12px;">${run.run_name} • ${run.engine}</div>
        <div class="log-viewport" style="max-height: 400px; overflow-y: auto; background: rgba(0,0,0,0.3);">
          ${logs.length ? renderLogEntries(logs.slice(-50)) : '<div class="empty-state">No logs saved for this run.</div>'}
        </div>
      </div>
    `;
  }).join("");
}

function renderChartGroup(prefix, runs) {
  chartDefinitions.forEach((definition) => {
    try {
      renderChartLegend(`${prefix}-${definition.key}-legend`, runs, definition.metrics);
      drawChart(`${prefix}-${definition.key}-chart`, runs, definition.metrics);
      renderSeriesStats(`${prefix}-${definition.key}-stats`, runs, definition.metrics);
    } catch (e) {
      console.warn(`Failed to render ${prefix} chart for ${definition.key}`, e);
    }
  });
}

function renderChartLegend(containerId, runs, metrics) {
  const container = document.getElementById(containerId);
  if (!container) return;
  let seriesIndex = 0;
  const items = [];
  runs.forEach((run) => {
    metrics.forEach((metric, metricIndex) => {
      const color = colors[seriesIndex % colors.length];
      const isDashed = metricIndex > 0;
      items.push(`
        <div class="legend-item">
          <span class="legend-swatch ${isDashed ? "is-dashed" : ""}" style="background:${color}; border-color:${color}"></span>
          <span>${run.config?.run_name || run.run_id} • ${metric.label}</span>
        </div>
      `);
      seriesIndex += 1;
    });
  });
  container.innerHTML = items.join("");
}

function drawChart(containerId, runs, metrics) {
  const canvas = document.getElementById(containerId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  // Use pixel-perfect scaling for sharp charts
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  ctx.scale(dpr, dpr);

  const width = rect.width;
  const height = rect.height;
  const margin = { top: 32, right: 32, bottom: 44, left: 86 };
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

  let globalMax = 1;
  runs.forEach(run => {
    (run.samples || []).forEach(sample => {
      metrics.forEach(metric => {
        const val = Number(sample[metric.key] || 0);
        if (val > globalMax) globalMax = val;
      });
    });
  });
  const maxValue = niceCeiling(globalMax);

  let maxElapsed = 1;
  runs.forEach(run => {
    const samples = run.samples || [];
    if (samples.length >= 2) {
      const elapsed = (samples.at(-1).timestamp_ms - samples[0].timestamp_ms) / 1000;
      if (elapsed > maxElapsed) maxElapsed = elapsed;
    }
  });

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
      seriesIndex += 1;
    });
  });
}

function renderSeriesStats(containerId, runs, metrics) {
  const container = document.getElementById(containerId);
  if (!container) return;
  if (!runs.length) {
    container.innerHTML = "";
    return;
  }

  let seriesIndex = 0;
  const cards = [];
  runs.forEach((run) => {
    metrics.forEach((metric) => {
      const stats = computeStats(run.samples || [], metric.key);
      if (!stats) {
        seriesIndex += 1;
        return;
      }
      const color = colors[seriesIndex % colors.length];
      cards.push(`
        <div class="series-card" style="border-left: 4px solid ${color}">
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
      seriesIndex += 1;
    });
  });
  container.innerHTML = cards.join("");
}

function computeStats(samples, key) {
  const values = samples
    .map((sample) => ({
      value: Number(sample[key] || 0),
      durationMs: Math.max(1, Number(sample.sample_duration_ms) || 0),
    }))
    .filter((entry) => Number.isFinite(entry.value));
  if (!values.length) {
    return null;
  }
  const sorted = values.map((entry) => entry.value).sort((a, b) => a - b);
  const count = values.length;
  const median = count % 2 === 0
    ? (sorted[count / 2 - 1] + sorted[count / 2]) / 2
    : sorted[Math.floor(count / 2)];
  const totalDurationMs = values.reduce((total, entry) => total + entry.durationMs, 0);
  const weightedSum = values.reduce((total, entry) => total + (entry.value * entry.durationMs), 0);
  return {
    current: values.at(-1).value,
    min: sorted[0],
    max: sorted.at(-1),
    average: weightedSum / totalDurationMs,
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
  if (!form) return;

  form.engine.addEventListener("change", () => {
    syncEngineSettingPanels(form.engine.value);
    renderSetupSummary();
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const { config } = readFormState(form, { strict: true });
      const mixTotal =
        config.load.mix.point_reads +
        config.load.mix.range_scans +
        config.load.mix.inserts +
        config.load.mix.updates +
        config.load.mix.deletes +
        config.load.mix.aggregates;
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

  document.getElementById("jump-to-dashboard")?.addEventListener("click", () => navigateTo("dashboard"));
}

function setupControls() {
  const runSelect = document.getElementById("active-run-select");
  const concurrency = document.getElementById("live-concurrency");
  const concurrencyInput = document.getElementById("live-concurrency-input");

  runSelect.addEventListener("change", async () => {
    state.activeRunId = runSelect.value;
    if (state.activeRunId) {
      const detail = await ensureRunDetail(state.activeRunId);
      syncLiveControlsFromDetail(detail);
    }
    renderDashboardSummary();
    renderDashboardCharts();
  });

  concurrency?.addEventListener("input", () => {
    syncConcurrencyControls(concurrency.value);
  });
  concurrencyInput?.addEventListener("input", () => {
    syncConcurrencyControls(concurrencyInput.value);
  });

  document.getElementById("pause-run")?.addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "pause" });
    }
  });
  document.getElementById("resume-run")?.addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "resume" });
    }
  });
  document.getElementById("stop-run")?.addEventListener("click", async () => {
    if (state.activeRunId) {
      await sendControl(state.activeRunId, { kind: "stop" });
    }
  });
  document.getElementById("apply-live-controls")?.addEventListener("click", async () => {
    if (!state.activeRunId) {
      return;
    }
    const pointReads = Number(document.getElementById("live-point").value);
    const rangeScans = Number(document.getElementById("live-range").value);
    const inserts = Number(document.getElementById("live-insert").value);
    const updates = Number(document.getElementById("live-update").value);
    const deletes = Number(document.getElementById("live-delete").value);
    const aggregates = Number(document.getElementById("live-aggregate").value);

    if (pointReads + rangeScans + inserts + updates + deletes + aggregates !== 100) {
      alert("The live mix must add up to 100.");
      return;
    }
    await sendControl(state.activeRunId, {
      kind: "update_concurrency",
      concurrency: Number(document.getElementById("live-concurrency").value),
    });
    await sendControl(state.activeRunId, {
      kind: "update_mix",
      point_reads: pointReads,
      range_scans: rangeScans,
      inserts,
      updates,
      deletes,
      aggregates,
    });
  });
  // Profiling controls: apply and dynamic enable/disable
  const perfCheckbox = document.getElementById("run-worker-perf");
  const perfFreq = document.getElementById("run-perf-freq");
  const perfOutput = document.getElementById("run-perf-output");
  const perfGenerate = document.getElementById("run-perf-generate-flamegraph");
  const straceCheckbox = document.getElementById("run-worker-strace");
  const straceOutput = document.getElementById("run-worker-strace-output");

  function syncProfilingControls() {
    if (!perfCheckbox) return;
    const perfEnabled = perfCheckbox.checked;
    if (perfFreq) perfFreq.disabled = !perfEnabled;
    if (perfOutput) perfOutput.disabled = !perfEnabled;
    if (perfGenerate) perfGenerate.disabled = !perfEnabled;
    if (straceCheckbox && straceOutput) straceOutput.disabled = !straceCheckbox.checked;
    renderSetupSummary();
  }

  perfCheckbox?.addEventListener("change", syncProfilingControls);
  straceCheckbox?.addEventListener("change", syncProfilingControls);

  document.getElementById("apply-profiling")?.addEventListener("click", async () => {
    try {
      await applyServerOptions();
      alert("Profiling settings applied");
      await loadServerOptions();
      syncProfilingControls();
    } catch (error) {
      alert("Failed to apply profiling settings: " + (error.message || error));
    }
  });

  const liveMixInputs = ["live-point", "live-range", "live-insert", "live-update", "live-delete", "live-aggregate"];
  liveMixInputs.forEach((id) => {
    document.getElementById(id)?.addEventListener("input", updateLiveMixStatus);
  });

  document.getElementById("clear-logs-btn")?.addEventListener("click", () => {
    state.clearedLogsTimestamp = Date.now();
    renderDashboardLogs();
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

function formatLatencyMs(value) {
  return `${formatDecimal(value, value < 10 ? 2 : 1)} ms`;
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

function syncConcurrencyControls(value) {
  const range = document.getElementById("live-concurrency");
  const numeric = Math.max(1, Number(value) || 1);
  if (range) {
    range.max = String(Math.max(32, numeric));
    range.value = String(numeric);
  }
  const label = document.getElementById("live-concurrency-value");
  if (label) label.textContent = String(numeric);
}

function statusPillClass(status) {
  const s = (status || "pending").toLowerCase();
  return `status-badge status-${s}`;
}

function formatLogTimestamp(timestampMs) {
  if (!timestampMs) {
    return "time unknown";
  }
  return new Date(timestampMs).toLocaleTimeString();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function renderLogEntries(entries) {
  return entries.map((entry) => `
    <div class="log-entry is-${entry.level || "info"}">
      <div class="log-meta">
        <span>${formatLogTimestamp(entry.timestamp_ms)}</span>
        <span>${(entry.level || "info").toUpperCase()}</span>
        <span>${String(entry.source || "server").replaceAll("_", " ")}</span>
      </div>
      <div class="log-message">${escapeHtml(entry.message)}</div>
    </div>
  `).join("");
}

function renderMessageItems(warnings, errors) {
  return [
    ...warnings.map((text) => `<div class="warning-item">${text}</div>`),
    ...errors.map((text) => `<div class="warning-item is-error">${text}</div>`),
  ].join("");
}

function totalSampleErrors(samples) {
  return samples.reduce((total, sample) => total + (Number(sample.error_count) || 0), 0);
}

function formatMix(mix) {
  if (!mix) {
    return "n/a";
  }
  return `${mix.point_reads}/${mix.range_scans}/${mix.inserts}/${mix.updates}/${mix.deletes}/${mix.aggregates}`;
}

async function loadServerOptions() {
  try {
    const opts = await fetchJson("/api/options");
    const perfCb = document.getElementById("run-worker-perf");
    const perfOut = document.getElementById("run-perf-output");
    const perfFr = document.getElementById("run-perf-freq");
    const perfGen = document.getElementById("run-perf-generate-flamegraph");
    const straceCb = document.getElementById("run-worker-strace");
    const straceOut = document.getElementById("run-worker-strace-output");

    if (perfCb) perfCb.checked = !!opts.worker_perf;
    if (perfOut) perfOut.value = opts.worker_perf_output || "";
    if (perfFr) perfFr.value = opts.worker_perf_freq_hz || "";
    if (perfGen) perfGen.checked = opts.worker_perf_generate_flamegraph !== false;
    if (straceCb) straceCb.checked = !!opts.worker_strace;
    if (straceOut) straceOut.value = opts.worker_strace_output || "";
  } catch (error) {
    console.warn("failed to load server options", error);
  }
}

async function applyServerOptions() {
  const payload = {
    worker_perf: document.getElementById("run-worker-perf")?.checked,
    worker_perf_generate_flamegraph: document.getElementById("run-perf-generate-flamegraph")?.checked,
    worker_perf_freq_hz: (function () {
      const v = document.getElementById("run-perf-freq")?.value;
      return v === "" || v === undefined ? null : Number(v);
    })(),
    worker_perf_output: document.getElementById("run-perf-output")?.value || null,
    worker_strace: document.getElementById("run-worker-strace")?.checked,
    worker_strace_output: document.getElementById("run-worker-strace-output")?.value || null,
  };
  const resp = await fetch("/api/options", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => resp.statusText);
    throw new Error(text || resp.statusText);
  }
}

async function boot() {
  setupNavigation();
  setupForm();
  setupControls();
  await loadServerOptions();
  setupDurationControls();
  syncPageFromLocation();
  const runForm = document.getElementById("run-form");
  if (runForm && runForm.engine) {
    syncEngineSettingPanels(runForm.engine.value);
    renderSetupSummary();
  }
  await refreshRuns();
  // No polling — live updates arrive via SSE. refreshRuns() is called
  // once at boot and again after each run finishes or fails.
}

boot().catch((error) => {
  document.getElementById("server-status").textContent = error.message;
});
