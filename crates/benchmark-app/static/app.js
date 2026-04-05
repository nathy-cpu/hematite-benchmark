const state = {
  runs: [],
  runDetails: new Map(),
  eventSources: new Map(),
  selectedRuns: new Set(),
};

const colors = ["#38b2ac", "#f59e0b", "#60a5fa", "#f472b6", "#34d399", "#f87171"];

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(payload.error || response.statusText);
  }
  return response.json();
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
      duration_secs: Number(form.duration_secs.value),
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

function renderRunList() {
  const container = document.getElementById("run-list");
  const activeSelect = document.getElementById("active-run-select");
  document.getElementById("run-count").textContent = String(state.runs.length);
  container.innerHTML = "";
  activeSelect.innerHTML = "";

  state.runs.forEach((run) => {
    const row = document.createElement("label");
    row.className = "run-item";
    row.innerHTML = `
      <input type="checkbox" ${state.selectedRuns.has(run.run_id) ? "checked" : ""} />
      <div>
        <strong>${run.run_name}</strong>
        <div class="run-meta">${run.engine} • ${new Date(run.started_at_ms).toLocaleString()}</div>
      </div>
      <span class="status-pill">${run.status}</span>
    `;
    row.querySelector("input").addEventListener("change", (event) => {
      if (event.target.checked) {
        state.selectedRuns.add(run.run_id);
        ensureRunDetail(run.run_id);
      } else {
        state.selectedRuns.delete(run.run_id);
      }
      drawAllCharts();
    });
    container.appendChild(row);

    if (run.status === "running" || run.status === "pending") {
      const option = document.createElement("option");
      option.value = run.run_id;
      option.textContent = `${run.run_name} (${run.engine})`;
      activeSelect.appendChild(option);
    }
  });
}

async function refreshRuns() {
  state.runs = await fetchJson("/api/runs");
  renderRunList();
  for (const run of state.runs) {
    if (state.selectedRuns.has(run.run_id)) {
      ensureRunDetail(run.run_id);
    }
    if ((run.status === "running" || run.status === "pending") && !state.eventSources.has(run.run_id)) {
      attachStream(run.run_id);
    }
  }
  drawAllCharts();
}

async function ensureRunDetail(runId) {
  const detail = await fetchJson(`/api/runs/${runId}`);
  state.runDetails.set(runId, detail);
  drawAllCharts();
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
    drawAllCharts();
  };
  source.onerror = () => {
    source.close();
    state.eventSources.delete(runId);
  };
  state.eventSources.set(runId, source);
}

function drawAllCharts() {
  const selectedDetails = [...state.selectedRuns]
    .map((id) => state.runDetails.get(id))
    .filter(Boolean);

  drawChart("throughput-chart", selectedDetails, [
    { key: "writes_per_sec", label: "writes/s" },
    { key: "reads_per_sec", label: "reads/s" },
  ]);
  drawChart("memory-chart", selectedDetails, [{ key: "rss_bytes", label: "rss bytes" }]);
  drawChart("io-chart", selectedDetails, [
    { key: "disk_read_bytes_per_sec", label: "read B/s" },
    { key: "disk_write_bytes_per_sec", label: "write B/s" },
  ]);
  drawChart("disk-chart", selectedDetails, [{ key: "disk_usage_bytes", label: "disk bytes" }]);
}

function drawChart(canvasId, runs, metrics) {
  const canvas = document.getElementById(canvasId);
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "#081015";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = "rgba(147, 173, 183, 0.2)";
  ctx.lineWidth = 1;

  for (let i = 0; i < 5; i += 1) {
    const y = 20 + i * ((canvas.height - 40) / 4);
    ctx.beginPath();
    ctx.moveTo(40, y);
    ctx.lineTo(canvas.width - 20, y);
    ctx.stroke();
  }

  const allValues = [];
  runs.forEach((run) => {
    run.samples.forEach((sample) => {
      metrics.forEach((metric) => allValues.push(sample[metric.key] || 0));
    });
  });
  const maxValue = Math.max(...allValues, 1);

  runs.forEach((run, runIndex) => {
    metrics.forEach((metric, metricIndex) => {
      const series = run.samples;
      if (!series.length) {
        return;
      }
      ctx.strokeStyle = colors[(runIndex + metricIndex) % colors.length];
      ctx.lineWidth = metricIndex === 0 ? 2.5 : 1.5;
      ctx.beginPath();
      series.forEach((sample, index) => {
        const x = 40 + (index / Math.max(series.length - 1, 1)) * (canvas.width - 60);
        const y = canvas.height - 20 - ((sample[metric.key] || 0) / maxValue) * (canvas.height - 40);
        if (index === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();
      ctx.fillStyle = ctx.strokeStyle;
      ctx.fillText(`${run.config?.run_name || run.run_id} ${metric.label}`, 46, 24 + runIndex * 16 + metricIndex * 12);
    });
  });
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
      const created = await fetchJson("/api/runs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(config),
      });
      state.selectedRuns.add(created.run_id);
      await refreshRuns();
      await ensureRunDetail(created.run_id);
    } catch (error) {
      alert(error.message);
    }
  });
}

function setupControls() {
  const runSelect = document.getElementById("active-run-select");
  const concurrency = document.getElementById("live-concurrency");
  concurrency.addEventListener("input", () => {
    document.getElementById("live-concurrency-value").textContent = concurrency.value;
  });

  document.getElementById("pause-run").addEventListener("click", async () => {
    if (runSelect.value) {
      await sendControl(runSelect.value, { kind: "pause" });
    }
  });
  document.getElementById("resume-run").addEventListener("click", async () => {
    if (runSelect.value) {
      await sendControl(runSelect.value, { kind: "resume" });
    }
  });
  document.getElementById("stop-run").addEventListener("click", async () => {
    if (runSelect.value) {
      await sendControl(runSelect.value, { kind: "stop" });
    }
  });
  document.getElementById("apply-live-controls").addEventListener("click", async () => {
    const runId = runSelect.value;
    if (!runId) {
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
    await sendControl(runId, {
      kind: "update_concurrency",
      concurrency: Number(concurrency.value),
    });
    await sendControl(runId, {
      kind: "update_mix",
      point_reads: pointReads,
      range_scans: rangeScans,
      inserts,
      updates,
    });
  });
}

async function boot() {
  setupForm();
  setupControls();
  await refreshRuns();
  setInterval(refreshRuns, 5000);
}

boot().catch((error) => {
  document.getElementById("server-status").textContent = error.message;
});
