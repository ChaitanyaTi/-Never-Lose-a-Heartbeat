"""
Cloud Service Web Application
FastAPI application for receiving streaming biosignal samples, triggering real-time window processing,
and servicing remote queries and cloud exports.
"""

import os
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel

from cloud_service.cloud_store import CloudStore

app = FastAPI(title="MayaCardio Cloud Ingestion & Analytics Service", version="1.0.0")

cloud_store = CloudStore()


@app.get("/api/v1/telemetry")
def get_telemetry():
    """Returns real-time telemetry metrics for the live dashboard."""
    conn = cloud_store._get_conn()
    row_sess = conn.execute("SELECT session_id FROM cloud_sessions ORDER BY created_at DESC LIMIT 1").fetchone()
    if not row_sess:
        return {
            "session_id": "waiting...",
            "samples_in_cloud": 0,
            "latest_result": None,
            "recent_samples": [],
        }

    session_id = row_sess["session_id"]
    cnt = cloud_store.get_sample_count(session_id)

    # Latest Section 3 result
    row_res = conn.execute(
        "SELECT * FROM cloud_results WHERE session_id = ? ORDER BY window DESC LIMIT 1",
        (session_id,),
    ).fetchone()

    latest_result = None
    if row_res:
        import json
        latest_result = {
            "window": row_res["window"],
            "mode": row_res["mode"],
            "missing_1": row_res["missing_1"],
            "missing_2": row_res["missing_2"],
            "complete": bool(row_res["complete"]),
            "mean_1": json.loads(row_res["mean_1"]) if row_res["mean_1"] else None,
            "mean_2": json.loads(row_res["mean_2"]) if row_res["mean_2"] else None,
            "min_1": json.loads(row_res["min_1"]) if row_res["min_1"] else None,
            "min_2": json.loads(row_res["min_2"]) if row_res["min_2"] else None,
            "max_1": json.loads(row_res["max_1"]) if row_res["max_1"] else None,
            "max_2": json.loads(row_res["max_2"]) if row_res["max_2"] else None,
            "magnitude": round(row_res["magnitude"], 1) if row_res["magnitude"] is not None else None,
            "heart_rate": round(row_res["heart_rate"], 1) if row_res["heart_rate"] is not None else None,
            "processed_at": row_res["processed_at"],
        }

    # Fetch last 400 samples of Sensor 1 Channel 2 for real-time waveform rendering
    cur_samples = conn.execute(
        "SELECT sample_index, ch2 FROM cloud_samples WHERE session_id = ? AND sensor = 1 ORDER BY sample_index DESC LIMIT 400",
        (session_id,),
    )
    samples = [{"idx": r["sample_index"], "val": r["ch2"]} for r in reversed(cur_samples.fetchall())]

    return {
        "session_id": session_id,
        "samples_in_cloud": cnt,
        "latest_result": latest_result,
        "recent_samples": samples,
    }


class SampleItem(BaseModel):
    sensor: int
    index: int
    values: List[int]


class LossItem(BaseModel):
    sensor: int
    from_index: int
    to_index: Optional[int] = None
    reason: str = "unknown"


class IngestPayload(BaseModel):
    session_id: str
    samples: List[SampleItem] = []
    losses: List[LossItem] = []


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MayaCardio Telemetry Center</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #070b14;
      --card-bg: rgba(15, 23, 42, 0.75);
      --border: rgba(255, 255, 255, 0.08);
      --accent-cyan: #06b6d4;
      --accent-emerald: #10b981;
      --accent-rose: #f43f5e;
      --accent-purple: #8b5cf6;
      --text: #f8fafc;
      --text-muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Outfit', -apple-system, sans-serif;
      min-height: 100vh;
      padding: 24px;
      background-image: 
        radial-gradient(circle at 15% 20%, rgba(6, 182, 212, 0.08) 0%, transparent 40%),
        radial-gradient(circle at 85% 80%, rgba(139, 92, 246, 0.08) 0%, transparent 40%);
    }
    .container { max-width: 1300px; margin: 0 auto; }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding-bottom: 20px;
      border-bottom: 1px solid var(--border);
    }
    .logo-group { display: flex; align-items: center; gap: 14px; }
    .heart-logo {
      width: 44px; height: 44px;
      background: linear-gradient(135deg, var(--accent-rose), #be123c);
      border-radius: 12px;
      display: flex; align-items: center; justify-content: center;
      box-shadow: 0 0 20px rgba(244, 63, 94, 0.4);
      animation: pulse 1.2s infinite ease-in-out;
    }
    @keyframes pulse {
      0%, 100% { transform: scale(1); }
      50% { transform: scale(1.08); box-shadow: 0 0 28px rgba(244, 63, 94, 0.6); }
    }
    h1 { font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .subtitle { font-size: 13px; color: var(--text-muted); font-weight: 400; }
    .badge {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 6px 14px; border-radius: 20px;
      font-size: 13px; font-weight: 600;
      background: rgba(16, 185, 129, 0.15);
      color: var(--accent-emerald);
      border: 1px solid rgba(16, 185, 129, 0.3);
    }
    .badge.offline {
      background: rgba(244, 63, 94, 0.15);
      color: var(--accent-rose);
      border-color: rgba(244, 63, 94, 0.3);
    }
    .badge-dot {
      width: 8px; height: 8px; border-radius: 50%;
      background: currentColor;
      box-shadow: 0 0 8px currentColor;
    }
    .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }
    .card {
      background: var(--card-bg);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px;
      transition: transform 0.2s, border-color 0.2s;
    }
    .card:hover { transform: translateY(-2px); border-color: rgba(255, 255, 255, 0.15); }
    .card-title { font-size: 12px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 1px; font-weight: 600; margin-bottom: 8px; }
    .card-value { font-size: 36px; font-weight: 700; letter-spacing: -1px; font-family: 'JetBrains Mono', monospace; }
    .card-sub { font-size: 12px; color: var(--text-muted); margin-top: 6px; }
    .waveform-card {
      background: var(--card-bg);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px;
      margin-bottom: 24px;
    }
    .waveform-header {
      display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px;
    }
    canvas {
      width: 100%; height: 200px;
      background: rgba(0, 0, 0, 0.4);
      border-radius: 10px;
      border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .table-card {
      background: var(--card-bg);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 20px;
      overflow-x: auto;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; }
    th { padding: 12px; color: var(--text-muted); border-bottom: 1px solid var(--border); font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px; }
    td { padding: 12px; border-bottom: 1px solid rgba(255, 255, 255, 0.04); font-family: 'JetBrains Mono', monospace; }
    .mono { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="logo-group">
        <div class="heart-logo">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="white"><path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z"/></svg>
        </div>
        <div>
          <h1>MayaCardio Bedside Telemetry</h1>
          <div class="subtitle">Mission-Critical Biosignal Edge Pipeline | Session: <span id="session-badge" class="mono">Initializing...</span></div>
        </div>
      </div>
      <div id="mode-badge" class="badge">
        <span class="badge-dot"></span>
        <span id="mode-text">ONLINE STREAM</span>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="card-title">Heart Rate (S1 Ch2)</div>
        <div class="card-value" style="color: var(--accent-emerald);" id="hr-value">-- <span style="font-size: 18px;">BPM</span></div>
        <div class="card-sub" id="hr-sub">4-Stage Peak Detection</div>
      </div>
      <div class="card">
        <div class="card-title">Sensor 2 Magnitude</div>
        <div class="card-value" style="color: var(--accent-cyan);" id="mag-value">--</div>
        <div class="card-sub">Mean of √(ch1² + ch2²)</div>
      </div>
      <div class="card">
        <div class="card-title">Cloud Received Samples</div>
        <div class="card-value" style="color: var(--accent-purple);" id="samples-value">0</div>
        <div class="card-sub">Synchronized Ingest</div>
      </div>
      <div class="card">
        <div class="card-title">Power-Cut Data Loss Risk</div>
        <div class="card-value" style="color: #f59e0b;">&le; 0.25s</div>
        <div class="card-sub">Bounded by SQLite fsync</div>
      </div>
    </div>

    <div class="waveform-card">
      <div class="waveform-header">
        <div>
          <div style="font-weight: 600; font-size: 15px;">Live ECG Waveform (Sensor 1, Channel 2 @ 400 Hz)</div>
          <div style="font-size: 12px; color: var(--text-muted);">Real-time stream with automated R-peak detection</div>
        </div>
        <div style="font-size: 12px; color: var(--accent-cyan); font-family: 'JetBrains Mono', monospace;" id="fps-counter">400 SPS BUFFER</div>
      </div>
      <canvas id="ecgCanvas" width="1200" height="200"></canvas>
    </div>

    <div class="table-card">
      <div style="font-weight: 600; font-size: 15px; margin-bottom: 12px;">Recent Section 3 Computed Windows (1-Second Windows)</div>
      <table>
        <thead>
          <tr>
            <th>Window</th>
            <th>Mode</th>
            <th>Status</th>
            <th>Sensor 1 Mean [Ch1, Ch2, Ch3]</th>
            <th>Sensor 2 Mean [Ch1, Ch2]</th>
            <th>Magnitude</th>
            <th>Heart Rate</th>
          </tr>
        </thead>
        <tbody id="results-body">
          <tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 24px;">Awaiting incoming stream batches...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    const canvas = document.getElementById('ecgCanvas');
    const ctx = canvas.getContext('2d');
    let historyResults = [];

    function renderWaveform(samples) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      
      // Draw grid lines
      ctx.strokeStyle = "rgba(255, 255, 255, 0.04)";
      ctx.lineWidth = 1;
      for (let x = 0; x < canvas.width; x += 40) {
        ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, canvas.height); ctx.stroke();
      }
      for (let y = 0; y < canvas.height; y += 40) {
        ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(canvas.width, y); ctx.stroke();
      }

      if (!samples || samples.length < 2) return;

      let minVal = Infinity, maxVal = -Infinity;
      for (let s of samples) {
        if (s.val < minVal) minVal = s.val;
        if (s.val > maxVal) maxVal = s.val;
      }
      const range = (maxVal - minVal) || 1;
      const padding = 20;
      const h = canvas.height - padding * 2;

      ctx.beginPath();
      ctx.strokeStyle = "#06b6d4";
      ctx.lineWidth = 2.5;
      ctx.lineJoin = "round";

      for (let i = 0; i < samples.length; i++) {
        const x = (i / (samples.length - 1)) * canvas.width;
        const normalized = (samples[i].val - minVal) / range;
        const y = canvas.height - padding - normalized * h;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();
    }

    async function pollTelemetry() {
      try {
        const res = await fetch('/api/v1/telemetry');
        const data = await res.json();

        if (data.session_id) {
          document.getElementById('session-badge').innerText = data.session_id;
        }
        document.getElementById('samples-value').innerText = Number(data.samples_in_cloud || 0).toLocaleString();

        if (data.latest_result) {
          const lr = data.latest_result;
          document.getElementById('hr-value').innerHTML = (lr.heart_rate !== null ? lr.heart_rate.toFixed(1) : '--') + ' <span style="font-size: 18px;">BPM</span>';
          document.getElementById('mag-value').innerText = lr.magnitude !== null ? lr.magnitude.toLocaleString() : '--';
          
          const modeBadge = document.getElementById('mode-badge');
          const modeText = document.getElementById('mode-text');
          if (lr.mode === 'offline') {
            modeBadge.className = 'badge offline';
            modeText.innerText = 'OFFLINE (DEVICE)';
          } else {
            modeBadge.className = 'badge';
            modeText.innerText = 'ONLINE (CLOUD)';
          }

          // Add to table if new window
          if (!historyResults.some(r => r.window === lr.window)) {
            historyResults.unshift(lr);
            if (historyResults.length > 8) historyResults.pop();
            renderTable();
          }
        }

        if (data.recent_samples) {
          renderWaveform(data.recent_samples);
        }
      } catch (e) {
        console.error("Telemetry error", e);
      }
    }

    function renderTable() {
      const tbody = document.getElementById('results-body');
      tbody.innerHTML = historyResults.map(r => `
        <tr>
          <td>#${r.window}</td>
          <td><span style="color: ${r.mode === 'online' ? '#10b981' : '#f43f5e'}; font-weight: 600;">${r.mode.toUpperCase()}</span></td>
          <td>${r.complete ? '<span style="color: #10b981;">Complete (0 loss)</span>' : '<span style="color: #f59e0b;">Incomplete</span>'}</td>
          <td>[${r.mean_1 ? r.mean_1.join(', ') : 'null'}]</td>
          <td>[${r.mean_2 ? r.mean_2.join(', ') : 'null'}]</td>
          <td style="color: var(--accent-cyan);">${r.magnitude !== null ? r.magnitude : 'null'}</td>
          <td style="color: var(--accent-emerald); font-weight: 700;">${r.heart_rate !== null ? r.heart_rate + ' BPM' : 'null'}</td>
        </tr>
      `).join('');
    }

    setInterval(pollTelemetry, 500);
    pollTelemetry();
  </script>
</body>
</html>
"""


@app.get("/health")
def health_check():
    return {"status": "healthy"}



@app.post("/api/v1/ingest")
def ingest_data(payload: IngestPayload):
    samples_data = [s.model_dump() for s in payload.samples]
    losses_data = [l.model_dump() for l in payload.losses]
    result = cloud_store.ingest_batch(payload.session_id, samples_data, losses_data)
    return {"status": "ok", **result}


@app.get("/api/v1/status")
def get_session_status(session_id: str = Query(...)):
    cnt = cloud_store.get_sample_count(session_id)
    return {"session_id": session_id, "samples_in_cloud": cnt}


@app.get("/api/v1/export")
def export_cloud_data(session_id: str = Query(...)):
    def line_generator():
        for line in cloud_store.export_samples_stream(session_id):
            yield line + "\n"
        for line in cloud_store.export_losses_stream(session_id):
            yield line + "\n"

    return StreamingResponse(line_generator(), media_type="application/x-ndjson")


@app.get("/api/v1/results")
def export_cloud_results(session_id: str = Query(...)):
    def results_generator():
        for line in cloud_store.get_results_stream(session_id):
            yield line + "\n"

    return StreamingResponse(results_generator(), media_type="application/x-ndjson")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("cloud_service.app:app", host="0.0.0.0", port=port, log_level="info")
