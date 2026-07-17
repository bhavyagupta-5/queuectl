import express from 'express';
import fs from 'fs';
import path from 'path';
import {
  initDb,
  getDb,
  enqueueJob,
  getJob,
  updateJobStatus,
  listJobs,
  getJobSummary,
  clearAll
} from './db.js';

function isPidRunning(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return false;
  }
}

export async function startDashboard(port = 3000) {
  const app = express();
  app.use(express.json());

  app.get('/api/status', async (req, res) => {
    try {
      const summary = await getJobSummary();
      const db = getDb();

      const workersRaw = await db.all(`SELECT * FROM workers`);
      const workers = [];
      for (const w of workersRaw) {
        if (isPidRunning(w.pid)) {
          workers.push(w);
        } else {
          await db.run(`DELETE FROM workers WHERE id = ?`, [w.id]);
        }
      }

      const jobs = await db.all(`SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 50`);

      const configRows = await db.all(`SELECT * FROM config`);
      const config = {};
      configRows.forEach(row => {
        config[row.key] = row.value;
      });

      res.json({
        summary,
        workers,
        jobs,
        config
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/enqueue', async (req, res) => {
    const { id, command, max_retries, priority, run_in_seconds, timeout } = req.body;

    if (!command) {
      return res.status(400).json({ error: 'Command is required' });
    }

    const jobId = id || `job-${Math.random().toString(36).substring(2, 11)}`;
    let run_at = new Date().toISOString();
    if (run_in_seconds && parseInt(run_in_seconds, 10) > 0) {
      run_at = new Date(Date.now() + parseInt(run_in_seconds, 10) * 1000).toISOString();
    }

    try {
      await enqueueJob({
        id: jobId,
        command,
        max_retries: max_retries ? parseInt(max_retries, 10) : 3,
        priority: priority ? parseInt(priority, 10) : 0,
        run_at,
        timeout: timeout ? parseInt(timeout, 10) : 0
      });
      res.json({ success: true, jobId });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/retry/:id', async (req, res) => {
    const jobId = req.params.id;
    try {
      const job = await getJob(jobId);
      if (!job) {
        return res.status(404).json({ error: 'Job not found' });
      }
      const db = getDb();
      await updateJobStatus(jobId, 'pending', new Date().toISOString(), null);
      await db.run(`UPDATE jobs SET attempts = 0 WHERE id = ?`, [jobId]);
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/logs/:id', (req, res) => {
    const jobId = req.params.id;
    const logsDir = process.env.LOGS_PATH || path.resolve(process.cwd(), 'logs');
    const logPath = path.join(logsDir, `job-${jobId}.log`);

    if (!fs.existsSync(logPath)) {
      return res.json({ logs: 'No logs found for this job execution.' });
    }

    fs.readFile(logPath, 'utf8', (err, data) => {
      if (err) {
        return res.status(500).json({ error: 'Could not read log file' });
      }
      res.json({ logs: data });
    });
  });

  app.post('/api/clear', async (req, res) => {
    try {
      await clearAll();
      await initDb();
      res.json({ success: true });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/', (req, res) => {
    res.send(HTML_CONTENT);
  });

  app.listen(port, () => {
    console.log(`\n`);
    console.log(`QueueCTL Web Dashboard is active!`);
    console.log(`🔗 Address: \x1b[36mhttp://localhost:${port}\x1b[0m`);
    console.log(`\n`);
  });
}

const HTML_CONTENT = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>QueueCTL - Monitoring Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg-dark: #0f172a;
      --bg-card: rgba(30, 41, 59, 0.7);
      --border-color: rgba(255, 255, 255, 0.08);
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #6366f1;
      --primary-glow: rgba(99, 102, 241, 0.15);
      
      --color-pending: #fb7185;
      --color-processing: #38bdf8;
      --color-completed: #34d399;
      --color-failed: #fbbf24;
      --color-dead: #f43f5e;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: 'Outfit', sans-serif;
      background-color: var(--bg-dark);
      background-image: 
        radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.1) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(56, 189, 248, 0.1) 0px, transparent 50%);
      color: var(--text-main);
      min-height: 100vh;
      overflow-x: hidden;
      padding-bottom: 50px;
    }

    header {
      backdrop-filter: blur(10px);
      border-bottom: 1px solid var(--border-color);
      padding: 20px 40px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 100;
      background: rgba(15, 23, 42, 0.6);
    }

    header h1 {
      font-size: 24px;
      font-weight: 700;
      background: linear-gradient(135deg, #a5b4fc, #38bdf8);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .btn {
      background: var(--primary);
      color: white;
      border: none;
      padding: 10px 20px;
      border-radius: 8px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.3s;
      font-family: inherit;
    }

    .btn:hover {
      box-shadow: 0 0 15px var(--primary);
      transform: translateY(-1px);
    }

    .btn-secondary {
      background: transparent;
      border: 1px solid var(--border-color);
      color: var(--text-main);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.05);
      box-shadow: none;
    }

    .btn-danger {
      background: var(--color-dead);
    }

    .btn-danger:hover {
      box-shadow: 0 0 15px rgba(244, 63, 94, 0.4);
    }

    .container {
      max-width: 1400px;
      margin: 40px auto 0;
      padding: 0 20px;
      display: grid;
      grid-template-columns: 3fr 1fr;
      gap: 30px;
    }

    @media (max-width: 1024px) {
      .container {
        grid-template-columns: 1fr;
      }
    }

    .panel {
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      backdrop-filter: blur(12px);
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
      margin-bottom: 30px;
    }

    .panel-title {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: #e2e8f0;
      border-left: 4px solid var(--primary);
      padding-left: 10px;
    }

    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 20px;
      margin-bottom: 30px;
    }

    .stat-card {
      background: rgba(15, 23, 42, 0.4);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      padding: 16px;
      text-align: center;
      transition: all 0.3s;
    }

    .stat-card:hover {
      transform: translateY(-2px);
      border-color: rgba(255, 255, 255, 0.15);
    }

    .stat-value {
      font-size: 32px;
      font-weight: 700;
      margin-top: 5px;
    }

    .stat-card.pending { border-bottom: 3px solid var(--color-pending); }
    .stat-card.pending .stat-value { color: var(--color-pending); }

    .stat-card.processing { border-bottom: 3px solid var(--color-processing); }
    .stat-card.processing .stat-value { color: var(--color-processing); }

    .stat-card.completed { border-bottom: 3px solid var(--color-completed); }
    .stat-card.completed .stat-value { color: var(--color-completed); }

    .stat-card.failed { border-bottom: 3px solid var(--color-failed); }
    .stat-card.failed .stat-value { color: var(--color-failed); }

    .stat-card.dead { border-bottom: 3px solid var(--color-dead); }
    .stat-card.dead .stat-value { color: var(--color-dead); }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }

    th, td {
      padding: 12px 16px;
      border-bottom: 1px solid var(--border-color);
    }

    th {
      font-weight: 600;
      color: var(--text-muted);
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    td {
      font-size: 15px;
      color: #cbd5e1;
    }

    tr:last-child td {
      border-bottom: none;
    }

    tr:hover td {
      background: rgba(255, 255, 255, 0.02);
    }

    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
    }

    .badge.pending { background: rgba(251, 113, 133, 0.15); color: var(--color-pending); }
    .badge.processing { background: rgba(56, 189, 248, 0.15); color: var(--color-processing); }
    .badge.completed { background: rgba(52, 211, 153, 0.15); color: var(--color-completed); }
    .badge.failed { background: rgba(251, 191, 36, 0.15); color: var(--color-failed); }
    .badge.dead { background: rgba(244, 63, 94, 0.15); color: var(--color-dead); }

    .pulse-dot {
      width: 8px;
      height: 8px;
      background: var(--color-completed);
      border-radius: 50%;
      display: inline-block;
      box-shadow: 0 0 8px var(--color-completed);
      animation: pulse 1.5s infinite;
    }

    .pulse-dot.processing {
      background: var(--color-processing);
      box-shadow: 0 0 8px var(--color-processing);
    }

    @keyframes pulse {
      0% { transform: scale(0.9); opacity: 0.6; }
      50% { transform: scale(1.1); opacity: 1; }
      100% { transform: scale(0.9); opacity: 0.6; }
    }

    .modal {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      width: 100%;
      height: 100%;
      background: rgba(15, 23, 42, 0.8);
      backdrop-filter: blur(8px);
      z-index: 1000;
      justify-content: center;
      align-items: center;
    }

    .modal-content {
      background: #1e293b;
      border: 1px solid var(--border-color);
      border-radius: 16px;
      width: 90%;
      max-width: 600px;
      padding: 30px;
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.4);
      position: relative;
    }

    .modal-header {
      font-size: 20px;
      font-weight: 700;
      margin-bottom: 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .modal-close {
      cursor: pointer;
      color: var(--text-muted);
      font-size: 24px;
      background: none;
      border: none;
    }

    .form-group {
      margin-bottom: 20px;
    }

    .form-group label {
      display: block;
      margin-bottom: 8px;
      font-weight: 600;
      color: #e2e8f0;
    }

    .form-group input, .form-group textarea {
      width: 100%;
      background: rgba(15, 23, 42, 0.6);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 12px;
      color: white;
      font-family: inherit;
      font-size: 15px;
    }

    .form-group input:focus, .form-group textarea:focus {
      outline: none;
      border-color: var(--primary);
    }

    .terminal-logs {
      background: #090d16;
      border-radius: 12px;
      padding: 20px;
      font-family: 'JetBrains Mono', monospace;
      color: #a7f3d0;
      max-height: 400px;
      overflow-y: auto;
      white-space: pre-wrap;
      font-size: 13px;
      line-height: 1.6;
      border: 1px solid var(--border-color);
    }

    .empty-state {
      text-align: center;
      padding: 40px;
      color: var(--text-muted);
    }
  </style>
</head>
<body>

  <header>
    <h1>⚙️ QueueCTL Dashboard</h1>
    <div>
      <button class="btn btn-secondary" onclick="openEnqueueModal()" style="margin-right: 10px;">+ Enqueue Job</button>
      <button class="btn btn-danger" onclick="purgeDatabase()">Purge Queue</button>
    </div>
  </header>

  <div class="container">
    <div>
      <div class="panel">
        <div class="panel-title">Overview Stats</div>
        <div class="stats-grid">
          <div class="stat-card pending">
            <div>Pending</div>
            <div id="stat-pending" class="stat-value">0</div>
          </div>
          <div class="stat-card processing">
            <div>Processing</div>
            <div id="stat-processing" class="stat-value">0</div>
          </div>
          <div class="stat-card completed">
            <div>Completed</div>
            <div id="stat-completed" class="stat-value">0</div>
          </div>
          <div class="stat-card failed">
            <div>Failed</div>
            <div id="stat-failed" class="stat-value">0</div>
          </div>
          <div class="stat-card dead">
            <div>Dead (DLQ)</div>
            <div id="stat-dead" class="stat-value">0</div>
          </div>
        </div>

        <div style="height: 250px; margin-bottom: 20px; display: flex; justify-content: center;">
          <canvas id="jobChart" style="max-width: 250px; max-height: 250px;"></canvas>
        </div>
      </div>

      <div class="panel">
        <div class="panel-title">Recent Jobs</div>
        <div style="overflow-x: auto;">
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>State</th>
                <th>Attempts</th>
                <th>Priority</th>
                <th>Last Update</th>
                <th>Command</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody id="jobs-tbody">
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div>
      <div class="panel">
        <div class="panel-title">Active Workers</div>
        <div id="workers-container">
        </div>
      </div>

      <div class="panel">
        <div class="panel-title">Queue Configuration</div>
        <div style="font-size: 14px;">
          <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border-color);">
            <span style="color: var(--text-muted);">Max Retries</span>
            <span id="config-max-retries" style="font-weight: 600;">3</span>
          </div>
          <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid var(--border-color);">
            <span style="color: var(--text-muted);">Backoff Base</span>
            <span id="config-backoff-base" style="font-weight: 600;">2</span>
          </div>
          <div style="display: flex; justify-content: space-between; padding: 8px 0;">
            <span style="color: var(--text-muted);">Default Timeout</span>
            <span id="config-default-timeout" style="font-weight: 600;">0s</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div id="enqueueModal" class="modal">
    <div class="modal-content">
      <div class="modal-header">
        <span>Enqueue New Background Job</span>
        <button class="modal-close" onclick="closeEnqueueModal()">&times;</button>
      </div>
      <form id="enqueueForm" onsubmit="submitJob(event)">
        <div class="form-group">
          <label>Command (Shell Script / Executable)</label>
          <input type="text" id="job-command" placeholder="e.g. sleep 3 && echo 'Task Complete'" required>
        </div>
        <div class="form-group" style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
          <div>
            <label>Priority (Higher runs first)</label>
            <input type="number" id="job-priority" value="0">
          </div>
          <div>
            <label>Max Retries</label>
            <input type="number" id="job-retries" value="3">
          </div>
        </div>
        <div class="form-group" style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
          <div>
            <label>Delay (seconds)</label>
            <input type="number" id="job-delay" value="0" placeholder="0 for immediate">
          </div>
          <div>
            <label>Execution Timeout (seconds)</label>
            <input type="number" id="job-timeout" value="0" placeholder="0 for infinite">
          </div>
        </div>
        <button type="submit" class="btn" style="width: 100%;">Submit Job to Queue</button>
      </form>
    </div>
  </div>

  <div id="logsModal" class="modal">
    <div class="modal-content" style="max-width: 800px;">
      <div class="modal-header">
        <span id="logs-modal-title">Job Logs</span>
        <button class="modal-close" onclick="closeLogsModal()">&times;</button>
      </div>
      <div class="terminal-logs" id="logs-content">
        Loading...
      </div>
    </div>
  </div>

  <script>
    let chartInstance = null;

    async function loadData() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        document.getElementById('stat-pending').innerText = data.summary.pending;
        document.getElementById('stat-processing').innerText = data.summary.processing;
        document.getElementById('stat-completed').innerText = data.summary.completed;
        document.getElementById('stat-failed').innerText = data.summary.failed;
        document.getElementById('stat-dead').innerText = data.summary.dead;

        document.getElementById('config-max-retries').innerText = data.config['max-retries'] || '3';
        document.getElementById('config-backoff-base').innerText = data.config['backoff-base'] || '2';
        document.getElementById('config-default-timeout').innerText = (data.config['default-timeout'] || '0') + 's';

        const workersContainer = document.getElementById('workers-container');
        if (data.workers.length === 0) {
          workersContainer.innerHTML = '<div class="empty-state">No workers active. Start workers using CLI: <br><code style="font-family: monospace; display:block; margin-top:5px; background: rgba(0,0,0,0.2); padding: 5px; border-radius:4px;">queuectl worker start --count 3</code></div>';
        } else {
          workersContainer.innerHTML = data.workers.map(w => {
            const isBusy = w.state === 'processing';
            return \`
              <div style="padding: 12px; border-bottom: 1px solid var(--border-color); display: flex; align-items: center; justify-content: space-between;">
                <div>
                  <div style="font-weight: 600; font-size:14px;">\${w.id}</div>
                  <div style="font-size: 12px; color: var(--text-muted);">PID: \${w.pid} \${w.current_job_id ? '| Active Job: ' + w.current_job_id : ''}</div>
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                  <span class="pulse-dot \${isBusy ? 'processing' : ''}"></span>
                  <span style="font-size:12px; font-weight:600; text-transform:uppercase; color: \${isBusy ? 'var(--color-processing)' : 'var(--color-completed)'}">\${w.state}</span>
                </div>
              </div>
            \`;
          }).join('');
        }

        const jobsTbody = document.getElementById('jobs-tbody');
        if (data.jobs.length === 0) {
          jobsTbody.innerHTML = '<tr><td colspan="7" class="empty-state">No jobs in queue history.</td></tr>';
        } else {
          jobsTbody.innerHTML = data.jobs.map(j => {
            const hasLog = j.state !== 'pending';
            const showRetry = j.state === 'dead';
            return \`
              <tr>
                <td style="font-family: 'JetBrains Mono', monospace; font-size: 13px;">\${j.id}</td>
                <td><span class="badge \${j.state}">\${j.state}</span></td>
                <td>\${j.attempts}/\${j.max_retries}</td>
                <td>\${j.priority}</td>
                <td style="font-size:13px; color: var(--text-muted);">\${new Date(j.updated_at).toLocaleTimeString()}</td>
                <td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: monospace;">\${j.command}</td>
                <td>
                  <div style="display: flex; gap: 8px;">
                    \${hasLog ? \`<button class="btn btn-secondary" style="padding: 4px 8px; font-size: 12px;" onclick="viewLogs('\${j.id}')">Logs</button>\` : ''}
                    \${showRetry ? \`<button class="btn" style="padding: 4px 8px; font-size: 12px;" onclick="retryJob('\${j.id}')">Retry</button>\` : ''}
                  </div>
                </td>
              </tr>
            \`;
          }).join('');
        }

        updateChart(data.summary);
      } catch (err) {
        console.error("Error loading data:", err);
      }
    }

    function updateChart(summary) {
      const ctx = document.getElementById('jobChart').getContext('2d');
      const counts = [summary.pending, summary.processing, summary.completed, summary.failed, summary.dead];
      
      if (chartInstance) {
        chartInstance.data.datasets[0].data = counts;
        chartInstance.update();
      } else {
        chartInstance = new Chart(ctx, {
          type: 'doughnut',
          data: {
            labels: ['Pending', 'Processing', 'Completed', 'Failed', 'Dead'],
            datasets: [{
              data: counts,
              backgroundColor: ['#fb7185', '#38bdf8', '#34d399', '#fbbf24', '#f43f5e'],
              borderWidth: 0,
              hoverOffset: 4
            }]
          },
          options: {
            plugins: {
              legend: {
                display: false
              }
            },
            cutout: '70%'
          }
        });
      }
    }

    function openEnqueueModal() {
      document.getElementById('enqueueModal').style.display = 'flex';
    }
    function closeEnqueueModal() {
      document.getElementById('enqueueModal').style.display = 'none';
    }

    async function submitJob(e) {
      e.preventDefault();
      const command = document.getElementById('job-command').value;
      const priority = document.getElementById('job-priority').value;
      const max_retries = document.getElementById('job-retries').value;
      const run_in_seconds = document.getElementById('job-delay').value;
      const timeout = document.getElementById('job-timeout').value;

      try {
        const res = await fetch('/api/enqueue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ command, priority, max_retries, run_in_seconds, timeout })
        });
        if (res.ok) {
          closeEnqueueModal();
          document.getElementById('enqueueForm').reset();
          loadData();
        }
      } catch (err) {
        alert("Failed to enqueue job: " + err.message);
      }
    }

    async function retryJob(id) {
      try {
        const res = await fetch(\`/api/retry/\${id}\`, { method: 'POST' });
        if (res.ok) loadData();
      } catch (err) {
        alert("Failed to retry job");
      }
    }

    async function viewLogs(id) {
      document.getElementById('logs-modal-title').innerText = \`Job Logs: \${id}\`;
      const logsContent = document.getElementById('logs-content');
      logsContent.innerText = "Loading logs...";
      document.getElementById('logsModal').style.display = 'flex';

      try {
        const res = await fetch(\`/api/logs/\${id}\`);
        const data = await res.json();
        logsContent.innerText = data.logs;
      } catch (err) {
        logsContent.innerText = "Error loading logs: " + err.message;
      }
    }

    function closeLogsModal() {
      document.getElementById('logsModal').style.display = 'none';
    }

    async function purgeDatabase() {
      if (confirm("Are you sure you want to purge all jobs and stop worker states? This deletes execution history.")) {
        try {
          const res = await fetch('/api/clear', { method: 'POST' });
          if (res.ok) loadData();
        } catch (err) {
          alert("Failed to purge database");
        }
      }
    }

    loadData();
    setInterval(loadData, 2000);
  </script>
</body>
</html>
`;
