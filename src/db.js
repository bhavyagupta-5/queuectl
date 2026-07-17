import sqlite3 from 'sqlite3';
import path from 'path';

const DB_FILE = process.env.DB_PATH || path.resolve(process.cwd(), 'queue.db');
let dbInstance = null;

export function getDb() {
  if (dbInstance) return dbInstance;

  const db = new sqlite3.Database(DB_FILE);

  const run = (sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve({ lastID: this.lastID, changes: this.changes });
      });
    });
  };

  const get = (sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) reject(err);
        else resolve(row);
      });
    });
  };

  const all = (sql, params = []) => {
    return new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows);
      });
    });
  };

  const close = () => {
    return new Promise((resolve, reject) => {
      db.close((err) => {
        if (err) reject(err);
        else {
          dbInstance = null;
          resolve();
        }
      });
    });
  };

  dbInstance = { run, get, all, close, raw: db };
  return dbInstance;
}

export async function initDb() {
  const db = getDb();

  await db.run(`
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL,
      attempts INTEGER DEFAULT 0,
      max_retries INTEGER DEFAULT 3,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      run_at TEXT NOT NULL,
      priority INTEGER DEFAULT 0,
      timeout INTEGER DEFAULT 0,
      worker_id TEXT,
      error_message TEXT
    )
  `);

  await db.run(`
    CREATE TABLE IF NOT EXISTS workers (
      id TEXT PRIMARY KEY,
      pid INTEGER NOT NULL,
      state TEXT NOT NULL,
      current_job_id TEXT,
      last_heartbeat TEXT NOT NULL
    )
  `);

  await db.run(`
    INSERT OR IGNORE INTO config (key, value) VALUES ('max-retries', '3')
  `);
  await db.run(`
    INSERT OR IGNORE INTO config (key, value) VALUES ('backoff-base', '2')
  `);
}

export async function fetchNextJob(workerId) {
  const db = getDb();
  await db.run('BEGIN IMMEDIATE TRANSACTION');
  try {
    const now = new Date().toISOString();
    const job = await db.get(`
      SELECT * FROM jobs
      WHERE (state = 'pending' OR state = 'failed')
        AND datetime(run_at) <= datetime(?)
      ORDER BY priority DESC, created_at ASC
      LIMIT 1
    `, [now]);

    if (!job) {
      await db.run('COMMIT');
      return null;
    }

    const nextAttempts = job.attempts + 1;

    await db.run(`
      UPDATE jobs
      SET state = 'processing',
          worker_id = ?,
          attempts = ?,
          updated_at = datetime('now')
      WHERE id = ?
    `, [workerId, nextAttempts, job.id]);

    await db.run('COMMIT');

    return {
      ...job,
      state: 'processing',
      worker_id: workerId,
      attempts: nextAttempts
    };
  } catch (error) {
    await db.run('ROLLBACK');
    throw error;
  }
}

export async function registerWorker(id, pid) {
  const db = getDb();
  await db.run(`
    INSERT OR REPLACE INTO workers (id, pid, state, current_job_id, last_heartbeat)
    VALUES (?, ?, 'idle', NULL, datetime('now'))
  `, [id, pid]);
}

export async function updateWorkerHeartbeat(id, state, currentJobId) {
  const db = getDb();
  await db.run(`
    UPDATE workers
    SET state = ?, current_job_id = ?, last_heartbeat = datetime('now')
    WHERE id = ?
  `, [state, currentJobId, id]);
}

export async function removeWorker(id) {
  const db = getDb();
  await db.run(`DELETE FROM workers WHERE id = ?`, [id]);
}

export async function cleanupStaleWorkers(timeoutSeconds = 15) {
  const db = getDb();
  try {
    const staleWorkers = await db.all(`
      SELECT id, current_job_id FROM workers
      WHERE datetime(last_heartbeat) < datetime('now', '-' || ? || ' seconds')
    `, [timeoutSeconds]);

    for (const worker of staleWorkers) {
      if (worker.current_job_id) {
        await db.run(`
          UPDATE jobs
          SET state = 'failed', worker_id = NULL, updated_at = datetime('now')
          WHERE id = ? AND state = 'processing'
        `, [worker.current_job_id]);
      }
      await db.run(`DELETE FROM workers WHERE id = ?`, [worker.id]);
    }
  } catch (error) {
  }
}

export async function updateJobStatus(id, state, runAt = null, errorMessage = null) {
  const db = getDb();
  const runAtVal = runAt || new Date().toISOString();
  await db.run(`
    UPDATE jobs
    SET state = ?, run_at = ?, error_message = ?, updated_at = datetime('now')
    WHERE id = ?
  `, [state, runAtVal, errorMessage, id]);
}

export async function enqueueJob({ id, command, max_retries, priority, run_at, timeout }) {
  const db = getDb();
  const now = new Date().toISOString();
  const runAtVal = run_at || now;
  await db.run(`
    INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, run_at, priority, timeout, worker_id, error_message)
    VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?, NULL, NULL)
    ON CONFLICT(id) DO UPDATE SET
      command = excluded.command,
      state = 'pending',
      attempts = 0,
      max_retries = excluded.max_retries,
      updated_at = excluded.updated_at,
      run_at = excluded.run_at,
      priority = excluded.priority,
      timeout = excluded.timeout,
      worker_id = NULL,
      error_message = NULL
  `, [id, command, max_retries || 3, now, now, runAtVal, priority || 0, timeout || 0]);
}

export async function getJob(id) {
  const db = getDb();
  return db.get(`SELECT * FROM jobs WHERE id = ?`, [id]);
}

export async function getJobSummary() {
  const db = getDb();
  const rows = await db.all(`
    SELECT state, COUNT(*) as count FROM jobs GROUP BY state
  `);
  const summary = { pending: 0, processing: 0, completed: 0, failed: 0, dead: 0 };
  for (const r of rows) {
    if (r.state in summary) {
      summary[r.state] = r.count;
    }
  }
  return summary;
}

export async function listJobs(state = null) {
  const db = getDb();
  if (state) {
    return db.all(`SELECT * FROM jobs WHERE state = ? ORDER BY created_at DESC`, [state]);
  }
  return db.all(`SELECT * FROM jobs ORDER BY created_at DESC`);
}

export async function clearAll() {
  const db = getDb();
  await db.run(`DELETE FROM jobs`);
  await db.run(`DELETE FROM workers`);
  await db.run(`DELETE FROM config`);
}
