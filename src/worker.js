import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import { 
  initDb, 
  fetchNextJob, 
  registerWorker, 
  updateWorkerHeartbeat, 
  removeWorker, 
  updateJobStatus, 
  cleanupStaleWorkers 
} from './db.js';
import { getConfigInt, getConfigFloat } from './config.js';

const pid = process.pid;
const workerId = process.env.WORKER_ID || `worker-${pid}-${Date.now()}`;
let stopping = false;
let currentJob = null;
let activeChildProcess = null;

const logsDir = process.env.LOGS_PATH || path.resolve(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

console.log(`[Worker ${workerId}] Starting up...`);

async function main() {
  await initDb();
  await registerWorker(workerId, pid);

  const heartbeatInterval = setInterval(async () => {
    try {
      const state = currentJob ? 'processing' : 'idle';
      const jobId = currentJob ? currentJob.id : null;
      await updateWorkerHeartbeat(workerId, state, jobId);
      await cleanupStaleWorkers(15);
    } catch (err) {
      console.error(`[Worker ${workerId}] Heartbeat update failed:`, err.message);
    }
  }, 3000);

  const shutdown = async (signal) => {
    if (stopping) return;
    stopping = true;
    console.log(`\n[Worker ${workerId}] Received ${signal}. Starting graceful shutdown...`);
    
    clearInterval(heartbeatInterval);

    if (currentJob) {
      console.log(`[Worker ${workerId}] Waiting for current job ${currentJob.id} to finish...`);
    } else {
      await cleanupAndExit();
    }
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  async function pollLoop() {
    if (stopping) {
      await cleanupAndExit();
      return;
    }

    try {
      const job = await fetchNextJob(workerId);
      if (job) {
        currentJob = job;
        console.log(`[Worker ${workerId}] Processing job: ${job.id} ("${job.command}")`);
        
        await updateWorkerHeartbeat(workerId, 'processing', job.id);
        
        const result = await executeJob(job);
        
        await handleJobCompletion(job, result);
        
        currentJob = null;
        activeChildProcess = null;
        
        setImmediate(pollLoop);
      } else {
        setTimeout(pollLoop, 1000);
      }
    } catch (error) {
      console.error(`[Worker ${workerId}] Error in poll loop:`, error);
      setTimeout(pollLoop, 1000);
    }
  }

  pollLoop();
}

async function cleanupAndExit() {
  try {
    console.log(`[Worker ${workerId}] Removing worker registration.`);
    await removeWorker(workerId);
  } catch (err) {
    console.error(`[Worker ${workerId}] Failed to clean up worker database row:`, err.message);
  }
  console.log(`[Worker ${workerId}] Gracefully stopped.`);
  process.exit(0);
}

function executeJob(job) {
  return new Promise((resolve) => {
    const logPath = path.join(logsDir, `job-${job.id}.log`);
    const logStream = fs.createWriteStream(logPath, { flags: 'a' });

    logStream.write(`\n=== JOB START: ${new Date().toISOString()} ===\n`);
    logStream.write(`Command: ${job.command}\n`);
    logStream.write(`Attempt: ${job.attempts}\n\n`);

    const child = spawn(job.command, { shell: true });
    activeChildProcess = child;

    let killedByTimeout = false;
    let timeoutTimer = null;

    const jobTimeout = job.timeout > 0 ? job.timeout : null;
    
    if (jobTimeout) {
      timeoutTimer = setTimeout(() => {
        killedByTimeout = true;
        console.log(`[Worker ${workerId}] Job ${job.id} timed out after ${jobTimeout}s. Terminating...`);
        logStream.write(`\n[ERROR] Job exceeded timeout limit of ${jobTimeout}s. Terminating...\n`);
        
        child.kill('SIGKILL');
      }, jobTimeout * 1000);
    }

    child.stdout.on('data', (data) => {
      logStream.write(data);
    });

    child.stderr.on('data', (data) => {
      logStream.write(data);
    });

    child.on('error', (err) => {
      logStream.write(`\n[ERROR] Spawn failed: ${err.message}\n`);
      resolve({ success: false, error: `Spawn error: ${err.message}` });
    });

    child.on('exit', (code, signal) => {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      
      logStream.write(`\n=== JOB END: ${new Date().toISOString()} ===\n`);
      logStream.write(`Exit Code: ${code !== null ? code : 'N/A'}\n`);
      if (signal) logStream.write(`Signal: ${signal}\n`);
      logStream.end();

      if (killedByTimeout) {
        resolve({ success: false, error: 'TIMEOUT_ERROR' });
      } else if (code === 0) {
        resolve({ success: true });
      } else {
        resolve({ success: false, error: `Exit code: ${code}, signal: ${signal}` });
      }
    });
  });
}

async function handleJobCompletion(job, result) {
  if (result.success) {
    console.log(`[Worker ${workerId}] Job ${job.id} completed successfully.`);
    await updateJobStatus(job.id, 'completed', null, null);
  } else {
    console.log(`[Worker ${workerId}] Job ${job.id} failed: ${result.error}`);

    const maxRetries = job.max_retries !== undefined ? job.max_retries : await getConfigInt('max-retries');
    const backoffBase = await getConfigFloat('backoff-base');

    if (job.attempts < maxRetries) {
      const delaySeconds = Math.pow(backoffBase, job.attempts);
      const runAt = new Date(Date.now() + delaySeconds * 1000).toISOString();
      
      console.log(`[Worker ${workerId}] Job ${job.id} will retry (Attempt ${job.attempts}/${maxRetries}) in ${delaySeconds}s (run_at: ${runAt}).`);
      await updateJobStatus(job.id, 'failed', runAt, result.error);
    } else {
      console.log(`[Worker ${workerId}] Job ${job.id} failed after ${job.attempts} attempts. Moving to DLQ.`);
      await updateJobStatus(job.id, 'dead', null, result.error);
    }
  }
}

main().catch((err) => {
  console.error(`[Worker ${workerId}] Fatal error in main:`, err);
  process.exit(1);
});
