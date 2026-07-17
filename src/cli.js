#!/usr/bin/env node
import path from 'path';
import { fileURLToPath } from 'url';
import { fork } from 'child_process';
import { 
  initDb, 
  getDb, 
  enqueueJob, 
  getJob, 
  updateJobStatus, 
  listJobs, 
  getJobSummary 
} from './db.js';
import { setConfig, getConfig } from './config.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  red: '\x1b[31m',
  gray: '\x1b[90m'
};

function printHelp() {
  console.log(`
${colors.bright}${colors.cyan}QueueCTL - Background Job Queue System CLI${colors.reset}

${colors.bright}Usage:${colors.reset}
  queuectl <command> [options]

${colors.bright}Commands:${colors.reset}
  ${colors.green}enqueue '<json>'${colors.reset}           Add a new job to the queue
                              Example: queuectl enqueue '{"id":"job1", "command":"sleep 2"}'
  ${colors.green}worker start --count <n>${colors.reset}    Start one or more workers (default count is 1)
  ${colors.green}worker stop${colors.reset}                 Stop running workers gracefully
  ${colors.green}status${colors.reset}                      Show summary of job states and active workers
  ${colors.green}list [--state <state>]${colors.reset}     List jobs (optional filter by state: pending, processing, completed, failed, dead)
  ${colors.green}dlq list${colors.reset}                    List all jobs in the Dead Letter Queue
  ${colors.green}dlq retry <id|all>${colors.reset}          Retry a dead job by ID, or retry all dead jobs
  ${colors.green}config set <key> <val>${colors.reset}      Set configuration parameter (max-retries, backoff-base, default-timeout)

${colors.bright}Job JSON Fields:${colors.reset}
  ${colors.yellow}id${colors.reset} (string, optional)        Unique identifier for the job
  ${colors.yellow}command${colors.reset} (string, required)    The terminal command to execute
  ${colors.yellow}max_retries${colors.reset} (number, optional) Maximum number of execution attempts
  ${colors.yellow}priority${colors.reset} (number, optional)    Execution priority (higher executes first, default 0)
  ${colors.yellow}run_at${colors.reset} (string, optional)      ISO datetime string specifying delayed execution
  ${colors.yellow}timeout${colors.reset} (number, optional)     Maximum execution time in seconds
`);
}

function isPidRunning(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return false;
  }
}

async function run() {
  await initDb();

  const args = process.argv.slice(2);
  if (args.length === 0 || ['--help', '-h', 'help'].includes(args[0])) {
    printHelp();
    process.exit(0);
  }

  const command = args[0];

  try {
    switch (command) {
      case 'enqueue': {
        const jsonStr = args[1];
        if (!jsonStr) {
          console.error(`${colors.red}Error: Please provide a job JSON string.${colors.reset}`);
          console.log(`Example: queuectl enqueue '{"id":"job1", "command":"sleep 2"}'`);
          process.exit(1);
        }

        let jobData;
        try {
          jobData = JSON.parse(jsonStr);
        } catch (err) {
          console.error(`${colors.red}Error: Invalid JSON format. ${err.message}${colors.reset}`);
          process.exit(1);
        }

        if (!jobData.command) {
          console.error(`${colors.red}Error: Job JSON must contain a "command" field.${colors.reset}`);
          process.exit(1);
        }

        if (!jobData.id) {
          jobData.id = `job-${Math.random().toString(36).substring(2, 11)}`;
        }

        await enqueueJob(jobData);
        console.log(`${colors.green}✓ Job enqueued successfully:${colors.reset}`);
        console.log(JSON.stringify(jobData, null, 2));
        break;
      }

      case 'worker': {
        const subCommand = args[1];
        if (subCommand === 'start') {
          let count = 1;
          const countIdx = args.indexOf('--count');
          if (countIdx !== -1 && args[countIdx + 1]) {
            count = parseInt(args[countIdx + 1], 10);
          }

          console.log(`Starting ${count} worker(s)...`);
          const workerPath = path.resolve(__dirname, 'worker.js');
          
          for (let i = 0; i < count; i++) {
            const workerId = `worker-${Math.random().toString(36).substring(2, 7)}-${process.pid}`;
            const child = fork(workerPath, [], {
              detached: true,
              stdio: 'ignore',
              env: {
                ...process.env,
                WORKER_ID: workerId
              }
            });
            child.unref();
            console.log(`${colors.green}✓ Spawned worker process (PID: ${child.pid}, ID: ${workerId})${colors.reset}`);
          }
        } else if (subCommand === 'stop') {
          console.log('Stopping active workers gracefully...');
          const db = getDb();
          const activeWorkers = await db.all(`SELECT * FROM workers`);
          
          let stoppedCount = 0;
          for (const worker of activeWorkers) {
            if (isPidRunning(worker.pid)) {
              console.log(`Sending SIGTERM to worker ${worker.id} (PID: ${worker.pid})...`);
              try {
                process.kill(worker.pid, 'SIGTERM');
                stoppedCount++;
              } catch (e) {
                console.error(`Failed to kill process ${worker.pid}:`, e.message);
              }
            } else {
              await db.run(`DELETE FROM workers WHERE id = ?`, [worker.id]);
            }
          }
          
          if (stoppedCount === 0) {
            console.log('No active workers found.');
          } else {
            console.log(`${colors.green}Sent stop signal to ${stoppedCount} worker(s). They will finish their active jobs before exiting.${colors.reset}`);
          }
        } else {
          console.error(`${colors.red}Error: Unknown worker subcommand. Use "start" or "stop".${colors.reset}`);
          process.exit(1);
        }
        break;
      }

      case 'status': {
        const summary = await getJobSummary();
        const db = getDb();
        const workers = await db.all(`SELECT * FROM workers`);

        console.log(`\n${colors.bright}${colors.cyan}--- QUEUE STATUS ---${colors.reset}`);
        console.log(`${colors.bright}Job States:${colors.reset}`);
        console.log(`  Pending:    ${summary.pending}`);
        console.log(`  Processing: ${summary.processing}`);
        console.log(`  Completed:  ${summary.completed}`);
        console.log(`  Failed:     ${summary.failed}`);
        console.log(`  Dead (DLQ): ${summary.dead}`);

        console.log(`\n${colors.bright}Active Workers:${colors.reset}`);
        const realActiveWorkers = workers.filter(w => isPidRunning(w.pid));
        
        for (const w of workers) {
          if (!isPidRunning(w.pid)) {
            await db.run(`DELETE FROM workers WHERE id = ?`, [w.id]);
          }
        }

        if (realActiveWorkers.length === 0) {
          console.log('  No active workers running.');
        } else {
          realActiveWorkers.forEach(w => {
            const jobStr = w.current_job_id ? `(Job ID: ${w.current_job_id})` : '(Idle)';
            console.log(`  ● ID: ${w.id} | PID: ${w.pid} | State: ${w.state} ${jobStr}`);
          });
        }
        console.log();
        break;
      }

      case 'list': {
        let stateFilter = null;
        const stateIdx = args.indexOf('--state');
        if (stateIdx !== -1 && args[stateIdx + 1]) {
          stateFilter = args[stateIdx + 1];
        }

        const jobs = await listJobs(stateFilter);
        if (jobs.length === 0) {
          console.log('No jobs found.');
          break;
        }

        console.log(`\n${colors.bright}Jobs (${jobs.length}):${colors.reset}`);
        console.log(
          `${colors.bright}${'ID'.padEnd(12)} | ${'State'.padEnd(10)} | ${'Attempts'.padEnd(8)} | ${'Priority'.padEnd(8)} | ${'Run At'.padEnd(24)} | ${'Command'}${colors.reset}`
        );
        console.log('-'.repeat(90));
        
        jobs.forEach(j => {
          let stateColor = colors.reset;
          if (j.state === 'completed') stateColor = colors.green;
          else if (j.state === 'processing') stateColor = colors.blue;
          else if (j.state === 'failed') stateColor = colors.yellow;
          else if (j.state === 'dead') stateColor = colors.red;

          console.log(
            `${j.id.padEnd(12)} | ${`${stateColor}${j.state}${colors.reset}`.padEnd(10 + stateColor.length + colors.reset.length)} | ${String(j.attempts).padEnd(8)} | ${String(j.priority).padEnd(8)} | ${j.run_at.padEnd(24)} | ${j.command}`
          );
        });
        console.log();
        break;
      }

      case 'dlq': {
        const subCommand = args[1];
        if (subCommand === 'list') {
          const deadJobs = await listJobs('dead');
          if (deadJobs.length === 0) {
            console.log('Dead Letter Queue (DLQ) is empty.');
            break;
          }

          console.log(`\n${colors.bright}${colors.red}Dead Letter Queue (DLQ) (${deadJobs.length} jobs):${colors.reset}`);
          console.log(
            `${colors.bright}${'ID'.padEnd(12)} | ${'Attempts'.padEnd(8)} | ${'Failed At'.padEnd(24)} | ${'Error Message'}${colors.reset}`
          );
          console.log('-'.repeat(90));
          
          deadJobs.forEach(j => {
            console.log(
              `${j.id.padEnd(12)} | ${String(j.attempts).padEnd(8)} | ${j.updated_at.padEnd(24)} | ${j.error_message || 'N/A'}`
            );
          });
          console.log();
        } else if (subCommand === 'retry') {
          const targetId = args[2];
          if (!targetId) {
            console.error(`${colors.red}Error: Please specify job ID or "all" to retry.${colors.reset}`);
            process.exit(1);
          }

          const db = getDb();
          if (targetId === 'all') {
            const deadJobs = await listJobs('dead');
            if (deadJobs.length === 0) {
              console.log('No dead jobs to retry.');
              break;
            }

            for (const job of deadJobs) {
              await updateJobStatus(job.id, 'pending', new Date().toISOString(), null);
              await db.run(`UPDATE jobs SET attempts = 0 WHERE id = ?`, [job.id]);
            }
            console.log(`${colors.green}✓ Retrying all ${deadJobs.length} dead jobs. Reset states to pending.${colors.reset}`);
          } else {
            const job = await getJob(targetId);
            if (!job) {
              console.error(`${colors.red}Error: Job ${targetId} not found.${colors.reset}`);
              process.exit(1);
            }
            if (job.state !== 'dead') {
              console.error(`${colors.red}Error: Job ${targetId} is not in dead state (current state: ${job.state}).${colors.reset}`);
              process.exit(1);
            }

            await updateJobStatus(targetId, 'pending', new Date().toISOString(), null);
            await db.run(`UPDATE jobs SET attempts = 0 WHERE id = ?`, [targetId]);
            console.log(`${colors.green}✓ Job ${targetId} moved back to queue (pending).${colors.reset}`);
          }
        } else {
          console.error(`${colors.red}Error: Unknown dlq subcommand. Use "list" or "retry".${colors.reset}`);
          process.exit(1);
        }
        break;
      }

      case 'config': {
        const action = args[1];
        if (action === 'set') {
          const key = args[2];
          const val = args[3];
          if (!key || val === undefined) {
            console.error(`${colors.red}Error: Please specify config key and value.${colors.reset}`);
            console.log('Example: queuectl config set max-retries 5');
            process.exit(1);
          }

          if (!['max-retries', 'backoff-base', 'default-timeout'].includes(key)) {
            console.error(`${colors.red}Error: Invalid config key. Supported keys: max-retries, backoff-base, default-timeout${colors.reset}`);
            process.exit(1);
          }

          if (key === 'max-retries' || key === 'default-timeout') {
            if (isNaN(parseInt(val, 10)) || parseInt(val, 10) < 0) {
              console.error(`${colors.red}Error: Value for ${key} must be a non-negative integer.${colors.reset}`);
              process.exit(1);
            }
          } else if (key === 'backoff-base') {
            if (isNaN(parseFloat(val)) || parseFloat(val) <= 0) {
              console.error(`${colors.red}Error: Value for backoff-base must be a positive number.${colors.reset}`);
              process.exit(1);
            }
          }

          await setConfig(key, val);
          console.log(`${colors.green}✓ Config ${key} successfully set to ${val}${colors.reset}`);
        } else {
          console.error(`${colors.red}Error: Unknown config subcommand. Use "set".${colors.reset}`);
          process.exit(1);
        }
        break;
      }

      default: {
        console.error(`${colors.red}Error: Unknown command "${command}".${colors.reset}`);
        printHelp();
        process.exit(1);
      }
    }
    try {
      const { getDb } = await import('./db.js');
      const db = getDb();
      if (db) await db.close();
    } catch (e) {}
    process.exit(0);
  } catch (err) {
    console.error(`${colors.red}Fatal Error: ${err.message}${colors.reset}`);
    process.exit(1);
  }
}

if (import.meta.url === `file://${process.argv[1]}` || process.argv[1].endsWith('queuectl') || process.argv[1].endsWith('queuectl.js')) {
  run();
}
export { run };
