import { fork } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { 
  initDb, 
  getDb, 
  enqueueJob, 
  getJob, 
  clearAll 
} from '../src/db.js';
import { setConfig } from '../src/config.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const workerPath = path.resolve(__dirname, '../src/worker.js');

const colors = {
  reset: '\x1b[0m',
  green: '\x1b[32m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  bright: '\x1b[1m'
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

let spawnedPids = [];

function startWorkerProcess(workerId = 'test-worker') {
  const child = fork(workerPath, [], {
    stdio: 'inherit',
    env: { ...process.env, WORKER_ID: workerId }
  });
  spawnedPids.push(child.pid);
  return child;
}

function stopWorkerProcess(child) {
  return new Promise((resolve) => {
    if (child.exitCode !== null) {
      resolve();
      return;
    }
    child.once('exit', () => {
      spawnedPids = spawnedPids.filter(p => p !== child.pid);
      resolve();
    });
    child.kill('SIGTERM');
  });
}

async function runTests() {
  console.log(`\n${colors.bright}${colors.cyan}======================================================`);
  console.log(`🧪 RUNNING QUEUECTL AUTOMATED TEST SUITE`);
  console.log(`======================================================${colors.reset}\n`);


  await initDb();
  await clearAll();
  await initDb();

  let failedTests = 0;

  
  try {
    console.log(`${colors.bright}[Test 1] Basic Job Completes Successfully...${colors.reset}`);
    await enqueueJob({
      id: 'test-basic',
      command: 'echo "Test 1 Execution"',
      max_retries: 3,
      priority: 0,
      timeout: 0
    });

    const worker = startWorkerProcess('worker-test-1');
    

    let success = false;
    for (let i = 0; i < 50; i++) {
      await sleep(100);
      const job = await getJob('test-basic');
      if (job && job.state === 'completed') {
        success = true;
        break;
      }
    }

    await stopWorkerProcess(worker);

    if (success) {
      console.log(`${colors.green}✓ Test 1 Passed!${colors.reset}\n`);
    } else {
      console.log(`${colors.red}✗ Test 1 Failed! Job did not complete successfully.${colors.reset}\n`);
      failedTests++;
    }
  } catch (err) {
    console.error(`${colors.red}Test 1 Error: ${err.message}${colors.reset}\n`);
    failedTests++;
  }


  try {
    console.log(`${colors.bright}[Test 2] Failed Job Retries with Backoff & Moves to DLQ...${colors.reset}`);
   
    await enqueueJob({
      id: 'test-retry-dlq',
      command: 'nonexistentcommand1234',
      max_retries: 2,
      priority: 0,
      timeout: 0
    });
    
    
    await setConfig('backoff-base', '1.5'); 

    const worker = startWorkerProcess('worker-test-2');

    let reachedDeadState = false;
    let attemptsCounted = 0;

    for (let i = 0; i < 60; i++) {
      await sleep(200);
      const job = await getJob('test-retry-dlq');
      if (job) {
        attemptsCounted = job.attempts;
        if (job.state === 'dead') {
          reachedDeadState = true;
          break;
        }
      }
    }

    await stopWorkerProcess(worker);

    if (reachedDeadState && attemptsCounted === 2) {
      console.log(`${colors.green}✓ Test 2 Passed! Job failed, retried, and moved to DLQ.${colors.reset}\n`);
    } else {
      console.log(`${colors.red}✗ Test 2 Failed! Expected dead state with 2 attempts, got state: ${reachedDeadState ? 'dead' : 'not dead'} and attempts: ${attemptsCounted}.${colors.reset}\n`);
      failedTests++;
    }
  } catch (err) {
    console.error(`${colors.red}Test 2 Error: ${err.message}${colors.reset}\n`);
    failedTests++;
  }

  try {
    console.log(`${colors.bright}[Test 3] Concurrency & Locking: 3 Workers, 6 Jobs...${colors.reset}`);
    await clearAll();
    await initDb();

    const jobIds = ['c-job-1', 'c-job-2', 'c-job-3', 'c-job-4', 'c-job-5', 'c-job-6'];
    for (const id of jobIds) {
      await enqueueJob({
        id,
        command: 'sleep 0.5',
        max_retries: 3,
        priority: 0,
        timeout: 0
      });
    }


    const w1 = startWorkerProcess('w-1');
    const w2 = startWorkerProcess('w-2');
    const w3 = startWorkerProcess('w-3');


    let allFinished = false;
    let jobRecords = [];

    for (let i = 0; i < 60; i++) {
      await sleep(200);
      let finishedCount = 0;
      jobRecords = [];
      
      for (const id of jobIds) {
        const j = await getJob(id);
        if (j) {
          jobRecords.push(j);
          if (j.state === 'completed') {
            finishedCount++;
          }
        }
      }
      if (finishedCount === 6) {
        allFinished = true;
        break;
      }
    }

    await stopWorkerProcess(w1);
    await stopWorkerProcess(w2);
    await stopWorkerProcess(w3);


    const workerAssignments = jobRecords.map(j => j.worker_id);
    const uniqueWorkers = new Set(workerAssignments.filter(Boolean));
    
   
    const hasMultipleWorkers = uniqueWorkers.size > 1;
    const noDuplicates = jobRecords.every(j => j.attempts === 1);

    if (allFinished && hasMultipleWorkers && noDuplicates) {
      console.log(`${colors.green}✓ Test 3 Passed! Jobs were parallelized across ${uniqueWorkers.size} workers with no overlap.${colors.reset}\n`);
    } else {
      console.log(`${colors.red}✗ Test 3 Failed! Finished: ${allFinished}, Workers involved: ${uniqueWorkers.size}, No duplicate executions check: ${noDuplicates}.${colors.reset}\n`);
      failedTests++;
    }
  } catch (err) {
    console.error(`${colors.red}Test 3 Error: ${err.message}${colors.reset}\n`);
    failedTests++;
  }

  try {
    console.log(`${colors.bright}[Test 4] Job Timeout Handling (Bonus)...${colors.reset}`);
    await clearAll();
    await initDb();

    
    await enqueueJob({
      id: 'test-timeout',
      command: 'sleep 5',
      max_retries: 1, 
      priority: 0,
      timeout: 1 
    });

    const worker = startWorkerProcess('worker-test-4');

    let isDead = false;
    let errorMessage = null;

    for (let i = 0; i < 40; i++) {
      await sleep(150);
      const job = await getJob('test-timeout');
      if (job && job.state === 'dead') {
        isDead = true;
        errorMessage = job.error_message;
        break;
      }
    }

    await stopWorkerProcess(worker);

    if (isDead && errorMessage === 'TIMEOUT_ERROR') {
      console.log(`${colors.green}✓ Test 4 Passed! Job timed out after 1s, was terminated and moved to DLQ.${colors.reset}\n`);
    } else {
      console.log(`${colors.red}✗ Test 4 Failed! IsDead: ${isDead}, Error message: ${errorMessage}.${colors.reset}\n`);
      failedTests++;
    }
  } catch (err) {
    console.error(`${colors.red}Test 4 Error: ${err.message}${colors.reset}\n`);
    failedTests++;
  }

  
  try {
    console.log(`${colors.bright}[Test 5] Job Persistence Across Worker Restarts...${colors.reset}`);
    await clearAll();
    await initDb();

    await enqueueJob({
      id: 'test-persistence',
      command: 'echo "Job run after restart"',
      max_retries: 3,
      priority: 0,
      timeout: 0
    });


    let job = await getJob('test-persistence');
    const isPendingInitially = job && job.state === 'pending';

    const worker = startWorkerProcess('worker-test-5');
    
    let isCompleted = false;
    for (let i = 0; i < 30; i++) {
      await sleep(100);
      job = await getJob('test-persistence');
      if (job && job.state === 'completed') {
        isCompleted = true;
        break;
      }
    }

    await stopWorkerProcess(worker);

    if (isPendingInitially && isCompleted) {
      console.log(`${colors.green}✓ Test 5 Passed! Job survived restarts and executed successfully.${colors.reset}\n`);
    } else {
      console.log(`${colors.red}✗ Test 5 Failed! Initial Pending: ${isPendingInitially}, Completed: ${isCompleted}.${colors.reset}\n`);
      failedTests++;
    }
  } catch (err) {
    console.error(`${colors.red}Test 5 Error: ${err.message}${colors.reset}\n`);
    failedTests++;
  }


  spawnedPids.forEach(pid => {
    try {
      process.kill(pid, 'SIGKILL');
    } catch (e) {}
  });

  console.log(`======================================================`);
  if (failedTests === 0) {
    console.log(`${colors.green}${colors.bright}🎉 ALL TESTS PASSED SUCCESSFULLY!${colors.reset}`);
  } else {
    console.log(`${colors.red}${colors.bright}🚨 TEST SUITE FAILED WITH ${failedTests} FAILURE(S)${colors.reset}`);
    process.exit(1);
  }
  console.log(`======================================================\n`);
  process.exit(0);
}

runTests();
