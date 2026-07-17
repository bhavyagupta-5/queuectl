# QueueCTL - Background Job Queue System

QueueCTL is a production-grade, CLI-based background job queue system built in Node.js. It manages background job execution with worker processes, handles job retries using exponential backoff, stores job states in a persistent SQLite database (ensuring ACID compliance and lock-safe concurrent polling), and routes permanently failing jobs to a Dead Letter Queue (DLQ).

---

## 🚀 Key Features

*   **Persistent Storage**: Job data survives system restarts using an embedded SQLite database (`queue.db`).
*   **Concurrency & Locking**: Multiple workers run in parallel without duplicate job processing, coordinated atomically via SQLite `BEGIN IMMEDIATE TRANSACTION` blocks.
*   **Automatic Retries**: Failed jobs automatically retry based on configurable exponential backoff calculations (`delay = base ^ attempts seconds`).
*   **Dead Letter Queue (DLQ)**: Jobs exceeding maximum retries are transitioned to the `dead` state (DLQ) for manual inspection and retrial.
*   **Graceful Shutdown**: Workers capture termination signals (`SIGTERM`, `SIGINT`) and finish their current active job before exiting safely.
*   **Dynamic Configurations**: Configure retry limits, backoff bases, and job timeouts dynamically via the CLI.
*   **Job Timeout Handling (Bonus)**: Limit maximum execution time per job. If a job times out, it is safely terminated and flagged.

---

## 🛠️ Tech Stack

*   **Runtime**: Node.js (v18+) using ES Modules
*   **Database**: SQLite (`sqlite3`)
*   **Process Orchestration**: Native Node `child_process` (forking detached daemons, signal signaling, stdout/stderr streaming)

---

## ⚙️ Installation & Setup

1.  **Clone / Navigate** to the project workspace directory.
2.  **Install dependencies**:
    ```bash
    npm install
    ```
3.  **Link the CLI globally**:
    ```bash
    npm link
    ```
    *(Note: This creates a global symlink, allowing you to run `queuectl` from anywhere on your machine).*

---

## 📖 CLI Usage & Commands

Run `queuectl --help` to print the command reference:

### 1. Enqueue Jobs
Add a job by providing its details as a JSON string.
```bash
queuectl enqueue '{"id": "job1", "command": "sleep 2 && echo Hello World", "max_retries": 3, "priority": 10}'
```
*   **Optional JSON properties**:
    *   `id` (string): Unique identifier (auto-generated UUID if omitted).
    *   `command` (string, required): Shell command to execute.
    *   `max_retries` (number): Max retries (falls back to global config if omitted).
    *   `priority` (number): Execution priority (higher priority runs first, default is `0`).
    *   `run_at` (string): ISO datetime string to schedule delayed execution.
    *   `timeout` (number): Maximum runtime in seconds (default is `0`, infinite).

### 2. Manage Workers
Start background worker processes:
```bash
# Start 3 background worker processes
queuectl worker start --count 3
```

Stop background workers gracefully:
```bash
queuectl worker stop
```
*Workers will capture the `SIGTERM` signal, wait for their current running command to complete, update the database, and exit.*

### 3. Check Queue Status
Show status aggregates and active worker metadata:
```bash
queuectl status
```

### 4. List Jobs
List jobs currently in the database, with optional state filters (`pending`, `processing`, `completed`, `failed`, `dead`):
```bash
# List all jobs
queuectl list

# List pending jobs
queuectl list --state pending
```

### 5. Dead Letter Queue (DLQ)
List permanently failed jobs:
```bash
queuectl dlq list
```

Retry a specific dead job or all dead jobs:
```bash
# Retry job by ID
queuectl dlq retry job1

# Retry all dead jobs in the DLQ
queuectl dlq retry all
```

### 6. Manage Configuration
Set global variables stored persistently:
```bash
# Change max retries
queuectl config set max-retries 5

# Change backoff base multiplier
queuectl config set backoff-base 2

# Change default timeout (seconds)
queuectl config set default-timeout 10
```

---

## 🏗️ Architecture & Internals

### 1. Concurrency Locking
To prevent duplicate job execution when multiple workers poll the database, SQLite's transactional locks are used.
Inside `src/db.js`, `fetchNextJob` wraps the SELECT and UPDATE queries in a `BEGIN IMMEDIATE TRANSACTION`. This locks the database file for writing immediately, preventing any other process from selecting and claiming the same job.

### 2. Job Lifecycle State Machine

```
      [enqueue]
          │
          ▼
   ┌─────────────┐
   │   pending   │◄───────────────────────┐
   └──────┬──────┘                        │
          │                               │
          │ (worker claims)               │
          ▼                               │
   ┌─────────────┐                        │ (retry: delay elapsed)
   │ processing  │                        │
   └────┬───┬────┘                        │
        │   │                             │
        │   ├────────────────────────┐    │
        │   │ (success: exit 0)      │    │
        │   ▼                        ▼    │
        │ ┌───────────┐            ┌──────┴──────┐
        │ │ completed │            │   failed    │
        │ └───────────┘            └──────┬──────┘
        │                                 │
        │ (attempts >= max_retries)       │
        ▼                                 ▼
   ┌─────────────┐                 (attempts < max_retries)
   │    dead     │
   │    (DLQ)    │
   └─────────────┘
```

### 3. Heartbeats & Crashed Worker Recovery
Each worker process updates its heartbeat timestamp in the `workers` table every 3 seconds.
Whenever a worker polls, it scans for inactive workers (heartbeat older than 15 seconds) and cleans them up. If a crashed worker had an active job marked as `processing`, the recovery step resets the job's state back to `failed` so it can be reclaimed and retried by other workers.

---

## 🧪 Testing Instructions

Run the automated test suite to verify all required scenarios (including locking, backoff, DLQ, persistence, and timeouts):
```bash
npm test
```
The test suite spins up dummy worker processes, enqueues specific jobs, asserts state changes in the database, verifies parallel worker division (non-overlapping), validates timeout terminations, and handles teardown automatically.
