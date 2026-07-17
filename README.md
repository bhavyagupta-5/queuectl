# QueueCTL - Background Job Queue System

QueueCTL is a CLI-based background job queue system built in Node.js. It manages background job execution with worker processes, handles job retries using exponential backoff, stores job states in a persistent SQLite database, and routes permanently failing jobs to a Dead Letter Queue (DLQ).

---

## Key Features

*   **Persistent Storage**: Job data survives system restarts using an embedded SQLite database (`queue.db`).
*   **Concurrency and Locking**: Multiple workers run in parallel without duplicate job processing, coordinated atomically via SQLite transaction locks.
*   **Automatic Retries**: Failed jobs automatically retry based on configurable exponential backoff calculations.
*   **Dead Letter Queue (DLQ)**: Jobs exceeding maximum retries are transitioned to the dead state for manual inspection and retrial.
*   **Graceful Shutdown**: Workers capture termination signals and finish their current active job before exiting safely.
*   **Dynamic Configurations**: Configure retry limits, backoff bases, and job timeouts dynamically via the CLI.
*   **Job Timeout Handling**: Limit maximum execution time per job. If a job times out, it is safely terminated and flagged.

---

## Tech Stack

*   **Runtime**: Node.js (v18+) using ES Modules
*   **Database**: SQLite (`sqlite3`)
*   **Process Orchestration**: Native Node child_process module

---

## Installation and Setup

1.  **Navigate** to the project directory.
2.  **Install dependencies**:
    ```bash
    npm install
    ```
3.  **Link the CLI globally**:
    ```bash
    npm link
    ```

---

## CLI Usage and Commands

Run `queuectl --help` to print the command reference:

### 1. Enqueue Jobs
Add a job by providing its details as a JSON string:

* **macOS / Linux**:
  ```bash
  queuectl enqueue '{"id": "job1", "command": "sleep 2 && echo Hello World", "max_retries": 3, "priority": 10}'
  ```

* **Windows Command Prompt (cmd.exe)**:
  ```cmd
  queuectl enqueue "{\"id\": \"job1\", \"command\": \"sleep 2 && echo Hello World\", \"max_retries\": 3, \"priority\": 10}"
  ```

* **Windows PowerShell**:
  ```powershell
  queuectl enqueue '{\"id\": \"job1\", \"command\": \"sleep 2 && echo Hello World\", \"max_retries\": 3, \"priority\": 10}'
  ```

*   **Optional JSON properties**:
    *   `id` (string): Unique identifier (auto-generated UUID if omitted).
    *   `command` (string, required): Shell command to execute.
    *   `max_retries` (number): Max retries (falls back to global config if omitted).
    *   `priority` (number): Execution priority (higher priority runs first, default is 0).
    *   `run_at` (string): ISO datetime string to schedule delayed execution.
    *   `timeout` (number): Maximum runtime in seconds (default is 0, infinite).

### 2. Manage Workers
Start background worker processes:
```bash
queuectl worker start --count 3
```

Stop background workers gracefully:
```bash
queuectl worker stop
```

### 3. Check Queue Status
Show status aggregates and active worker metadata:
```bash
queuectl status
```

### 4. List Jobs
List jobs currently in the database, with optional state filters (pending, processing, completed, failed, dead):
```bash
queuectl list

queuectl list --state pending
```

### 5. Dead Letter Queue (DLQ)
List permanently failed jobs:
```bash
queuectl dlq list
```

Retry a specific dead job or all dead jobs:
```bash
queuectl dlq retry job1

queuectl dlq retry all
```

### 6. Manage Configuration
Set global variables stored persistently:
```bash
queuectl config set max-retries 5

queuectl config set backoff-base 2

queuectl config set default-timeout 10
```

### 7. Start Web Dashboard
Start the monitoring Web UI:
```bash
queuectl dashboard

queuectl dashboard --port 8080
```
Open your browser to `http://localhost:3000` to view active workers, status charts, execution history, read job execution logs, trigger retries for dead jobs, and enqueue new jobs directly from the UI.

---

## Architecture and Internals

### 1. Concurrency Locking
To prevent duplicate job execution when multiple workers poll the database, SQLite's transactional locks are used.
Inside `src/db.js`, the query wraps the SELECT and UPDATE commands in a `BEGIN IMMEDIATE TRANSACTION` block. This locks the database file for writing immediately, preventing any other process from selecting and claiming the same job.

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

### 3. Heartbeats and Crashed Worker Recovery
Each worker process updates its heartbeat timestamp in the database every 3 seconds.
Whenever a worker polls, it scans for inactive workers (heartbeats older than 15 seconds) and cleans them up. If a crashed worker had an active job marked as processing, the recovery step resets the job's state back to failed so it can be reclaimed and retried by other workers.

---

## Testing Instructions

Run the automated test suite to verify all required scenarios:
```bash
npm test
```
