# Live Presentation & Demonstration Script

This document provides a step-by-step presentation playbook for demonstrating the **Distributed Job Processing Platform** during an academic project evaluation, lab viva, or GitHub showcase.

---

## 📋 Terminal Setup Overview

Arrange **4 Terminal Windows / Split Panes** on your display before starting:

```
┌──────────────────────────────┬──────────────────────────────┐
│  Terminal 1: Scheduler       │  Terminal 2: Worker 1        │
│  (Port 8000 + Dashboard)     │  (Autonomous Consumer 1)     │
├──────────────────────────────┼──────────────────────────────┤
│  Terminal 3: Worker 2        │  Terminal 4: Test Client     │
│  (Autonomous Consumer 2)     │  (Job Submission & Polling)  │
└──────────────────────────────┴──────────────────────────────┘
```

---

## 🎬 Step-by-Step Presentation Script

### Step 0: Ensure Redis Message Queue is Running
* **Command**:
  ```powershell
  docker start redis-server || docker run -d --name redis-server -p 6379:6379 redis:latest
  ```
* **Say Out Loud**:
  > *"We start by ensuring our message broker, Redis, is running on port 6379. Redis acts as our Message-Oriented Middleware, establishing location transparency and temporal decoupling between our scheduler and workers."*

---

### Step 1: Start the Scheduler Service (Terminal 1)
* **Command**:
  ```powershell
  venv\Scripts\activate
  uvicorn scheduler.main:app --port 8000 --reload
  ```
* **Say Out Loud**:
  > *"Next, we launch the Scheduler service on port 8000. On startup, it initializes our SQLite persistence database, connects to the Redis queue, and spawns an asynchronous background worker staleness monitor."*

---

### Step 2: Open the Live Web Dashboard (Browser)
* **Action**: Open browser to **[http://localhost:8000/](http://localhost:8000/)**
* **Say Out Loud**:
  > *"Here is our live web dashboard, built for system observability. It polls our aggregated API endpoint every 2 seconds to render live metrics cards, registered worker health, and recent job history without full page reloads."*

---

### Step 3: Launch Worker Consumer 1 (Terminal 2)
* **Command**:
  ```powershell
  venv\Scripts\activate
  python -m worker.consumer
  ```
* **Say Out Loud**:
  > *"Now we launch Worker 1. Notice that Worker 1 generates a unique instance ID, registers itself in SQLite, and immediately spawns a dedicated background thread delivering liveness heartbeats every 5 seconds. Look at the dashboard — Worker 1 appears live with an emerald green online badge."*

---

### Step 4: Launch Worker Consumer 2 (Terminal 3)
* **Command**:
  ```powershell
  venv\Scripts\activate
  python -m worker.consumer
  ```
* **Say Out Loud**:
  > *"We now launch Worker 2 in another terminal session. Notice we didn't update any configuration in the Scheduler or pass any IP addresses. Because we use a pull-based queue model, Worker 2 connects directly to Redis and joins the worker pool instantly. The dashboard now shows 2 / 2 workers online."*

---

### Step 5: Submit Jobs & Observe Dynamic Load Balancing (Terminal 4)
* **Command**:
  ```powershell
  venv\Scripts\activate
  python -m tests.test_client
  ```
* **Say Out Loud**:
  > *"Now we execute our integration test client. Notice that POST /submit_job returns HTTP 202 Accepted with status 'queued' in under 10 milliseconds — the client is not blocked! Over in Redis, BRPOP atomically pops jobs, distributing tasks across Worker 1 and Worker 2 without any duplicate processing or application locks. Watch the dashboard metrics update live!"*

* **Continuous Stream Alternative (Load Generator)**:
  ```powershell
  # Continuously stream jobs to showcase the dashboard in real-time
  python -m tests.load_generator --count 30 --interval 0.5
  # Or run continuously throughout the demo for 60 seconds:
  python -m tests.load_generator --duration 60
  ```

---

### Step 6: The Grand Finale — Worker Failure Detection Live Demo! 💥
* **Action**: Go to **Terminal 2 (Worker 1)** and press **`Ctrl + C`** (or close the terminal window).
* **Say Out Loud**:
  > *"Now for our failure detection demo. I am Simulating an ungraceful node crash by terminating Worker 1 mid-operation. Because physical nodes don't share memory or clocks in a distributed network, the Scheduler relies on heartbeat liveness signals."*

* **Wait ~15 Seconds**: Watch Terminal 1 (Scheduler logs) and the Browser Dashboard.
* **Observe Log**:
  ```text
  WARNING | scheduler | STALENESS CHECKER: Worker(s) ['worker-xyz'] marked OFFLINE (no heartbeat in >15s)
  ```
* **Say Out Loud**:
  > *"Look at the Scheduler logs and the live dashboard! After 15 seconds of heartbeat silence, the Scheduler's background monitor detected the liveness failure, updated SQLite, and turned Worker 1's badge to offline rose-red. Meanwhile, Worker 2 remains online and continues processing queued jobs unimpeded!"*

---

## 🛠️ Troubleshooting Guide ("If Something Goes Wrong")

| Symptom | Cause | Quick Fix Command |
|---|---|---|
| `redis.exceptions.ConnectionError` | Redis Docker container is stopped or not running on port 6379 | `docker start redis-server` |
| `[Errno 10048] address already in use (port 8000)` | Previous Uvicorn instance is still bound to port 8000 | `powershell -Command "Stop-Process -Id (Get-NetTCPConnection -LocalPort 8000).OwningProcess -Force"` |
| `sqlite3.OperationalError: database is locked` | SQLite file opened in exclusive lock mode by external tool | Close DBeaver / DB Browser for SQLite during test runs |
| Dashboard shows `Connection Error` | Scheduler service stopped or crashed | Restart `uvicorn scheduler.main:app --port 8000 --reload` |
