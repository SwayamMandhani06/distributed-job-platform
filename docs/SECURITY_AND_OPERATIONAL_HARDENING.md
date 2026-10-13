# Security, Observability and Operational Hardening Guide

> **Phase 7 Implementation Reference** — Threat modeling, security boundary analysis, input hygiene, observability topology, and operational runbooks for the Distributed Job Platform.

---

## 1. Security Architecture & Threat Model

In an asynchronous distributed architecture with decoupled compute workers, queues, and persistent databases, the threat surface spans network ingress, serialization boundaries, execution runtimes, and coordination metadata.

```
       [ External Untrusted Clients / Web Traffic ]
                           │
                           ▼ (TLS + OAuth2 / API Key Gateway)
                 ┌───────────────────┐
                 │    API Gateway    │ (Edge Auth & Rate Limiting)
                 └─────────┬─────────┘
                           │ (Internal VPC Network)
                           ▼
          ┌──────────────────────────────────┐
          │  FastAPI Scheduler Coordinator   │
          │  • 1MB HTTP Request Body Cap     │
          │  • Alphanumeric Job Type Filter  │
          │  • 512KB Payload Size Validation │
          │  • Correlation ID Tracing        │
          └───────┬──────────────────┬───────┘
                  │                  │
        (Job Enqueue / Leases) (Fenced Transactions)
                  │                  │
                  ▼                  ▼
          ┌───────────────┐  ┌───────────────┐
          │ Redis 7 (AOF) │  │ PostgreSQL 16 │
          └───────┬───────┘  └───────────────┘
                  │
          (Atomic Claim)
                  │
                  ▼
          ┌──────────────────────────────────┐
          │   Autonomous Compute Worker(s)   │
          │  • Explicit Handler Allowlist    │
          │  • OS Process Timeout Isolation  │
          │  • Contextual Trace Propagation  │
          │  • Secret Redaction in Logs      │
          └──────────────────────────────────┘
```

---

## 2. Security Safeguards Implemented

### A. Defensive Input Validation & Request Hygiene
* **Job Type Character Allowlist**: All `job_type` values are strictly validated via regex `^[a-zA-Z0-9_-]{1,64}$`. Any submission containing shell metacharacters, control bytes, or directory traversal sequences (`../`, `;`, `&`, `|`) is rejected immediately with HTTP 422 Unprocessable Entity.
* **Payload Size Constraints**: Job payloads are constrained to $\le 512\text{ KB}$ serialized JSON.
* **HTTP Body Limit Middleware**: Global middleware intercepts requests with `Content-Length > 1,048,576` bytes ($1\text{ MB}$) and terminates the connection with HTTP 413 (Payload Too Large) before loading into memory.
* **JSON Nesting Depth Protection**: Payloads are recursively validated to enforce a maximum nesting depth of 10, preventing stack-overflow recursion attacks.
* **Dunder Key Rejection**: Payloads cannot contain reserved Python dunder attributes (`__class__`, `__globals__`).

### B. Deserialization Safety
* **Zero Untrusted `pickle` Execution**: All client-facing RPC and queue message delivery relies strictly on standard JSON deserialization (`json.loads`). Python's `pickle` library is completely prohibited from parsing external network data, eliminating remote code execution (RCE) via object instantiation gadgets.

### C. FaaS & RPC Execution Sandboxing
* **Method Registration Allowlist**: Networked RPC (`common/distributed_object.py`) accepts only explicitly registered allowlisted methods. Any attempt to invoke private methods (`_foo`), dunder methods (`__init__`), or unregistered endpoints returns HTTP 400 Bad Request.
* **Serverless Process Isolation**: The Serverless Runtime executes handlers inside dedicated child operating system processes (`multiprocessing.Process`) with enforceable execution timeouts (`proc.terminate()` followed by `proc.kill()`), guaranteeing that frozen or rogue code cannot run indefinitely or corrupt shared memory.

### D. Secret Management & Sensitive Data Redaction
* **Environment-Based Secret Injection**: Secrets (`POSTGRES_PASSWORD`, database credentials) are stored exclusively in environment variables or `.env` and are strictly excluded from git tracking (`.gitignore`).
* **Log Redaction Engine**: The structured logging formatter (`common/logging_config.py`) scans all logged dictionaries and message parameters, replacing any fields matching sensitive patterns (`password`, `token`, `secret`, `api_key`, `auth`, `authorization`, `private_key`) with `[REDACTED]`.

---

## 3. Distributed Tracing & Structured Logging

Every incoming request and background operation is assigned a structured correlation identifier:
1. **Correlation Propagation**: The scheduler inspects incoming HTTP headers for `X-Correlation-ID` or `X-Request-ID`. If absent, a unique correlation ID (`corr-<hex>`) is minted.
2. **Contextual Injection**: Using Python `contextvars`, the correlation ID and active `job_id` are automatically injected into all log records emitted during request processing and worker execution:
   ```text
   2026-10-14 01:22:45 | INFO     | scheduler | corr=c8e19a4f job=3df9a1b0 | Received job submission | type=math_prime_sieve L#42
   2026-10-14 01:22:46 | INFO     | worker-1  | corr=c8e19a4f job=3df9a1b0 | Claimed job_id=3df9a1b0 into processing list
   2026-10-14 01:22:47 | INFO     | worker-1  | corr=c8e19a4f job=3df9a1b0 | Job 3df9a1b0 COMPLETED and ACKed by worker-1 in 0.0124s (L#44)
   ```
3. **Response Header**: The `X-Correlation-ID` header is echoed in the HTTP response to the client for end-to-end tracing.

---

## 4. Production Hardening Checklist Before External Exposure

| Boundary | Local Development Setting | Production Hardening Requirement |
| :--- | :--- | :--- |
| **API Authentication** | Open HTTP without auth | Place behind API Gateway (Envoy/Kong) with OAuth2 / JWT / mTLS verification. |
| **Redis Port (6379)** | Bound to `0.0.0.0:6379` | Restrict to internal Docker network; remove host port mapping (`ports:` $\to$ `expose:`). Set `requirepass`. |
| **PostgreSQL Port (5432)**| Bound to `0.0.0.0:5432` | Restrict to internal Docker network; enforce TLS client certificates (`sslmode=verify-full`). |
| **Prometheus (:9090)** | Public | Internal monitoring VPC only, or basic auth / reverse proxy. |
| **Grafana (:3000)** | Default `admin/admin` | Change `GF_SECURITY_ADMIN_PASSWORD` in `.env`; integrate corporate SSO (Okta/Google). |
| **Rate Limiting** | Unrestricted | Enforce token-bucket rate limiting (e.g. 100 req/sec per client IP) at the ingress gateway. |

---

## 5. Prometheus Observability Metrics Reference

The platform exposes real-time Prometheus metrics at `/metrics`:

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `job_platform_jobs_total{status="..."}` | Gauge | Authoritative database job counts (`pending`, `running`, `completed`, `failed`, `dead_letter`). |
| `job_platform_job_submissions_total{job_type="..."}` | Counter | Cumulative job submissions by validated job type. |
| `job_platform_jobs_by_type{job_type="...", status="..."}` | Gauge | Cross-tabulated job counts by category and terminal state. |
| `job_platform_queue_depth{queue="..."}` | Gauge | Live Redis queue depth for `pending` and `processing` queues. |
| `job_platform_job_execution_duration_seconds_sum` | Summary | Sum of elapsed execution durations of completed jobs by type. |
| `job_platform_job_execution_duration_seconds_count` | Summary | Count of completed jobs by type (allows computing average latency). |
| `job_platform_job_execution_duration_seconds_max` | Gauge | Maximum execution duration observed for completed jobs. |
| `job_platform_queue_wait_duration_seconds_sum` | Summary | Cumulative wait duration spent by jobs in queue before claim. |
| `job_platform_dependency_up{dependency="..."}` | Gauge | Operational health of Redis and Database (`1` = Healthy, `0` = Down). |
| `job_platform_dependency_latency_seconds{dependency="..."}` | Gauge | Roundtrip ping/query latency for dependencies in seconds. |
| `job_platform_workers{status="..."}` | Gauge | Registered worker processes (`online`, `stale`, `offline`, `total`). |
| `job_platform_worker_heartbeat_age_seconds{worker_id="..."}` | Gauge | Seconds elapsed since last heartbeat for each worker node. |
| `job_platform_worker_max_heartbeat_age_seconds` | Gauge | Maximum heartbeat age across active registered workers. |
| `job_platform_is_leader` | Gauge | Leader status of this scheduler instance (`1` = Leader, `0` = Standby). |
| `job_platform_leader_epoch` | Gauge | Monotonic leader fencing epoch counter. |
| `job_platform_leader_lease_ttl_seconds` | Gauge | Remaining time-to-live on active leader lease. |
| `job_platform_reconciliation_runs_total` | Counter | Total background stale job recovery sweeps executed. |
| `job_platform_reconciliation_reclaimed_jobs_total` | Counter | Number of abandoned jobs recovered and requeued. |
| `job_platform_reconciliation_evicted_jobs_total` | Counter | Number of unpersisted ghost jobs evicted from Redis queue. |

---

## 6. Prometheus Alerting Rules Summary

Located in `prometheus/alert_rules.yml`:

1. **`JobPlatformDependencyDown`**: Fired when Redis or Database dependency is offline for $\ge 30\text{s}$ (`job_platform_dependency_up == 0`).
2. **`JobPlatformNoActiveLeader`**: Fired when no scheduler node holds the active leader lease for $\ge 60\text{s}$ (`job_platform_is_leader == 0`).
3. **`JobPlatformProlongedWorkerUnavailability`**: Fired when zero online workers exist to process jobs for $\ge 60\text{s}$ (`job_platform_workers{status="online"} == 0`).
4. **`JobPlatformPersistentJobFailures`**: Fired when more than 3 job failures occur in a 5-minute window for $\ge 2\text{m}$.
5. **`JobPlatformHighQueueBacklog`**: Fired when Redis pending queue exceeds 50 items for $\ge 2\text{m}$.
