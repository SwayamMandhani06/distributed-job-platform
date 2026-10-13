# Distributed Background Job Processing Platform

## Final Mini-Project Upgrade Plan --- FA-2 (Units III & IV) + Complete Syllabus Coverage

**Project type:** Distributed Systems mini-project\
**Existing repository:**
https://github.com/SwayamMandhani06/distributed-job-platform\
**Implementation approach:** Upgrade the existing FA-1 codebase
incrementally; do not rebuild from scratch.\
**Primary goal:** Turn the existing background-job processing prototype
into a demonstrable, tested distributed system that covers the concepts
from Units I--IV and adds the required Unit III/IV upgrades for FA-2.

------------------------------------------------------------------------

## 1. Project Overview

### 1.1 Problem statement

Applications often need to run tasks that should not block a user-facing
request, such as sending notifications, processing CSV files, generating
image thumbnails, or performing report generation. A single process can
become a bottleneck and may lose work when it crashes. This project
provides a distributed platform in which clients submit jobs, a
scheduler coordinates job assignment, workers execute jobs
independently, and the system tracks job state, results, failures, and
worker health.

The FA-2 upgrade extends the original single-scheduler design with
multiple scheduler instances, leader election, logical clocks,
distributed mutual exclusion, recovery mechanisms, and cluster-state
snapshots.

### 1.2 Project objectives

1.  Accept jobs through a documented HTTP API and a dashboard.
2.  Store job metadata, execution attempts, results, timestamps, and
    event history.
3.  Distribute jobs to independent worker processes using a message
    queue.
4.  Implement actual job handlers for CSV processing and image-thumbnail
    generation, plus a clearly labelled simulated notification handler.
5.  Track worker health with heartbeats and expose useful status
    information.
6.  Add Lamport logical clocks and timestamped distributed event logs.
7.  Add Bully leader election across scheduler replicas and ensure that
    only the current coordinator schedules work.
8.  Implement distributed mutual exclusion for a shared critical-section
    demonstration.
9.  Recover stale/in-progress jobs after worker failures using
    acknowledgement, timeout, retry, and dead-letter handling.
10. Capture and display a useful cluster-state snapshot.
11. Containerize the system with Docker Compose and make the full demo
    repeatable.
12. Test normal operation, concurrency, failure scenarios, recovery, and
    coordinator changes.
13. Map implemented features to the Distributed Systems syllabus and
    prepare clear evidence for FA-2 viva and final mini-project
    evaluation.

### 1.3 Scope and honest claims

This is an educational distributed-systems project, not a
production-grade job platform. The implementation should demonstrate the
algorithms and their limits accurately.

-   Redis queue operations can atomically assign a queue item, but this
    alone does **not** guarantee exactly-once execution.
-   A worker can perform a side effect and crash before recording
    success. Retries may therefore execute a job more than once;
    handlers should be idempotent where practical.
-   Bully election chooses a coordinator under the assumptions of the
    demonstration, but it is **not consensus** and does not by itself
    prevent split-brain during partitions or long pauses. The
    implementation must add a Redis-backed leader lease and
    monotonically increasing fencing token, and document the remaining
    limitations.
-   SQLite is acceptable for a small, single-host demonstration if
    access is serialized appropriately. If concurrent scheduler writes
    cause locking problems, use PostgreSQL as the shared metadata
    database.
-   Logical clocks order events; they do not synchronize physical time.
-   A cluster snapshot is an observed state, not automatically a
    mathematically consistent distributed snapshot. If a formal
    consistent-snapshot algorithm is required by the faculty, implement
    the optional Chandy--Lamport extension described below.

------------------------------------------------------------------------

## 2. Starting Point and Repository Audit

Before implementing features, inspect the current repository and record
its actual state. Do not assume that old documentation exactly matches
the current code.

### 2.1 Audit checklist

-   [ ] Inspect the repository tree, current branch, README,
    architecture documentation, dependency files, and environment
    examples.
-   [ ] Identify the real API framework and the actual
    client-to-scheduler communication path.
-   [ ] Identify queue keys, queue semantics, job states, worker
    registration/heartbeat logic, database schema, and dashboard routes.
-   [ ] Find all RPC/gRPC code and establish whether it is actively
    used, experimental, or obsolete.
-   [ ] Run the current application and existing tests from a clean
    environment.
-   [ ] Record current known defects and the commands needed to
    reproduce them.
-   [ ] Remove or update documentation claims that are not implemented.
-   [ ] Create a tagged baseline or branch such as `fa1-baseline` before
    changing behavior.

### 2.2 Resolve the HTTP/RPC architecture decision

The repository must document one clear architecture rather than claiming
RPC when the active path uses REST.

**Recommended decision:** keep HTTP/REST for client submission,
dashboard access, and operational endpoints; add a small, genuine gRPC
service for an explicit service-to-service operation if RPC is required
by the course rubric. If the course accepts HTTP APIs as the
communication demonstration, document HTTP/REST honestly and do not
claim gRPC.

Possible gRPC operations: - `SubmitJob` or `GetJobStatus` between a
client/service and scheduler. - `RegisterWorker` or `ReportHeartbeat`
between a worker and scheduler.

Do not maintain two competing implementations of the same critical path
unless there is a clear reason. The README must identify which path is
used in the main demo.

### 2.3 Baseline acceptance criteria

-   [ ] The project can be installed from documented steps.
-   [ ] The API/dashboard starts and reports its health.
-   [ ] Redis and the metadata database connect successfully.
-   [ ] A job can be submitted, claimed by a worker, completed, and
    queried.
-   [ ] A worker heartbeat appears in the dashboard or status endpoint.
-   [ ] The existing behavior has at least basic automated tests.
-   [ ] The README architecture matches the running implementation.

------------------------------------------------------------------------

## 3. Target Architecture

### 3.1 Logical components

1.  **Client / Dashboard**
    -   Submit jobs and view job details.
    -   Show queue depth, job counts, worker status, scheduler leader,
        election events, logical-clock events, lock state, retries, and
        snapshots.
2.  **API service**
    -   Validate requests and job payloads.
    -   Create job metadata and publish accepted jobs.
    -   Expose status, result, history, health, metrics, and
        administrative demonstration endpoints.
    -   Never execute long-running jobs inside the API request handler.
3.  **Scheduler replicas**
    -   Run as independent processes/containers with unique scheduler
        IDs.
    -   Participate in Bully election.
    -   Renew/check the leader lease and fencing token.
    -   Only the authorized leader performs scheduling decisions.
    -   Publish jobs to the queue and record scheduling events.
4.  **Redis**
    -   Job queue and, if implemented, processing/acknowledgement
        structures.
    -   Worker heartbeats and presence.
    -   Election messages/state, leader lease, fencing counter, lock
        coordination, and short-lived cluster coordination data.
    -   Use separate, clearly named key prefixes for each
        responsibility.
5.  **Worker processes**
    -   Independently claim jobs.
    -   Validate job type and payload.
    -   Execute a registered handler.
    -   Report started/completed/failed status and heartbeat.
    -   Acknowledge successful work or allow it to be recovered after
        timeout.
6.  **Metadata database**
    -   Jobs, attempts, results, scheduler/worker observations, event
        history, and snapshots.
    -   Start with SQLite if appropriate for the existing code; move to
        PostgreSQL if multi-process write concurrency requires it.
7.  **Observability layer**
    -   Structured logs with component ID, event type, job ID, scheduler
        ID, worker ID, and logical timestamp.
    -   Health endpoints and metrics.
    -   Dashboard views for system behavior and failure demonstrations.

### 3.2 High-level flow

1.  Client submits a job to the API.
2.  API validates the request, creates a `QUEUED` job record, and
    records an event.
3.  Authorized scheduler leader selects/publishes the job.
4.  A worker claims the job and records an attempt.
5.  Worker executes the handler and stores the result or error.
6.  Worker acknowledges completion; job state becomes `COMPLETED` or
    `FAILED`.
7.  If a worker disappears while a job is in progress, a recovery
    process identifies the stale attempt and requeues it or marks it
    dead-lettered after the retry limit.
8.  Every important distributed event receives a Lamport timestamp.
9.  Scheduler election, lock operations, and snapshots are visible in
    the dashboard.

### 3.3 Suggested service/container layout

``` text
client/dashboard
       |
       v
api-service -------------------- metadata-db
       |                              ^
       v                              |
redis <---- scheduler-1 / scheduler-2 / scheduler-3
  ^             | leader election, lease, fencing
  |
  +------ worker-1
  +------ worker-2
  +------ worker-3
```

The API may run as a separate service or be hosted by one application
process. Keep the process boundaries explicit in the demo: multiple
scheduler replicas and multiple workers must be separate
processes/containers, not merely Python objects in one process.

------------------------------------------------------------------------

## 4. Technology and Repository Organization

### 4.1 Recommended technology choices

-   **Language:** Python.
-   **API/dashboard backend:** retain the current framework where
    practical.
-   **Client/service RPC:** gRPC only if the selected course mapping
    requires a genuine RPC demonstration.
-   **Queue and coordination:** Redis.
-   **Metadata:** current SQLite implementation initially; PostgreSQL if
    multi-process concurrency requires it.
-   **Dashboard:** retain the existing frontend and extend it.
-   **Containerization:** Docker and Docker Compose.
-   **Testing:** `pytest`, plus integration tests and repeatable
    fault-injection scripts.
-   **Metrics:** lightweight application metrics and health endpoints;
    Prometheus/Grafana are optional extensions, not prerequisites for
    the distributed-algorithm requirements.

### 4.2 Suggested folder structure

Adapt this structure to the current repository instead of renaming
everything blindly.

``` text
distributed-job-platform/
├── app/
│   ├── api/
│   ├── scheduler/
│   │   ├── election.py
│   │   ├── leader_lease.py
│   │   └── dispatcher.py
│   ├── worker/
│   │   ├── runner.py
│   │   ├── heartbeat.py
│   │   └── handlers/
│   │       ├── csv_handler.py
│   │       ├── thumbnail_handler.py
│   │       └── notification_demo.py
│   ├── coordination/
│   │   ├── lamport_clock.py
│   │   ├── mutual_exclusion.py
│   │   └── snapshot.py
│   ├── queue/
│   ├── storage/
│   ├── observability/
│   └── config.py
├── dashboard/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── failure/
│   └── acceptance/
├── scripts/
│   ├── demo_submit_jobs.py
│   ├── kill_worker_demo.py
│   ├── kill_leader_demo.py
│   └── verify_cluster.py
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PROTOCOLS.md
│   ├── API.md
│   ├── TESTING.md
│   └── DEMO_GUIDE.md
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── requirements.txt
├── PLAN.md
└── README.md
```

The filenames are a target organization, not a requirement to move
working files without benefit.

------------------------------------------------------------------------

## 5. Data Model and State Machines

### 5.1 Job entity

Each job should have at least:

-   `job_id`: unique identifier.
-   `job_type`: `csv_process`, `thumbnail`, or `notification_demo`.
-   `payload`: validated JSON or a reference to an input file.
-   `status`: `QUEUED`, `RUNNING`, `COMPLETED`, `RETRY_WAIT`, or
    `DEAD_LETTER`.
-   `created_at`, `started_at`, `completed_at`: physical timestamps for
    display/auditing.
-   `attempt_count`, `max_attempts`.
-   `assigned_worker_id`, `last_error`.
-   `result`: JSON or a reference to an output artifact.
-   `idempotency_key`: optional client-provided key to avoid duplicate
    submissions.
-   `logical_timestamp`: latest associated Lamport clock value where
    useful.

### 5.2 Additional records

-   **JobAttempt:** attempt ID, job ID, worker ID, start/end times,
    status, error, and result reference.
-   **WorkerStatus:** worker ID, last heartbeat, current job, state, and
    capabilities.
-   **SchedulerStatus:** scheduler ID, last heartbeat, election state,
    current leader observation, and fencing token.
-   **EventLog:** event ID, component ID, event type, job ID if
    relevant, logical timestamp, physical timestamp, and structured
    details.
-   **LockRequest:** requester ID, request timestamp, resource name, and
    lock state, if durable lock history is needed.
-   **ClusterSnapshot:** snapshot ID, capture time, leader/fencing
    token, queue counts, job counts, worker/scheduler states, active
    lock information, and consistency mode.

### 5.3 State transitions

``` text
QUEUED -> RUNNING -> COMPLETED
                   -> RETRY_WAIT -> QUEUED
                   -> DEAD_LETTER

RUNNING -> RETRY_WAIT when a stale attempt is recovered
```

Validate transitions in one service/module. Do not allow arbitrary
endpoints to change job state without checking the current state and
attempt identity.

------------------------------------------------------------------------

## 6. Implementation Roadmap

Complete each phase and its exit criteria before moving to the next.
Commit each stable phase separately.

### Phase 0 --- Repository audit and baseline

**Purpose:** establish what already works and prevent regressions.

Tasks: - Inspect all source, configuration, and documentation. - Run the
existing project from a clean setup. - Draw the current architecture and
actual message paths. - Identify incomplete features, dead code, TODOs,
and unsupported claims. - Decide the active API/RPC path. - Add
`.env.example`, input validation, consistent configuration, and safe
handling of secrets. - Record baseline commands and known limitations.

Deliverables: - `docs/ARCHITECTURE.md` updated to current behavior. - A
baseline test report. - A clean, reproducible setup section in README.

**Exit criteria:** a new team member can start the baseline and complete
one end-to-end job.

### Phase 1 --- Stabilize FA-1 core and API contracts

Tasks: - Define job submission and status schemas. - Validate job types
and payloads. - Standardize job states and error handling. - Ensure job
metadata is created before a job is reported as accepted. - Add stable
endpoints such as: - `POST /jobs` - `GET /jobs` - `GET /jobs/{job_id}` -
`GET /jobs/{job_id}/result` - `GET /health` - `GET /workers` -
`GET /cluster` - Add request IDs and structured logs. - Add a small
automated happy-path integration test.

Deliverables: - Documented API contract. - Working job submission and
status lookup. - Clear error responses for invalid job types/payloads.

**Exit criteria:** valid jobs are accepted and tracked; invalid requests
fail safely.

### Phase 2 --- Real worker handlers and result storage

Implement at least two real handlers and one explicitly simulated
handler.

1.  **CSV processing**
    -   Accept a local mounted-file path or safe upload/reference
        supported by the current app.
    -   Validate file existence and size.
    -   Calculate useful statistics such as row count, column count,
        missing values, and summary values.
    -   Save a structured JSON result.
2.  **Image thumbnail**
    -   Read an input image from a mounted input directory.
    -   Generate a resized thumbnail with a standard Python image
        library.
    -   Save the artifact in a mounted output directory and return its
        path/metadata.
    -   Reject paths outside allowed input/output directories.
3.  **Notification demonstration**
    -   Simulate sending a notification and record the intended
        recipient/message.
    -   Do not claim a real email was sent unless a real mail provider
        is configured.

Tasks: - Add handler registration by job type. - Record start, finish,
duration, output, and errors. - Ensure exceptions do not silently kill a
worker. - Ensure a worker can continue to the next job after a handler
failure.

**Exit criteria:** each handler can be submitted through the API and
produces a verifiable result or useful error.

### Phase 3 --- Queue safety, acknowledgements, retries, and recovery

This phase is essential to make the project defensible as a
fault-tolerant job platform.

Tasks: - Replace or wrap the simple pop-and-forget flow with a reliable
claim/ack pattern. - Maintain separate queued and in-progress/processing
records, using Redis Streams consumer groups or a carefully designed
processing list and acknowledgement structure. - Record a job attempt
before or during execution. - Acknowledge a job only after its final
status/result is persisted. - Detect jobs whose worker/claim has been
stale longer than the configured visibility timeout. - Recover stale
attempts and requeue jobs within the retry limit. - Apply exponential or
bounded backoff if feasible. - Move exhausted jobs to `DEAD_LETTER` with
a reason. - Make recovery safe against two recovery loops trying to
requeue the same job. - Add idempotency keys for submission and make
handlers idempotent where practical. - Add a worker shutdown signal so
it stops accepting new jobs and completes or releases its current claim
where possible.

Tests: - Kill a worker while idle. - Kill a worker during a job. - Force
a handler exception. - Restart Redis or the worker where supported by
the chosen setup. - Confirm stale work is recovered or explicitly
dead-lettered.

**Important:** document delivery semantics as **at-least-once with
idempotency/recovery safeguards**, not exactly-once.

**Exit criteria:** a worker failure does not silently leave a job
permanently `RUNNING`; the system either recovers it or records a
visible terminal failure.

### Phase 4 --- Lamport logical clocks and event ordering (Unit III)

Tasks: - Implement a reusable `LamportClock` per participating
process. - Increment the local clock before each relevant local event. -
Attach the current timestamp to messages sent between components. - On
receipt of a message timestamp `T`, update the local clock as
`L = max(L, T) + 1`. - Include the logical timestamp in scheduler,
worker, election, and lock event records. - Record event type, component
ID, related job ID, and message/correlation ID. - Display a filterable
event history in the dashboard. - Add unit tests for local increment,
send, receive, and ordering behavior.

Required demonstrations: - Show events from multiple workers and
schedulers. - Show that a receive event advances the receiver's clock
correctly. - Explain that Lamport timestamps preserve the
happened-before ordering when events are causally related, but equal or
ordered timestamps do not prove causality in every case. - Keep physical
timestamps separately for human-readable display.

**Exit criteria:** all important protocol events have logical timestamps
and clock rules are tested.

### Phase 5 --- Bully leader election and scheduler replicas (Unit IV)

Tasks: - Run at least **three scheduler instances** with unique numeric
IDs. - Define the scheduler states: `FOLLOWER`, `CANDIDATE`, and
`LEADER` (or equivalent documented states). - Add scheduler heartbeats
and a configurable timeout. - When a scheduler suspects the leader has
failed, start the Bully election: 1. Send `ELECTION` messages to
schedulers with higher IDs. 2. If no higher-ID scheduler responds,
announce itself as coordinator. 3. If a higher-ID scheduler responds,
wait for that scheduler to continue the election. 4. The highest
reachable live scheduler wins under the algorithm's assumptions and
sends a coordinator announcement. - Record election messages and results
with Lamport timestamps. - Show the current leader and election history
in the dashboard. - Ensure schedulers do not all publish jobs
concurrently merely because they locally believe they are leader.

**Leader authority and split-brain protection:** - Use a Redis-backed
lease with an expiry and periodic renewal. - Use a monotonically
increasing fencing token when leadership is granted/renewed. - Require
scheduler writes/scheduling actions to validate current authority/token
where possible. - Stop scheduling immediately if lease renewal fails. -
Do not treat a Bully election message alone as sufficient authority. -
Document that lease correctness depends on timing, Redis availability,
and implementation details; this educational arrangement is not a
consensus protocol.

**Tests:** - Start all three schedulers and confirm the expected
leader. - Stop the leader and confirm another scheduler is elected. -
Restart the old leader and confirm it does not immediately overwrite the
current leader. - Delay or drop heartbeat messages in a controlled test
if feasible. - Verify that a job is not intentionally dispatched by two
schedulers at once.

**Exit criteria:** a leader failure triggers a visible election and the
remaining system continues scheduling after a new leader obtains valid
authority.

### Phase 6 --- Distributed mutual exclusion (Unit III/IV synchronization topic)

**Recommended implementation:** Lamport's distributed mutual-exclusion
algorithm, because it demonstrates the protocol directly. A Redis lock
may be added as a simpler comparison, but it should not replace the
syllabus algorithm if Lamport mutual exclusion is expected.

Tasks: - Define a named shared critical section/resource, such as
updating a shared report/index or a cluster-wide maintenance record. -
Implement `REQUEST`, `REPLY`, and `RELEASE` messages. - Order requests
by `(Lamport timestamp, process ID)`. - A process enters the critical
section only when: 1. its request is at the head of its local request
queue; and 2. it has received replies from every other participating
process. - On release, broadcast `RELEASE` and remove the request from
the local queue. - Add timeouts, membership assumptions, and visible
error handling for unavailable participants. - Log request, reply,
enter, exit, and release events. - Display the active holder/request
queue and event history. - Keep the protected operation short and
deterministic for the demo.

Tests: - Trigger simultaneous requests from at least two participants. -
Assert that no two participants enter the protected critical section at
the same time. - Test release and subsequent acquisition. - Demonstrate
what happens when a participant becomes unreachable; explain that basic
Lamport mutual exclusion can block when required replies are missing.

**Exit criteria:** automated tests verify mutual exclusion and the
dashboard shows the request/reply/release sequence.

### Phase 7 --- Cluster-state snapshot

Implement a snapshot feature that captures the state needed to explain
the cluster at a particular observation point.

Minimum snapshot content: - Snapshot ID and capture time. - Current
leader and fencing token. - Scheduler states and last heartbeats. -
Worker states and last heartbeats. - Job counts by state. -
Queue/processing/dead-letter counts. - Active job attempts. - Active
mutual-exclusion requests/holder, if any. - Latest logical timestamp and
relevant event-log offset. - Snapshot mode and any known limitations.

Tasks: - Add an API endpoint and dashboard action to capture a
snapshot. - Store snapshots so they remain visible after a process
restart. - Show a snapshot detail page and allow comparison between two
snapshots. - Label a simple API-collected snapshot as
**best-effort/observational**. - If the faculty requires a formal
consistent global-state algorithm, add a separate Chandy--Lamport-style
snapshot protocol with marker messages and channel-state capture. Do not
call a collection of unrelated status queries a Chandy--Lamport
snapshot.

Tests: - Capture a snapshot while jobs are queued and running. - Capture
another after a worker failure/recovery or leader election. - Confirm
snapshot contents can be read after restarting the dashboard/API.

**Exit criteria:** snapshots are persisted, inspectable, and their
consistency guarantees are accurately labelled.

### Phase 8 --- Communication and Unit I/II coverage

Make sure the core mini-project clearly demonstrates the foundational
concepts, not only the FA-2 upgrades.

-   **Communication models:** document and demonstrate the actual
    client-to-API communication and scheduler-to-worker queue messaging.
-   **RPC:** implement and demonstrate genuine RPC if required by the
    course rubric; otherwise document HTTP/REST and asynchronous queue
    communication accurately.
-   **Message-oriented communication:** demonstrate queued jobs and
    worker consumption, including the difference between synchronous API
    response and asynchronous execution.
-   **Stream-oriented communication:** explain whether the
    implementation has a real stream-based path. If required, implement
    a small streaming endpoint or use Redis Streams; do not claim
    streaming solely because logs update on a page.
-   **Naming and addressing:** define stable job IDs, worker IDs,
    scheduler IDs, resource names, and how components locate Redis/API
    services through configuration or Compose service names.
-   **Fault tolerance:** demonstrate worker failure detection, job
    recovery, scheduler election, and the limits of each mechanism.
-   **Middleware:** explain the API framework, Redis client, queue
    abstraction, and database layer as middleware/software layers
    between application logic and infrastructure.
-   **Architecture and design trade-offs:** document client/server,
    coordinator-worker, queue-based asynchronous processing, and the
    implications of central metadata/Redis dependencies.
-   **P2P communication:** the scheduler election protocol has
    peer-to-peer scheduler messages. Clearly identify which messages are
    direct peer messages and which are mediated by Redis.
-   **WebRTC and multimedia concepts:** these are not naturally central
    to this project. If they are explicit examinable syllabus items,
    provide a concise design note or a small isolated proof-of-concept
    rather than falsely claiming the job platform implements WebRTC.
    Thumbnail generation is a multimedia workload example, not a WebRTC
    implementation.
-   **Model of computation and virtualization:** document processes,
    containers, message passing, and failure assumptions. Docker Compose
    demonstrates containerization; it does not by itself prove full
    virtual-machine virtualization.
-   **Physical clock synchronization:** only add NTP/clock-offset
    measurement if the faculty explicitly expects it as an implemented
    feature. Lamport logical clocks are a different concept.

Create a syllabus mapping table in the final README with columns:
`Unit/topic`, `project component`, `evidence/demo`, and
`status (implemented/optional/theory only)`.

### Phase 9 --- Dashboard and observability

Extend the existing dashboard instead of building an unrelated UI.

Required views: 1. **Overview:** job counts, queue depth, processing
count, failed/dead-letter count, scheduler leader, and worker count. 2.
**Jobs:** list, filters, state, attempts, duration, result, and error.
3. **Job details:** payload summary, event timeline, attempts, worker
assignment, and result. 4. **Workers:** online/offline status, last
heartbeat, current job, completed/failed count. 5. **Schedulers:** IDs,
state, leader lease, fencing token, heartbeat, and election history. 6.
**Distributed events:** Lamport timestamp, physical time, component,
event type, job/correlation ID. 7. **Mutual exclusion:** resource,
queued requests, active holder, and protocol events. 8. **Snapshots:**
capture, list, inspect, and compare. 9. **Health/metrics:** API health,
Redis/database availability, throughput, job duration, retries, and
recovery count.

Operational requirements: - Show offline/stale states clearly. - Avoid
exposing secrets or full sensitive payloads. - Make demo controls
clearly labelled and restrict destructive controls to the local/demo
environment. - If Prometheus/Grafana are added, use them for metrics
visualization; they do not replace application-level protocol history.

### Phase 10 --- Docker Compose and reproducible deployment

Tasks: - Create a Dockerfile for the application. - Create a
`docker-compose.yml` with Redis, metadata DB (if PostgreSQL is used),
API/dashboard, at least three schedulers, and at least two workers. -
Use named volumes for data and output artifacts. - Use environment
variables and `.env.example`; never commit secrets. - Add health checks
and service dependencies where appropriate. - Ensure each scheduler has
a distinct configured ID. - Ensure each worker has a distinct configured
ID. - Use Compose service names for internal addressing. - Add resource
limits if needed for a laptop demo. - Document clean start, status,
logs, restart, and shutdown commands. - Verify that state expected to
persist survives container recreation.

**Exit criteria:** the entire project can be launched from documented
Compose commands and the FA-2 demo does not depend on manually starting
hidden background processes.

### Phase 11 --- Testing and evaluation

#### A. Unit tests

-   Lamport clock increment/send/receive rules.
-   Job state transitions and payload validation.
-   Retry/backoff and dead-letter decisions.
-   Leader election state transitions.
-   Lease/fencing checks.
-   Mutual-exclusion queue ordering and eligibility.
-   Snapshot serialization and schema.

#### B. Integration tests

-   API submission → queue → worker → stored result.
-   Multiple workers process independent jobs.
-   Scheduler leader dispatches jobs.
-   Worker heartbeat and offline detection.
-   Event history includes logical timestamps.
-   Snapshot endpoint persists a snapshot.
-   Dashboard/API can query jobs and cluster state.

#### C. Fault and concurrency tests

-   Kill worker before claim, during execution, and after a side effect
    where practical.
-   Force job handler failure.
-   Stop the scheduler leader.
-   Restart the previous leader.
-   Simulate simultaneous lock requests.
-   Run several jobs in parallel.
-   Test duplicate submissions with the same idempotency key.
-   Test retry exhaustion and dead-letter visibility.
-   Test Redis/database unavailability and document graceful
    degradation.

#### D. Measurements

Record actual measured values; do not invent results. - Total jobs
submitted and completed. - Successful/failed/retried jobs. - Throughput:
completed jobs per second over a defined interval. - Average and
percentile job duration if enough observations exist. - Time to detect a
failed worker. - Time to elect a replacement scheduler. - Recovery
success rate for the chosen failure scenarios. - Mutual-exclusion
overlap violations (target: zero in tests). - Resource use if
measurable.

Use a repeatable test script and report environment, number of jobs,
worker count, duration, and limitations. Compare one worker versus
multiple workers only if the setup can produce a fair measurement.

**Exit criteria:** the README includes reproducible test commands and a
results table populated with real observations.

### Phase 12 --- Documentation, presentation, and viva preparation

Required documents: - `README.md`: problem, objectives, features,
architecture, setup, usage, tests, limitations, and syllabus mapping. -
`docs/ARCHITECTURE.md`: component and deployment diagrams, state
transitions, data flow, and dependencies. - `docs/PROTOCOLS.md`: Lamport
clock, Bully election, leader lease/fencing, mutual exclusion,
retry/recovery, and snapshot protocol. - `docs/API.md`:
endpoint/request/response examples. - `docs/TESTING.md`: test strategy,
commands, environment, observed results, and known gaps. -
`docs/DEMO_GUIDE.md`: exact demo sequence, expected output, and recovery
instructions. - `PLAN.md`: this implementation plan, updated as phases
are completed.

Presentation structure: 1. Problem statement and motivation. 2. FA-1
baseline and its limitations. 3. Updated architecture and components. 4.
Job lifecycle and message flow. 5. Unit III concepts implemented. 6.
Unit IV concepts implemented. 7. Failure handling and recovery. 8.
Dashboard and live demonstration. 9. Tests and measured results. 10.
Limitations, trade-offs, and future work.

For viva, each team member should be able to explain the code they
present, why each protocol is needed, message/state transitions, failure
cases, and why the project does not claim exactly-once execution or
production consensus.

------------------------------------------------------------------------

## 7. Recommended Live Demonstration Script

Prepare this sequence and rehearse it from a clean Compose start.

1.  Start all services and show API, Redis, scheduler, and worker
    health.
2.  Show the scheduler IDs and current elected leader.
3.  Submit a batch of 15--20 jobs, including CSV, thumbnail, and
    notification-demo jobs.
4.  Show workers claiming jobs and the dashboard updating job states.
5.  Open a job detail and show its result, attempts, and logical-clock
    event timeline.
6.  Submit two concurrent shared-resource requests and show the
    mutual-exclusion protocol; prove that critical-section execution
    does not overlap.
7.  Stop one worker during a long-running job. Show heartbeat expiry,
    stale-attempt recovery, retry, completion or dead-lettering.
8.  Stop the current scheduler leader. Show Bully election messages, the
    replacement leader, a new fencing token, and scheduling resumption.
9.  Restart the old scheduler and show that it does not regain authority
    without a valid lease.
10. Capture a cluster snapshot before and after failover; compare the
    visible state.
11. Show test output and actual performance/failover measurements.
12. Finish with limitations and explain which syllabus topics are
    implemented versus only discussed.

Keep a fallback demo script and sample input files available, but do not
fake failure-test results. If a live failure scenario is unstable, show
a clearly labelled recorded test log or automated test output.

------------------------------------------------------------------------

## 8. Acceptance Criteria for the Final Mini-Project

The project is considered ready only when the following high-priority
requirements are met.

### Core platform

-   [ ] API accepts and validates jobs.
-   [ ] Multiple worker processes execute jobs independently.
-   [ ] At least two real handlers produce verifiable output.
-   [ ] Job states, attempts, results, and errors are persisted.
-   [ ] Dashboard shows jobs and worker health.

### Reliability

-   [ ] In-progress jobs can be recovered after a worker failure or are
    explicitly dead-lettered.
-   [ ] Retry limits and failure reasons are visible.
-   [ ] Duplicate execution risk and idempotency behavior are
    documented.
-   [ ] Acknowledgement occurs only after successful result/status
    persistence.

### Unit III/IV upgrades

-   [ ] Lamport logical clock rules have unit tests and visible event
    history.
-   [ ] At least three scheduler replicas participate in Bully election.
-   [ ] Leader lease/fencing prevents an unvalidated stale scheduler
    from dispatching jobs.
-   [ ] Distributed mutual exclusion is demonstrated with simultaneous
    requests.
-   [ ] Cluster snapshots are persisted and their consistency mode is
    labelled.
-   [ ] If formal global-state snapshot is a rubric requirement, the
    Chandy--Lamport extension is implemented and tested.

### Deployment and quality

-   [ ] Docker Compose launches the complete demo.
-   [ ] Unit, integration, concurrency, and failure tests pass or
    documented known failures remain.
-   [ ] Architecture, API, protocols, setup, testing, and demo docs
    match the code.
-   [ ] Actual measured results are reported without fabricated numbers.
-   [ ] Every syllabus topic is mapped to an implemented feature, an
    optional extension, or a theory-only explanation.

------------------------------------------------------------------------

## 9. Priority and Time Management

If time is limited, follow this order. Do not begin with optional
infrastructure or a large UI redesign.

### Must complete

1.  Audit and stabilize the current repo.
2.  Reliable job lifecycle and worker recovery.
3.  Lamport clocks and timestamped event history.
4.  Three scheduler replicas, Bully election, lease, and fencing.
5.  Lamport mutual exclusion with a concurrency test.
6.  Docker Compose and a clear dashboard.
7.  Automated tests, architecture documentation, and repeatable demo.

### Complete if required by rubric / time permits

8.  Persisted cluster-state snapshots.
9.  Chandy--Lamport consistent snapshot protocol if formal global
    snapshots are explicitly required.
10. Genuine gRPC path if RPC is explicitly required.
11. Redis Streams if it improves the queue/recovery design.
12. Prometheus/Grafana metrics.
13. Physical clock-offset measurement/NTP explanation if required.

### Do not prioritize early

-   Kubernetes deployment before the Compose demo works.
-   Microservices refactoring without a course or reliability need.
-   Decorative dashboard redesign.
-   Claims of "exactly once," "zero downtime," "consensus," or
    "production ready" that the implementation cannot prove.

------------------------------------------------------------------------

## 10. Final Syllabus Coverage Matrix

Use the official syllabus and faculty rubric to verify exact wording.
The following is a planning matrix; topics marked optional/theory-only
are **not** claims that they already exist in the repository.

  -----------------------------------------------------------------------
  Syllabus concept        Planned project mapping Evidence
  ----------------------- ----------------------- -----------------------
  Distributed-system      Background-job use      Architecture diagram
  goals, types,           case;                   and introduction
  architectures           coordinator-worker and  
                          queue-based             
                          architecture            

  Design issues and       API, queue abstraction, Component diagram and
  middleware              Redis client, metadata  design trade-offs
                          storage, failure        
                          assumptions             

  Communication           API requests, queue     Message-flow diagram
  fundamentals            messages, scheduler     and logs
                          peer messages           

  RPC                     Genuine gRPC path if    API/RPC demo and
                          required; otherwise     protocol docs
                          explicitly documented   
                          HTTP/REST path          

  Message-oriented        Redis queue/stream and  Submit job, return
  communication           asynchronous job        request, worker
                          execution               completes later

  Stream-oriented         Redis Streams or an     Consumer/stream
  communication           explicit streaming      demonstration
                          endpoint if required    

  P2P messaging           Scheduler election      Election logs
                          messages between peers, 
                          with transport clearly  
                          identified              

  Naming, IDs, and        Job IDs, worker IDs,    Configuration and logs
  addresses               scheduler IDs, resource 
                          names, Compose service  
                          discovery               

  Fault tolerance         Worker heartbeats,      Worker/leader failure
                          retries, stale-job      demo
                          recovery, scheduler     
                          election                

  Lamport logical clocks  Per-process clocks and  Unit tests and event
                          timestamped events      timeline

  Distributed mutual      Lamport                 Simultaneous request
  exclusion               REQUEST/REPLY/RELEASE   test
                          algorithm               

  Leader election         Bully algorithm with    Kill-leader demo
                          lease/fencing authority 
                          check                   

  Global state/snapshot   Persisted best-effort   Snapshot comparison
                          snapshot; optional      
                          Chandy--Lamport formal  
                          snapshot                

  Physical clock          Separate optional       Only if explicitly
  synchronization         measurement/theory      implemented
                          item; not replaced by   
                          logical clocks          

  Multimedia processing   Image thumbnail job     Input/output artifact

  WebRTC                  Not naturally part of   Clearly labelled
                          the core platform;      separate demo or theory
                          separate optional       note
                          proof-of-concept if     
                          required                

  Computation             Independent processes   Deployment diagram and
  model/virtualization    and Docker containers   Compose demo
  -----------------------------------------------------------------------

### 10.1 Unit IV — Emerging Paradigms Coverage Matrix

  -----------------------------------------------------------------------------------------------------------------------------
  Syllabus Concept / Case Study        Implementation Module & Demo                     Evidence & Verification
  ------------------------------------ ------------------------------------------------ ---------------------------------------
  Distributed Object-Based Systems     common/distributed_object.py                     tests/test_unit4_emerging_paradigms.py
                                       tools/remote_object_demo.py                      Client proxy, marshalling, timeout

  Web-Based Distributed Systems        scheduler/main.py                                tests/test_scheduler_worker.py
                                       tools/web_architecture_demo.py                   FastAPI REST, HTTP 202, Redis broker

  Distributed File Systems             common/distributed_filesystem.py                 tests/test_unit4_emerging_paradigms.py
                                       tools/dfs_simulation_demo.py                     NameNode metadata, DataNodes, R=2 failover

  Serverless Computing                 common/serverless_runtime.py                     tests/test_unit4_emerging_paradigms.py
                                       tools/serverless_function_demo.py                FaaS container pool, cold start, idemp

  Case Study: Megaport                 docs/CASE_STUDIES.md                             Section 1: SDN NaaS, BGP, VXC failover

  Case Study: Cloudflare               docs/CASE_STUDIES.md                             Section 2: Anycast CDN, V8 Workers, K/V

  Case Study: AWS                      docs/CASE_STUDIES.md                             Section 3: Multi-AZ, DynamoDB Paxos, S3

  Case Study: Hadoop                   docs/CASE_STUDIES.md                             Section 4: MapReduce locality, YARN

  Case Study: Kubernetes               docs/CASE_STUDIES.md                             Section 5: Declarative state, etcd Raft

  Case Study: Apache HDFS              docs/CASE_STUDIES.md                             Section 6: NameNode, DataNodes, 3x rack
  -----------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 11. Definition of Done

The final deliverable is not just a list of algorithms. It is one
runnable application in which:

-   a client submits real jobs;
-   independent workers process them;
-   results and failures are persisted;
-   worker failure triggers visible recovery;
-   scheduler failure triggers a visible election and controlled leader
    change;
-   logical timestamps make event ordering inspectable;
-   a shared critical section is protected by a tested distributed
    mutual-exclusion protocol;
-   snapshots capture and preserve cluster observations;
-   Docker Compose starts the environment reproducibly;
-   tests demonstrate the intended behavior; and
-   the documentation distinguishes implemented behavior, assumptions,
    optional features, and known limitations.

**Implementation rule:** upgrade the existing repository in small,
tested phases. At the end of each phase, update the README and commit
the working state before proceeding.
