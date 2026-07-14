# Job Distribution Platform

This repository is the starting point for a generic background job platform in Go, inspired by tools like Sidekiq, Celery, and Asynq.

The goal is to build a reusable system for submitting, dispatching, retrying, scheduling, and observing arbitrary jobs, not a one-off worker for a single use case.

## Vision

The platform should support multiple job types such as:

- `email`
- `image_resize`
- `webhook`
- `deployment`

New job types should be added by registering handlers, without changing the core worker flow.

## Core Design Goals

- Generic job execution through a dispatcher and handler interface
- Retry support with retry states and dead-letter handling
- Priority-based job processing
- Scheduling for delayed or future execution
- Concurrency controls per job type
- Distributed lock protection against duplicate execution
- Idempotency to prevent double-processing after crashes
- Structured logging and observability with `slog`

---

## Architecture

The project supports two implementations of the same workflow.

The default mode stays simple and runs inside one Go process:

```text
Service -> priority channels -> workers -> dispatcher -> handler
```

The high, medium, and low Go channels act as the current in-memory message queues. Workers wait for job IDs and react when work arrives. This is useful for learning and local development, but the channels cannot deliver jobs to workers running in another application instance and their contents disappear when the process stops.

The distributed mode keeps the same job model and execution flow while replacing storage and transport:

```text
Service/Scheduler -> Job Repository -> Job Queue -> Worker
                         |                 |           |
                         |                 |           -> Dispatcher -> Handler
                         |                 |
                         |                 -> in-memory channels or RabbitMQ
                         |
                         -> in-memory storage or PostgreSQL

Worker -> job events -> Redis Pub/Sub -> dashboards and notifications
```

Responsibilities:

- The existing Go channels remain the default in-memory queue for development and tests.
- The current repository remains available for local development, while PostgreSQL provides job data shared by separate processes.
- RabbitMQ provides the durable job transport for separate workers, with publisher confirms and manual acknowledgements.
- Redis Pub/Sub broadcasts live events such as `job.started`, `job.completed`, and `job.failed`.
- Redis Pub/Sub will not be the main job queue because messages can be lost while a worker is disconnected.
- Redis can later also support distributed locks and idempotency keys, which are separate from Pub/Sub.

This direction does not require a full rewrite. The service, scheduler, dispatcher, handlers, job statuses, and concurrency limits remain useful. Small repository and queue interfaces will isolate the in-memory and production implementations from the rest of the project.

RabbitMQ messages contain a job ID because every producer and worker can load the complete job from PostgreSQL.

The project stores retry times with each job. Retry delays grow exponentially with a small amount of jitter, and the scheduler recovers due retries after worker restarts. gRPC is optional and is not part of the core worker architecture; it should only be added if separate internal services need a strongly typed job-submission API.

---

## Current Status

The repository now has both a working in-memory mode and the first distributed mode:

- Phase 0 is complete
- Phase 1 is complete for in-memory job submission
- Phase 2 is complete for handler registration and sample handlers
- Phase 3 is complete for a basic single-worker execution loop
- Phase 4 is complete for simple retries and dead-letter handling
- Phase 5 is complete for simple priority queues
- Phase 6 is complete for simple scheduled job enqueueing
- Phase 7 is complete for a basic worker pool and per-job-type concurrency limits
- Phase 8 is complete for the in-memory queue abstraction
- Phase 9 is complete for PostgreSQL persistence
- Phase 10 is complete for RabbitMQ job transport
- Phase 11 is complete for durable retry and exponential backoff

The core project is complete. The distributed demo runs the producer and worker as separate containers while sharing PostgreSQL and RabbitMQ.

---

## Running The Project

### Simple In-Memory Demo

No external services are required:

```powershell
go run .
```

This uses `APP_MODE=demo`, submits the sample jobs, runs three workers for three seconds, and prints their final states.

### Distributed Demo

Start PostgreSQL, RabbitMQ, and the long-running worker:

```powershell
docker compose up -d --build postgres rabbitmq worker
```

Run a separate producer that submits the sample jobs and exits:

```powershell
docker compose --profile tools run --rm producer
```

Follow the worker processing jobs received through RabbitMQ:

```powershell
docker compose logs -f worker
```

Inspect persisted job states directly in PostgreSQL:

```powershell
docker compose exec postgres psql -U jobs -d jobs -c "SELECT id, type, priority, status, attempts FROM jobs ORDER BY id;"
```

RabbitMQ Management is available at `http://localhost:15673` with username `jobs` and password `jobs`.

Stop the local services when finished:

```powershell
docker compose down
```

Runtime modes:

- `APP_MODE=demo` submits and processes jobs in one process, then prints their states.
- `APP_MODE=producer` submits sample jobs and exits.
- `APP_MODE=worker` runs the worker pool and scheduler until the process is stopped.
- Leave `DATABASE_URL` and `RABBITMQ_URL` empty for in-memory mode, or set both to enable distributed mode.

---

## Recommended MVP

The first version should prove the core workflow end-to-end:

1. Define the domain model for jobs
2. Allow a job to be submitted in memory
3. Dispatch the job to a registered handler
4. Execute the job with a simple worker flow
5. Update job status based on success or failure

If this flow is clean, retries, scheduling, persistence, and observability can be layered on without rewriting the foundation.

---

## Phase-wise Implementation Plan

### Phase 0 - Foundation

Status: complete.

Goal: set up the module, project layout, and core job model.

Tasks:

- Create `go.mod`
- Define core types:
  - `Job`
  - `JobType`
  - `JobStatus`
  - `JobPriority`
- Add a small entry point for exercising the flow

Deliverable:

- A minimal app that can create and inspect jobs

### Phase 1 - Basic Job Submission

Status: complete for the in-memory version.

Goal: accept jobs from inside the application.

Tasks:

- Add a `SubmitJob` flow
- Support job metadata:
  - type
  - payload
  - priority
  - scheduled time
  - max retries
- Store jobs in an in-memory repository

Deliverable:

- A working submission pipeline for multiple job types

### Phase 2 - Dispatcher and Handler Registration

Status: complete for the in-memory version.

Goal: remove hardcoded execution logic.

Tasks:

- Define a `JobHandler` interface:
  - `Execute(ctx context.Context, job Job) error`
- Build a dispatcher that routes by `JobType`
- Add a registration API such as `RegisterHandler(jobType, handler)`
- Add sample handlers for `email` and `deployment`
- Add a sample handler for `webhook`

Deliverable:

- A generic dispatch layer that is easy to extend

### Phase 3 - Worker Execution

Status: complete for the basic single-worker version.

Goal: process jobs asynchronously.

Tasks:

- Create a basic worker or worker pool
- Pull jobs from a queue
- Mark jobs as `pending`, `running`, `completed`, or `failed`
- Execute the registered handler
- Persist status changes

Deliverable:

- A functional worker flow that processes jobs from a queue

### Phase 4 - Retries and Dead Lettering

Status: complete for the basic in-memory version.

Goal: make failed jobs safer to process.

Tasks:

- Increment `Attempts` each time a worker runs a job
- Retry failed jobs until `MaxRetries` is reached
- Move exhausted jobs to `dead_letter`
- Keep the retry loop easy to follow in `worker.go`

Deliverable:

- A worker that can retry temporary failures and separate permanently failed jobs

### Phase 5 - Priority Queues

Status: complete for the basic in-memory version.

Goal: make important jobs run before less important jobs.

Tasks:

- Add high, medium, and low priority queues
- Route submitted jobs into the correct queue
- Make the worker check high priority work first, then medium, then low
- Keep retries on the same priority level as the original job

Deliverable:

- A worker that prefers higher priority jobs while keeping the code easy to trace

### Phase 6 - Scheduling

Status: complete for the basic in-memory version.

Goal: support jobs that should run in the future.

Tasks:

- Store future `ScheduledAt` times on jobs
- Add a scheduler loop that checks for due jobs
- Enqueue due scheduled jobs into the existing worker priority queues
- Avoid enqueueing the same scheduled job repeatedly

Deliverable:

- A basic scheduler that turns future jobs into runnable queued work

### Phase 7 - Worker Pool and Concurrency Limits

Status: complete for the basic in-memory version.

Goal: process different jobs concurrently without allowing one job type to overload a dependency.

Tasks:

- Start multiple worker loops using the same priority queues
- Give each worker an ID so concurrent execution is easy to follow in logs
- Add configurable limits for individual job types
- Limit deployment jobs to one active execution at a time in the demo
- Allow job types without a configured limit to run normally

Deliverable:

- Three workers can process jobs concurrently while deployment jobs run one at a time

### Phase 8 - Queue Abstraction

Status: complete for the in-memory version.

Goal: separate job execution from the technology used to transport jobs.

Tasks:

- Define a small queue interface for publishing and consuming job IDs
- Move the existing priority channels behind an in-memory implementation
- Keep the current behavior and demo working without external services
- Make workers depend on the queue interface instead of concrete channels

Deliverable:

- The application can change queue implementations without changing handlers or job business logic

### Phase 9 - Persistent Job Repository

Status: complete for the PostgreSQL version.

Goal: make job state available to producers and workers running in separate processes.

Tasks:

- Define a repository interface based on the operations already used by the service, scheduler, and workers
- Keep the existing in-memory repository for development and tests
- Add a PostgreSQL repository for jobs, attempts, statuses, errors, priorities, and scheduled times
- Ensure status changes are persisted safely

Deliverable:

- Different application processes can load and update the same jobs

### Phase 10 - RabbitMQ Job Transport

Status: complete for the first distributed version.

Goal: allow workers in different application instances to consume durable jobs.

Tasks:

- Add a RabbitMQ implementation of the queue interface
- Preserve high, medium, and low priority routing
- Use manual acknowledgements after a job reaches a safe final or retry state
- Configure consumer prefetch to avoid sending too much work to one worker
- Keep the in-memory queue available as the default development option

Deliverable:

- Producers and workers can run as separate processes without changing job handlers

### Phase 11 - Durable Retry and Backoff

Status: complete.

Goal: make retries survive process restarts and avoid repeatedly hitting a failing dependency.

Tasks:

- Replace the fixed retry delay with exponential backoff
- Add jitter so many failed jobs do not retry simultaneously
- Store the next retry time instead of relying only on a sleeping goroutine
- Preserve the existing maximum-attempt and dead-letter behavior

Deliverable:

- Failed jobs retry at increasing durable intervals and still reach the dead-letter state when exhausted

### Phase 12 - Optional Redis Extension

Status: optional future learning phase.

Goal: broadcast job lifecycle events and protect work across multiple instances.

Tasks:

- Publish `job.started`, `job.completed`, `job.failed`, and `job.dead_lettered` events
- Add optional Redis Pub/Sub subscribers for dashboards and notifications
- Add Redis-backed distributed locks
- Add idempotency keys so re-delivered jobs do not repeat unsafe side effects

Deliverable:

- External services can react to live job events while workers remain safe against duplicate execution

### Phase 13+

Add these only after the MVP is stable:

- Metrics and production hardening
- HTTP submission API
- Optional gRPC API if multiple internal services require it

---

## Suggested Starting Structure

For the first iteration, the layout is intentionally small:

```text
/
  main.go
  internal/
    jobs/
      model.go
      repository.go
      dispatcher.go
      handlers.go
      postgres.go
      queue.go
      rabbitmq.go
      scheduler.go
      service.go
      worker.go
```

This can expand later if we add an API server, persistent storage, metrics, or scheduler packages.

---

## Completed Build Scope

The first vertical slice now includes:

- Initialized Go module
- Job model and enums
- In-memory repository
- Basic `SubmitJob` flow
- Job metadata for type, payload, priority, scheduled time, and max retries
- Handler registration and dispatching
- Sample handlers for `email` and `deployment`
- Simple worker execution loop
- Retry and dead-letter handling
- Priority-aware queues
- Scheduled job enqueueing
- Basic multi-worker execution
- Per-job-type concurrency limits
- Replaceable queue interface with an in-memory priority queue
- Replaceable repository interface with PostgreSQL persistence
- RabbitMQ priority queue with persistent messages and publisher confirms
- Manual acknowledgements, negative acknowledgements, and consumer prefetch
- Separate `producer` and `worker` runtime modes
- Docker Compose development environment
- Durable retry timestamps stored with jobs
- Exponential backoff with jitter
- Scheduler recovery of retries after restarts
- Demo flow from `main.go`

---

## Non-Goals For The First Slice

To keep the foundation clean, the first implementation should avoid:

- Redis or PostgreSQL integration
- Distributed locks
- Metrics exporters
- Advanced retry and backoff policies
- HTTP or gRPC APIs
- Complex scheduler logic

---

## Next Phase

The core job distribution project is complete. The remaining ideas are optional extensions rather than requirements for the main worker platform:

- Add Redis event publishing as a guided learning exercise
- Add distributed idempotency before running many worker instances
- Add an HTTP or gRPC submission API only when an external client needs one
- Add metrics and structured logging before a production deployment

Redis and gRPC are intentionally not implemented in the core project so they can be added separately without making the current code harder to understand.
