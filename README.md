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

## Current and Target Architecture

The project is already event-driven inside one Go process:

```text
Service -> priority channels -> workers -> dispatcher -> handler
```

The high, medium, and low Go channels act as the current in-memory message queues. Workers wait for job IDs and react when work arrives. This is useful for learning and local development, but the channels cannot deliver jobs to workers running in another application instance and their contents disappear when the process stops.

The target architecture will keep the same job model and execution flow while making storage and transport replaceable:

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

Planned responsibilities:

- The existing Go channels remain the default in-memory queue for development and tests.
- The current repository remains available for local development, while PostgreSQL later provides job data shared by separate processes.
- RabbitMQ becomes the durable job transport for production workers, with manual acknowledgements after processing.
- Redis Pub/Sub broadcasts live events such as `job.started`, `job.completed`, and `job.failed`.
- Redis Pub/Sub will not be the main job queue because messages can be lost while a worker is disconnected.
- Redis can later also support distributed locks and idempotency keys, which are separate from Pub/Sub.

This direction does not require a full rewrite. The service, scheduler, dispatcher, handlers, job statuses, and concurrency limits remain useful. Small repository and queue interfaces will isolate the in-memory and production implementations from the rest of the project.

RabbitMQ messages can then contain a job ID because every producer and worker can load the same job from PostgreSQL. Sending only an ID before shared persistence exists would not work across processes.

The project currently has retry and dead-letter handling with one fixed delay. Exponential backoff and jitter are valuable later, but retry scheduling must first survive application restarts. gRPC is optional and is not part of the core worker architecture; it should only be added if separate internal services need a strongly typed job-submission API.

---

## Current Status

The repository now has the first working in-memory version:

- Phase 0 is complete
- Phase 1 is complete for in-memory job submission
- Phase 2 is complete for handler registration and sample handlers
- Phase 3 is complete for a basic single-worker execution loop
- Phase 4 is complete for simple retries and dead-letter handling
- Phase 5 is complete for simple priority queues
- Phase 6 is complete for simple scheduled job enqueueing
- Phase 7 is complete for a basic worker pool and per-job-type concurrency limits
- Phase 8 is complete for the in-memory queue abstraction

The demo runs three workers, while limiting deployment jobs to one active execution at a time.

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

Status: planned.

Goal: make job state available to producers and workers running in separate processes.

Tasks:

- Define a repository interface based on the operations already used by the service, scheduler, and workers
- Keep the existing in-memory repository for development and tests
- Add a PostgreSQL repository for jobs, attempts, statuses, errors, priorities, and scheduled times
- Ensure status changes are persisted safely

Deliverable:

- Different application processes can load and update the same jobs

### Phase 10 - RabbitMQ Job Transport

Status: planned.

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

Status: planned.

Goal: make retries survive process restarts and avoid repeatedly hitting a failing dependency.

Tasks:

- Replace the fixed retry delay with exponential backoff
- Add jitter so many failed jobs do not retry simultaneously
- Store the next retry time instead of relying only on a sleeping goroutine
- Preserve the existing maximum-attempt and dead-letter behavior

Deliverable:

- Failed jobs retry at increasing durable intervals and still reach the dead-letter state when exhausted

### Phase 12 - Redis Events and Distributed Safety

Status: planned.

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
      queue.go
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

The next phase should introduce a repository abstraction and shared persistence:

- Define a repository interface from the operations already used by the application
- Keep the current in-memory repository implementation
- Add PostgreSQL without changing the service, scheduler, or worker behavior
- Store shared job state before RabbitMQ carries job IDs between processes

RabbitMQ, Redis, and advanced backoff should be added one boundary at a time. This keeps the code understandable and prevents external infrastructure from becoming mixed into the worker and handler logic. gRPC remains optional because it does not improve the core job execution path by itself.
