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

## Current Status

The repository now has the first working in-memory version:

- Phase 0 is complete
- Phase 1 is complete for in-memory job submission
- Phase 2 is complete for handler registration and sample handlers
- Phase 3 is complete for a basic single-worker execution loop
- Phase 4 is complete for simple retries and dead-letter handling
- Phase 5 is complete for simple priority queues

Scheduled jobs can store a future `ScheduledAt` time, but automatic future execution is intentionally left for the scheduling phase.

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

### Phase 6+

Add these only after the MVP is stable:

- Scheduling
- Per-job-type concurrency limits
- Distributed locking
- Idempotency
- Metrics and production hardening

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

The next major phase should focus on scheduling:

- Find queued jobs whose `ScheduledAt` time has arrived
- Move ready scheduled jobs into the worker queues
- Keep future jobs stored without running them early
- Keep the code simple enough to trace from `main.go`
