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

The repository is still at the foundation stage:

- `README.md` defines the architecture direction
- `main.go` is currently empty
- `go.mod` has not been created yet

That means the next prompt should focus on a tight first slice instead of jumping to advanced features.

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

Goal: process jobs asynchronously.

Tasks:

- Create a basic worker or worker pool
- Pull jobs from a queue
- Mark jobs as `pending`, `running`, `completed`, or `failed`
- Execute the registered handler
- Persist status changes

Deliverable:

- A functional worker flow that processes jobs from a queue

### Phase 4+

Add these only after the MVP is stable:

- Retries and dead-letter handling
- Priority queues
- Scheduling
- Per-job-type concurrency limits
- Distributed locking
- Idempotency
- Metrics and production hardening

---

## Suggested Starting Structure

For the first iteration, keep the layout small:

```text
/
  cmd/
    app/
      main.go
  internal/
    jobs/
      model.go
      repository.go
      dispatcher.go
      service.go
      worker.go
```

This can expand later if we add an API server, persistent storage, metrics, or scheduler packages.

---

## Immediate Build Scope

The next implementation prompt should focus only on this first vertical slice:

- Initialize the Go module
- Define the job model and enums
- Build an in-memory repository
- Add handler registration and dispatching
- Add a simple worker execution loop
- Demonstrate the flow from `main.go`

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

## Definition of Done For The Next Prompt

We can consider the first build successful when:

- The project compiles
- A sample job can be submitted
- A registered handler can process it
- Job state changes are visible in memory
- The code structure leaves room for retries and scheduling later
