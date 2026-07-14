# Job Distribution Platform

A background job-processing platform built in Go with priority queues, concurrent workers, scheduling, durable retries, PostgreSQL, and RabbitMQ.

The project can run entirely in memory for learning and local testing, or in distributed mode with producers and workers running as separate processes.

The original phase-by-phase development notes are preserved in [JOURNEY.txt](JOURNEY.txt).

## Features

- Generic job handlers registered by job type
- High, medium, and low job priorities
- Multiple concurrent workers
- Per-job-type concurrency limits
- Future job scheduling
- Configurable maximum retry attempts
- Exponential retry backoff with jitter
- Dead-letter status for exhausted jobs
- PostgreSQL job persistence
- RabbitMQ persistent messages and publisher confirms
- Manual RabbitMQ acknowledgements and requeueing
- In-memory repository and queue for local development
- Separate demo, producer, and worker runtime modes
- Docker and Docker Compose support

## Architecture

### In-Memory Mode

```text
Service -> in-memory repository -> priority channels -> workers
                                                     -> dispatcher
                                                     -> handler
```

This mode requires no external services. Jobs and queue contents are lost when the process exits.

### Distributed Mode

```text
Producer
   |
   +-> PostgreSQL stores the complete job
   |
   +-> RabbitMQ receives the job ID
                         |
                         v
                      Worker
                         |
                         +-> loads job from PostgreSQL
                         +-> dispatcher selects handler
                         +-> handler executes work
                         +-> final status is saved
                         +-> RabbitMQ delivery is acknowledged

Scheduler -> finds scheduled jobs and retries in PostgreSQL
          -> returns due jobs to RabbitMQ
```

RabbitMQ transports only job IDs. PostgreSQL is the shared source of truth for the complete job and its status.

## Job Lifecycle

A normal job moves through these states:

```text
queued -> running -> completed
```

A failed job with retries remaining follows this path:

```text
running -> failed -> retry scheduled -> queued -> running
```

When all retries are exhausted:

```text
running -> dead_letter
```

Retry times are stored in PostgreSQL. This allows the scheduler to recover pending retries after a worker restart.

## Technology

- Go 1.26
- PostgreSQL 17
- RabbitMQ 4
- `pgx` PostgreSQL driver
- `amqp091-go` RabbitMQ client
- Docker and Docker Compose

## Project Structure

```text
.
|-- main.go
|-- Dockerfile
|-- compose.yaml
|-- internal/
|   `-- jobs/
|       |-- model.go
|       |-- service.go
|       |-- worker.go
|       |-- scheduler.go
|       |-- dispatcher.go
|       |-- handlers.go
|       |-- repository.go
|       |-- postgres.go
|       |-- queue.go
|       `-- rabbitmq.go
`-- JOURNEY.txt
```

## Requirements

For the in-memory demo:

- Go 1.26 or newer

For distributed mode:

- Docker
- Docker Compose

## Quick Start

### In-Memory Demo

Run the application without PostgreSQL or RabbitMQ:

```powershell
go run .
```

The demo submits sample jobs, starts three workers, waits for scheduled work and retries, and prints the final job states.

### Distributed Demo

Start PostgreSQL, RabbitMQ, and the worker:

```powershell
docker compose up -d --build postgres rabbitmq worker
```

Submit sample jobs from a separate producer container:

```powershell
docker compose --profile tools run --rm producer
```

Follow the worker logs:

```powershell
docker compose logs -f worker
```

Inspect persisted jobs:

```powershell
docker compose exec postgres psql -U jobs -d jobs -c "SELECT id, type, priority, status, attempts, next_retry_at FROM jobs ORDER BY id;"
```

Open RabbitMQ Management at [http://localhost:15673](http://localhost:15673).

```text
Username: jobs
Password: jobs
```

Stop the services:

```powershell
docker compose down
```

The PostgreSQL Docker volume is preserved when the services are stopped.

## Runtime Modes

| Mode | Purpose |
| --- | --- |
| `demo` | Submits and processes sample jobs in one process, then exits. |
| `producer` | Submits sample jobs and exits. |
| `worker` | Runs the worker pool and scheduler continuously. |

`demo` is used when `APP_MODE` is not provided.

## Configuration

| Variable | Required | Description |
| --- | --- | --- |
| `APP_MODE` | No | `demo`, `producer`, or `worker`. Defaults to `demo`. |
| `DATABASE_URL` | Distributed mode | PostgreSQL connection URL. |
| `RABBITMQ_URL` | Distributed mode | RabbitMQ AMQP connection URL. |
| `JOB_QUEUE_NAME` | No | RabbitMQ queue name. Defaults to `jobs`. |

`DATABASE_URL` and `RABBITMQ_URL` must either both be set or both be empty. Leaving both empty selects the in-memory implementations.

## Sample Jobs

The demo registers three sample handlers:

- `email` simulates sending an email and succeeds.
- `webhook` simulates calling a webhook and succeeds.
- `deployment` simulates a failing operation so retries, backoff, concurrency limits, and dead-lettering are visible.

The deployment handler does not perform a real deployment. It does not connect to Docker, Kubernetes, a cloud provider, or another server.

## Adding A Job Type

To add a real job type:

1. Create a handler implementing `Execute(context.Context, Job) error`.
2. Register the handler with the dispatcher using a unique job type.
3. Submit the job type, payload, priority, schedule, and retry limit through `Service.SubmitJob`.

Handlers contain job-specific business logic. The worker, scheduler, repository, queue, and retry flow do not need to change when a handler is added.

## Testing

Run the automated tests:

```powershell
go test ./...
```

Run static checks:

```powershell
go vet ./...
```

## Deployment

### Docker Image

Build the worker image:

```powershell
docker build -t job-distributed:latest .
```

The image starts the binary defined in the `Dockerfile`. Set `APP_MODE=worker` when running it as a long-lived worker service.

### Single-Server Deployment

For a private demonstration server:

1. Install Docker and Docker Compose on the server.
2. Place the repository on the server.
3. Replace the development database and RabbitMQ credentials in `compose.yaml`.
4. Remove any ports that do not need public access.
5. Start the services with `docker compose up -d --build`.
6. Check worker health through logs with `docker compose logs worker`.

The credentials in the repository are only for local development. Do not use them on a public server.

### Container Platform Deployment

For a production-style deployment:

1. Provision PostgreSQL and RabbitMQ, preferably as private or managed services.
2. Build and publish the Docker image to a container registry.
3. Deploy one or more copies of the image with `APP_MODE=worker`.
4. Provide `DATABASE_URL`, `RABBITMQ_URL`, and `JOB_QUEUE_NAME` as secrets or environment variables.
5. Keep PostgreSQL and RabbitMQ private rather than exposing them directly to the internet.

The worker is a background service and does not expose an HTTP port. There is no website or public API in the current version.

## Current Limitations

- The producer mode submits fixed demonstration jobs.
- There is no HTTP or gRPC submission API yet.
- Redis events and distributed idempotency are not implemented.
- The sample handlers simulate work rather than calling real external services.
- RabbitMQ connection recovery and production observability still need hardening.
- The automatic PostgreSQL schema setup is convenient for the MVP but is not a full migration system.

## Possible Extensions

- Redis lifecycle events
- Distributed locks and idempotency keys
- HTTP or gRPC job-submission API
- Structured logging and metrics
- Authentication and authorization
- Production database migrations

## Project Status

The core distributed job-processing MVP is complete and ready to demonstrate. Production deployment should add secure secret management, idempotency, observability, automated integration tests, and connection recovery.
