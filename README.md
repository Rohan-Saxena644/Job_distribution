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
- Redis Pub/Sub lifecycle events
- gRPC job-submission API
- In-memory repository and queue for local development
- Separate demo, producer, worker, API, and gRPC client modes
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
gRPC Client -> gRPC API -> Service
                            |
Producer -------------------+
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
                         +-> Redis publishes lifecycle events

Scheduler -> finds scheduled jobs and retries in PostgreSQL
          -> returns due jobs to RabbitMQ
```

RabbitMQ transports only job IDs. PostgreSQL is the shared source of truth for the complete job and its status. Redis broadcasts live events but is not used for durable job delivery.

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
- Redis 7
- gRPC and Protocol Buffers
- `pgx` PostgreSQL driver
- `amqp091-go` RabbitMQ client
- `go-redis` Redis client
- Docker and Docker Compose

## Project Structure

```text
.
|-- main.go
|-- Dockerfile
|-- compose.yaml
|-- api/
|   `-- jobs.proto
|-- internal/
|   `-- jobs/
|       |-- model.go
|       |-- service.go
|       |-- worker.go
|       |-- scheduler.go
|       |-- dispatcher.go
|       |-- events.go
|       |-- grpc.go
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

Start PostgreSQL, RabbitMQ, Redis, the worker, and the gRPC API:

```powershell
docker compose up -d --build postgres rabbitmq redis worker api
```

Submit sample jobs from a separate producer container:

```powershell
docker compose --profile tools run --rm producer
```

Submit an email job through gRPC:

```powershell
docker compose --profile tools run --rm grpc-client
```

Watch live Redis lifecycle events in another terminal before submitting jobs:

```powershell
docker compose exec redis redis-cli SUBSCRIBE job.events
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
| `api` | Runs the gRPC job-submission API. |
| `grpc-client` | Submits one sample email job through gRPC and exits. |

`demo` is used when `APP_MODE` is not provided.

## Configuration

| Variable | Required | Description |
| --- | --- | --- |
| `APP_MODE` | No | `demo`, `producer`, `worker`, `api`, or `grpc-client`. Defaults to `demo`. |
| `DATABASE_URL` | Distributed mode | PostgreSQL connection URL. |
| `RABBITMQ_URL` | Distributed mode | RabbitMQ AMQP connection URL. |
| `JOB_QUEUE_NAME` | No | RabbitMQ queue name. Defaults to `jobs`. |
| `REDIS_URL` | No | Redis connection URL. Enables lifecycle event publishing. |
| `EVENT_CHANNEL` | No | Redis Pub/Sub channel. Defaults to `job.events`. |
| `GRPC_ADDRESS` | API/client mode | Address the gRPC server listens on or the client connects to. |

`DATABASE_URL` and `RABBITMQ_URL` must either both be set or both be empty. Leaving both empty selects the in-memory implementations.

## gRPC API

The API contract is documented in [`api/jobs.proto`](api/jobs.proto). It exposes one method:

```text
jobs.v1.JobService/SubmitJob
```

The request is a protobuf `Struct` with these fields:

```json
{
  "type": "email",
  "payload": {
    "to": "user@example.com",
    "subject": "Hello"
  },
  "priority": "high",
  "max_retries": 2,
  "scheduled_at": "2026-07-14T18:00:00Z"
}
```

`scheduled_at` is optional and uses RFC3339 format. The included `grpc-client` mode demonstrates a complete client call without requiring another command-line tool.

## Redis Events

Workers publish JSON events after durable status updates:

- `job.started`
- `job.completed`
- `job.failed`
- `job.dead_lettered`

Redis Pub/Sub is intended for live dashboards, notifications, and monitoring. Subscribers that are offline can miss events, so PostgreSQL remains the durable source of truth.

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

The image starts the binary defined in the `Dockerfile`. Run it with `APP_MODE=worker` for job processing or `APP_MODE=api` for gRPC submission.

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

1. Provision PostgreSQL, RabbitMQ, and optionally Redis, preferably as private or managed services.
2. Build and publish the Docker image to a container registry.
3. Deploy one copy of the image with `APP_MODE=worker` for the current MVP.
4. Deploy the image with `APP_MODE=api` when external services need gRPC submission.
5. Provide database, broker, Redis, and gRPC configuration as secrets or environment variables.
6. Keep PostgreSQL, RabbitMQ, and Redis private rather than exposing them directly to the internet.

The worker does not expose a port. The API container exposes gRPC on port `50051`; it does not provide a website or HTTP/JSON API.

## Current Limitations

- The producer mode submits fixed demonstration jobs.
- The gRPC API currently exposes only job submission.
- Redis Pub/Sub events are live notifications and are not replayable.
- Run a single worker service. Multiple worker replicas require atomic job claiming and idempotency to avoid duplicate work.
- The sample handlers simulate work rather than calling real external services.
- The gRPC endpoint does not yet include authentication or TLS.
- RabbitMQ connection recovery and production observability still need hardening.
- The automatic PostgreSQL schema setup is convenient for the MVP but is not a full migration system.

## Possible Extensions

- Distributed locks and idempotency keys
- Additional gRPC methods for getting and listing jobs
- Generated typed protobuf clients
- HTTP/JSON gateway
- Structured logging and metrics
- Authentication and authorization
- Production database migrations

## Project Status

The distributed job-processing MVP now includes PostgreSQL, RabbitMQ, Redis lifecycle events, and gRPC submission. Production deployment should add secure secret management, authentication, TLS, idempotency, observability, automated integration tests, and connection recovery.
