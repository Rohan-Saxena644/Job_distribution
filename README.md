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
- TLS-protected gRPC API with bearer-token authentication
- Idempotent job submission
- Atomic PostgreSQL worker claims
- Docker Compose secret-file support
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
                         +-> atomically claims job in PostgreSQL
                         +-> dispatcher selects handler
                         +-> handler executes work
                         +-> final status is saved
                         +-> RabbitMQ delivery is acknowledged
                         +-> Redis publishes lifecycle events

Scheduler -> finds scheduled jobs and retries in PostgreSQL
          -> returns due jobs to RabbitMQ
```

RabbitMQ transports only job IDs. PostgreSQL is the shared source of truth for the complete job and its status. Redis broadcasts live events but is not used for durable job delivery.

RabbitMQ provides at-least-once delivery. A PostgreSQL status update allows only one worker to claim a queued job, so duplicate queue messages are acknowledged without executing the handler twice. A redelivered unacknowledged message can reclaim a `running` job after its previous worker connection is lost.

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
|-- scripts/
|   `-- generate-local-secrets.ps1
|-- secrets/
|   `-- README.md
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
- OpenSSL for generating the local TLS certificate

## Quick Start

### In-Memory Demo

Run the application without PostgreSQL or RabbitMQ:

```powershell
go run .
```

The demo submits sample jobs, starts three workers, waits for scheduled work and retries, and prints the final job states.

### Distributed Demo

Generate ignored local credentials, a bearer token, and a self-signed TLS certificate:

```powershell
./scripts/generate-local-secrets.ps1
```

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

Open RabbitMQ Management at [http://localhost:15673](http://localhost:15673). The generated username and password are stored in the ignored `secrets/rabbitmq.conf` file.

Stop the services:

```powershell
docker compose down
```

The PostgreSQL and RabbitMQ Docker volumes are preserved when the services are stopped.

If RabbitMQ is restarted by itself, restart the API and worker so they open fresh broker connections:

```powershell
docker compose restart api worker
```

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
| `DATABASE_URL_FILE` | No | File containing the PostgreSQL URL. Takes precedence over `DATABASE_URL`. |
| `RABBITMQ_URL` | Distributed mode | RabbitMQ AMQP connection URL. |
| `RABBITMQ_URL_FILE` | No | File containing the RabbitMQ URL. Takes precedence over `RABBITMQ_URL`. |
| `JOB_QUEUE_NAME` | No | RabbitMQ queue name. Defaults to `jobs`. |
| `REDIS_URL` | No | Redis connection URL. Enables lifecycle event publishing. |
| `REDIS_URL_FILE` | No | File containing the Redis URL. Takes precedence over `REDIS_URL`. |
| `EVENT_CHANNEL` | No | Redis Pub/Sub channel. Defaults to `job.events`. |
| `GRPC_ADDRESS` | API/client mode | Address the gRPC server listens on or the client connects to. |
| `GRPC_TLS_CERT_FILE` | Secure API | Server certificate file. |
| `GRPC_TLS_KEY_FILE` | Secure API | Server private-key file. |
| `GRPC_TLS_CA_FILE` | Secure client | Certificate authority file used to verify the server. |
| `GRPC_TLS_SERVER_NAME` | Secure client | Server name expected in the TLS certificate. |
| `GRPC_AUTH_TOKEN_FILE` | Secure API/client | File containing the bearer token. |
| `GRPC_ALLOW_INSECURE` | No | Set to `true` only for isolated local experiments without TLS/authentication. |
| `IDEMPOTENCY_KEY` | gRPC client | Optional key for the included sample client. A unique value is generated when omitted. |

The PostgreSQL and RabbitMQ URLs must either both be provided or both be absent. Each can come from its direct variable or its `_FILE` variable. Leaving both absent selects the in-memory implementations.

## gRPC API

The API contract is documented in [`api/jobs.proto`](api/jobs.proto). It exposes one method:

```text
jobs.v1.JobService/SubmitJob
```

The request is a protobuf `Struct` with these fields:

```json
{
  "idempotency_key": "create-welcome-email-for-user-42",
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

`idempotency_key` is required by the gRPC API. Repeating the same key returns the original job and does not enqueue another message. `scheduled_at` is optional and uses RFC3339 format. The included `grpc-client` mode demonstrates a TLS-authenticated call without requiring another command-line tool.

The API refuses to start without TLS and its bearer token. Authentication is also refused without TLS so the token cannot be sent over an unencrypted connection. Insecure mode requires an explicit `GRPC_ALLOW_INSECURE=true` opt-in and should only be used for isolated local experiments.

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
3. Submit the job type, payload, priority, schedule, retry limit, and a stable idempotency key through `Service.SubmitJob`.

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
3. Replace every generated file under `secrets/` with deployment-specific values and a trusted TLS certificate.
4. Keep PostgreSQL, RabbitMQ, and Redis on private interfaces.
5. Change the gRPC host binding only if external clients must connect directly.
6. Start the services with `docker compose up -d --build`.
7. Check worker health through logs with `docker compose logs worker`.

Generated secret files are ignored by Git. The local certificate is self-signed and must not be used as a public production certificate.

### Container Platform Deployment

For a production-style deployment:

1. Provision PostgreSQL, RabbitMQ, and optionally Redis, preferably as private or managed services.
2. Build and publish the Docker image to a container registry.
3. Deploy one or more copies of the image with `APP_MODE=worker`.
4. Deploy the image with `APP_MODE=api` when external services need gRPC submission.
5. Mount database, broker, Redis, TLS, and authentication values as secret files.
6. Keep PostgreSQL, RabbitMQ, and Redis private rather than exposing them directly to the internet.

The worker does not expose a port. The API container exposes gRPC on port `50051`; it does not provide a website or HTTP/JSON API.

## Current Limitations

- The producer mode submits fixed demonstration jobs.
- The gRPC API currently exposes only job submission.
- Redis Pub/Sub events are live notifications and are not replayable.
- RabbitMQ delivery and handler side effects are at-least-once; external handlers should also use idempotency when calling third-party systems.
- Per-job-type concurrency limits apply inside one worker process, not globally across every replica.
- The sample handlers simulate work rather than calling real external services.
- RabbitMQ does not automatically reconnect after a broker restart; restart the API and worker as shown above.
- Production metrics and tracing are intentionally left as later operational improvements.
- The automatic PostgreSQL schema setup is convenient for the MVP but is not a full migration system.

## Possible Extensions

- Additional gRPC methods for getting and listing jobs
- Generated typed protobuf clients
- HTTP/JSON gateway
- Structured logging and metrics
- Role-based authorization
- Production database migrations

## Project Status

The project now includes PostgreSQL persistence, RabbitMQ delivery, Redis lifecycle events, TLS-authenticated gRPC submission, idempotency, atomic worker claims, retries, scheduling, and Docker secret files. The remaining work is operational hardening such as managed migrations, metrics, tracing, and RabbitMQ connection recovery.
