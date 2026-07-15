package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, databaseURL string) (*PostgresRepository, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	repo := &PostgresRepository{pool: pool}
	if err := repo.createSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return repo, nil
}

func (r *PostgresRepository) createSchema(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(742013)`); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobs (
			id BIGSERIAL PRIMARY KEY,
			idempotency_key TEXT NOT NULL DEFAULT '',
			type TEXT NOT NULL,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL,
			priority TEXT NOT NULL,
			scheduled_at TIMESTAMPTZ,
			next_retry_at TIMESTAMPTZ,
			enqueued BOOLEAN NOT NULL DEFAULT FALSE,
			max_retries INTEGER NOT NULL DEFAULT 0,
			attempts INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		ALTER TABLE jobs
		ADD COLUMN IF NOT EXISTS idempotency_key TEXT NOT NULL DEFAULT ''
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		ALTER TABLE jobs
		ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_idx
		ON jobs (idempotency_key)
		WHERE idempotency_key <> ''
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS jobs_due_scheduled_idx
		ON jobs (scheduled_at)
		WHERE status = 'queued' AND enqueued = FALSE
	`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS jobs_due_retry_idx
		ON jobs (next_retry_at)
		WHERE status = 'failed' AND enqueued = FALSE
	`)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepository) Create(ctx context.Context, job Job) (Job, bool, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return Job{}, false, err
	}

	now := time.Now()
	job.Status = JobStatusQueued
	job.CreatedAt = now
	job.UpdatedAt = now

	err = r.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (idempotency_key) WHERE idempotency_key <> '' DO NOTHING
		RETURNING id
	`,
		job.IdempotencyKey,
		job.Type,
		payload,
		job.Status,
		job.Priority,
		job.ScheduledAt,
		job.NextRetryAt,
		job.Enqueued,
		job.MaxRetries,
		job.Attempts,
		job.Error,
		job.CreatedAt,
		job.UpdatedAt,
	).Scan(&job.ID)
	if errors.Is(err, pgx.ErrNoRows) && job.IdempotencyKey != "" {
		existing, getErr := r.getByIdempotencyKey(ctx, job.IdempotencyKey)
		return existing, false, getErr
	}

	return job, true, err
}

func (r *PostgresRepository) Get(ctx context.Context, id int) (Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}

	return job, err
}

func (r *PostgresRepository) getByIdempotencyKey(ctx context.Context, key string) (Job, error) {
	return scanJob(r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		WHERE idempotency_key = $1
	`, key))
}

func (r *PostgresRepository) Save(ctx context.Context, job Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	job.UpdatedAt = time.Now()
	result, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET idempotency_key = $2,
			type = $3,
			payload = $4,
			status = $5,
			priority = $6,
			scheduled_at = $7,
			next_retry_at = $8,
			enqueued = $9,
			max_retries = $10,
			attempts = $11,
			error = $12,
			updated_at = $13
		WHERE id = $1
	`,
		job.ID,
		job.IdempotencyKey,
		job.Type,
		payload,
		job.Status,
		job.Priority,
		job.ScheduledAt,
		job.NextRetryAt,
		job.Enqueued,
		job.MaxRetries,
		job.Attempts,
		job.Error,
		job.UpdatedAt,
	)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		return ErrJobNotFound
	}

	return nil
}

func (r *PostgresRepository) MarkEnqueued(ctx context.Context, id int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET enqueued = TRUE, updated_at = $2
		WHERE id = $1 AND status IN ($3, $4)
	`, id, time.Now(), JobStatusQueued, JobStatusFailed)
	return err
}

func (r *PostgresRepository) Claim(ctx context.Context, id int, allowRunning bool) (Job, bool, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = $2,
			enqueued = FALSE,
			next_retry_at = NULL,
			attempts = attempts + 1,
			error = '',
			updated_at = $3
		WHERE id = $1
			AND (status IN ($4, $5) OR ($6 AND status = $2))
		RETURNING id, idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
	`, id, JobStatusRunning, time.Now(), JobStatusQueued, JobStatusFailed, allowRunning))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}

	return job, true, nil
}

func (r *PostgresRepository) List(ctx context.Context) ([]Job, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

func (r *PostgresRepository) DueJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, type, payload, status, priority, scheduled_at, next_retry_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		WHERE enqueued = FALSE
			AND (
				(status = $1 AND (scheduled_at IS NULL OR scheduled_at <= $3))
				OR
				(status = $2 AND next_retry_at IS NOT NULL AND next_retry_at <= $3)
			)
		ORDER BY COALESCE(next_retry_at, scheduled_at), id
	`, JobStatusQueued, JobStatusFailed, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row rowScanner) (Job, error) {
	var job Job
	var payload []byte
	var jobType string
	var status string
	var priority string
	var scheduledAt sql.NullTime
	var nextRetryAt sql.NullTime

	err := row.Scan(
		&job.ID,
		&job.IdempotencyKey,
		&jobType,
		&payload,
		&status,
		&priority,
		&scheduledAt,
		&nextRetryAt,
		&job.Enqueued,
		&job.MaxRetries,
		&job.Attempts,
		&job.Error,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return Job{}, err
	}

	if err := json.Unmarshal(payload, &job.Payload); err != nil {
		return Job{}, err
	}

	job.Type = JobType(jobType)
	job.Status = JobStatus(status)
	job.Priority = JobPriority(priority)
	if scheduledAt.Valid {
		job.ScheduledAt = &scheduledAt.Time
	}
	if nextRetryAt.Valid {
		job.NextRetryAt = &nextRetryAt.Time
	}

	return job, nil
}
