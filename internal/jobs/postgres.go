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
	_, err := r.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS jobs (
			id BIGSERIAL PRIMARY KEY,
			type TEXT NOT NULL,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			status TEXT NOT NULL,
			priority TEXT NOT NULL,
			scheduled_at TIMESTAMPTZ,
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

	_, err = r.pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS jobs_due_scheduled_idx
		ON jobs (scheduled_at)
		WHERE status = 'queued' AND enqueued = FALSE
	`)
	return err
}

func (r *PostgresRepository) Create(ctx context.Context, job Job) (Job, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return Job{}, err
	}

	now := time.Now()
	job.Status = JobStatusQueued
	job.CreatedAt = now
	job.UpdatedAt = now

	err = r.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			type, payload, status, priority, scheduled_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id
	`,
		job.Type,
		payload,
		job.Status,
		job.Priority,
		job.ScheduledAt,
		job.Enqueued,
		job.MaxRetries,
		job.Attempts,
		job.Error,
		job.CreatedAt,
		job.UpdatedAt,
	).Scan(&job.ID)

	return job, err
}

func (r *PostgresRepository) Get(ctx context.Context, id int) (Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}

	return job, err
}

func (r *PostgresRepository) Save(ctx context.Context, job Job) error {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return err
	}

	job.UpdatedAt = time.Now()
	result, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET type = $2,
			payload = $3,
			status = $4,
			priority = $5,
			scheduled_at = $6,
			enqueued = $7,
			max_retries = $8,
			attempts = $9,
			error = $10,
			updated_at = $11
		WHERE id = $1
	`,
		job.ID,
		job.Type,
		payload,
		job.Status,
		job.Priority,
		job.ScheduledAt,
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

func (r *PostgresRepository) List(ctx context.Context) ([]Job, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, enqueued,
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

func (r *PostgresRepository) DueScheduledJobs(ctx context.Context, now time.Time) ([]Job, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, type, payload, status, priority, scheduled_at, enqueued,
			max_retries, attempts, error, created_at, updated_at
		FROM jobs
		WHERE status = $1
			AND enqueued = FALSE
			AND scheduled_at IS NOT NULL
			AND scheduled_at <= $2
		ORDER BY scheduled_at, id
	`, JobStatusQueued, now)
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

	err := row.Scan(
		&job.ID,
		&jobType,
		&payload,
		&status,
		&priority,
		&scheduledAt,
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

	return job, nil
}
