package jobs

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrJobNotFound = errors.New("job not found")

type JobRepository interface {
	Create(ctx context.Context, job Job) (Job, error)
	Get(ctx context.Context, id int) (Job, error)
	Save(ctx context.Context, job Job) error
	List(ctx context.Context) ([]Job, error)
	DueJobs(ctx context.Context, now time.Time) ([]Job, error)
	Close()
}

type Repository struct {
	mu     sync.Mutex
	nextID int
	jobs   map[int]Job
	order  []int
}

func NewRepository() *Repository {
	return &Repository{
		jobs: make(map[int]Job),
	}
}

func (r *Repository) Create(ctx context.Context, job Job) (Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextID++

	now := time.Now()
	job.ID = r.nextID
	job.Status = JobStatusQueued
	job.CreatedAt = now
	job.UpdatedAt = now

	r.jobs[job.ID] = job
	r.order = append(r.order, job.ID)

	return job, nil
}

func (r *Repository) Get(ctx context.Context, id int) (Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, exists := r.jobs[id]
	if !exists {
		return Job{}, ErrJobNotFound
	}

	return job, nil
}

func (r *Repository) Save(ctx context.Context, job Job) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[job.ID]; !exists {
		return ErrJobNotFound
	}

	job.UpdatedAt = time.Now()
	r.jobs[job.ID] = job
	return nil
}

func (r *Repository) List(ctx context.Context) ([]Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs := make([]Job, 0, len(r.jobs))
	for _, id := range r.order {
		jobs = append(jobs, r.jobs[id])
	}

	return jobs, nil
}

func (r *Repository) DueJobs(ctx context.Context, now time.Time) ([]Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs := []Job{}
	for _, id := range r.order {
		job := r.jobs[id]

		if job.Enqueued {
			continue
		}

		if job.Status == JobStatusQueued && job.ScheduledAt != nil && !job.ScheduledAt.After(now) {
			jobs = append(jobs, job)
		}

		if job.Status == JobStatusFailed && job.NextRetryAt != nil && !job.NextRetryAt.After(now) {
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

func (r *Repository) Close() {}
