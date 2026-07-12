package jobs

import (
	"sync"
	"time"
)

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

func (r *Repository) Create(job Job) Job {
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

	return job
}

func (r *Repository) Get(id int) (Job, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job, exists := r.jobs[id]
	if !exists {
		return Job{}, false
	}

	return job, true
}

func (r *Repository) Save(job Job) {
	r.mu.Lock()
	defer r.mu.Unlock()

	job.UpdatedAt = time.Now()
	r.jobs[job.ID] = job
}

func (r *Repository) List() []Job {
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs := make([]Job, 0, len(r.jobs))
	for _, id := range r.order {
		jobs = append(jobs, r.jobs[id])
	}

	return jobs
}

func (r *Repository) DueScheduledJobs(now time.Time) []Job {
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs := []Job{}
	for _, id := range r.order {
		job := r.jobs[id]

		if job.Status != JobStatusQueued {
			continue
		}

		if job.Enqueued {
			continue
		}

		if job.ScheduledAt == nil {
			continue
		}

		if job.ScheduledAt.After(now) {
			continue
		}

		jobs = append(jobs, job)
	}

	return jobs
}
