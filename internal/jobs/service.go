package jobs

import (
	"context"
	"fmt"
	"time"
)

type Service struct {
	Repo   JobRepository
	Worker *Worker
}

func NewService(repo JobRepository, worker *Worker) *Service {
	return &Service{
		Repo:   repo,
		Worker: worker,
	}
}

func (s *Service) SubmitJob(ctx context.Context, input SubmitJobInput) (Job, error) {
	if input.Type == "" {
		return Job{}, fmt.Errorf("job type is required")
	}
	if input.MaxRetries < 0 {
		return Job{}, fmt.Errorf("max retries cannot be negative")
	}
	if len(input.IdempotencyKey) > 200 {
		return Job{}, fmt.Errorf("idempotency key cannot exceed 200 characters")
	}

	if input.Priority == "" {
		input.Priority = JobPriorityMedium
	}
	if input.Priority != JobPriorityLow && input.Priority != JobPriorityMedium && input.Priority != JobPriorityHigh {
		return Job{}, fmt.Errorf("priority must be low, medium, or high")
	}

	if input.Payload == nil {
		input.Payload = map[string]string{}
	}

	job := Job{
		IdempotencyKey: input.IdempotencyKey,
		Type:           input.Type,
		Payload:        input.Payload,
		Priority:       input.Priority,
		ScheduledAt:    input.ScheduledAt,
		MaxRetries:     input.MaxRetries,
	}

	job, created, err := s.Repo.Create(ctx, job)
	if err != nil {
		return Job{}, err
	}
	if !created {
		return job, nil
	}

	if job.ScheduledAt == nil || !job.ScheduledAt.After(time.Now()) {
		if err := s.Worker.Enqueue(ctx, job); err != nil {
			return Job{}, err
		}
	}

	return job, nil
}

func (s *Service) ListJobs(ctx context.Context) ([]Job, error) {
	return s.Repo.List(ctx)
}
