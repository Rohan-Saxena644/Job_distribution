package jobs

import (
	"context"
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
	if input.Priority == "" {
		input.Priority = JobPriorityMedium
	}

	if input.Payload == nil {
		input.Payload = map[string]string{}
	}

	job := Job{
		Type:        input.Type,
		Payload:     input.Payload,
		Priority:    input.Priority,
		ScheduledAt: input.ScheduledAt,
		MaxRetries:  input.MaxRetries,
	}

	job, err := s.Repo.Create(ctx, job)
	if err != nil {
		return Job{}, err
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
