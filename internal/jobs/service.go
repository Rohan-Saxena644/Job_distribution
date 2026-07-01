package jobs

import "time"

type Service struct {
	Repo   *Repository
	Worker *Worker
}

func NewService(repo *Repository, worker *Worker) *Service {
	return &Service{
		Repo:   repo,
		Worker: worker,
	}
}

func (s *Service) SubmitJob(input SubmitJobInput) Job {
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

	job = s.Repo.Create(job)

	if job.ScheduledAt == nil || !job.ScheduledAt.After(time.Now()) {
		s.Worker.Queue <- job.ID
	}

	return job
}

func (s *Service) ListJobs() []Job {
	return s.Repo.List()
}
