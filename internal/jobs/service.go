package jobs

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

	job := Job{
		Type:       input.Type,
		Payload:    input.Payload,
		Priority:   input.Priority,
		MaxRetries: input.MaxRetries,
	}

	job = s.Repo.Create(job)

	s.Worker.Queue <- job.ID

	return job
}

func (s *Service) ListJobs() []Job {
	return s.Repo.List()
}
