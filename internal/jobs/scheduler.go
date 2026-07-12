package jobs

import (
	"log"
	"time"
)

type Scheduler struct {
	Repo     *Repository
	Worker   *Worker
	Interval time.Duration
}

func NewScheduler(repo *Repository, worker *Worker) *Scheduler {
	return &Scheduler{
		Repo:     repo,
		Worker:   worker,
		Interval: 500 * time.Millisecond,
	}
}

func (s *Scheduler) Start() {
	log.Println("scheduler started")

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for range ticker.C {
		s.EnqueueDueJobs()
	}
}

func (s *Scheduler) EnqueueDueJobs() {
	dueJobs := s.Repo.DueScheduledJobs(time.Now())

	for _, job := range dueJobs {
		log.Println("scheduled job is ready", job.ID, "type:", job.Type)
		s.Worker.Enqueue(job)
	}
}
