package jobs

import (
	"context"
	"log"
	"time"
)

type Scheduler struct {
	Repo     JobRepository
	Worker   *Worker
	Interval time.Duration
}

func NewScheduler(repo JobRepository, worker *Worker) *Scheduler {
	return &Scheduler{
		Repo:     repo,
		Worker:   worker,
		Interval: 500 * time.Millisecond,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	log.Println("scheduler started")

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.EnqueueDueJobs(ctx); err != nil {
				log.Println("scheduler error:", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Scheduler) EnqueueDueJobs(ctx context.Context) error {
	dueJobs, err := s.Repo.DueJobs(ctx, time.Now())
	if err != nil {
		return err
	}

	for _, job := range dueJobs {
		if job.Status == JobStatusFailed {
			log.Println("retry job is ready", job.ID, "type:", job.Type)
		} else {
			log.Println("scheduled job is ready", job.ID, "type:", job.Type)
		}

		if err := s.Worker.Enqueue(ctx, job); err != nil {
			return err
		}
	}

	return nil
}
