package jobs

import (
	"context"
	"log"
)

type Worker struct {
	Repo       *Repository
	Dispatcher *Dispatcher
	Queue      chan int
}

func NewWorker(repo *Repository, dispatcher *Dispatcher) *Worker {
	return &Worker{
		Repo:       repo,
		Dispatcher: dispatcher,
		Queue:      make(chan int, 100),
	}
}

func (w *Worker) Start() {
	log.Println("worker started")

	for jobID := range w.Queue {
		w.Process(context.Background(), jobID)
	}
}

func (w *Worker) Process(ctx context.Context, jobID int) {
	job, exists := w.Repo.Get(jobID)
	if !exists {
		log.Println("job not found:", jobID)
		return
	}

	job.Status = JobStatusRunning
	w.Repo.Save(job)

	log.Println("processing job", job.ID, "type:", job.Type)

	err := w.Dispatcher.Run(ctx, job)

	if err != nil {
		job.Status = JobStatusFailed
		job.Error = err.Error()
		w.Repo.Save(job)
		log.Println("job failed", job.ID, err)
		return
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	w.Repo.Save(job)

	log.Println("job completed", job.ID)
}
