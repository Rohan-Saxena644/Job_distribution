package jobs

import (
	"context"
	"log"
	"time"
)

type Worker struct {
	Repo        *Repository
	Dispatcher  *Dispatcher
	HighQueue   chan int
	MediumQueue chan int
	LowQueue    chan int
	RetryDelay  time.Duration
}

func NewWorker(repo *Repository, dispatcher *Dispatcher) *Worker {
	return &Worker{
		Repo:        repo,
		Dispatcher:  dispatcher,
		HighQueue:   make(chan int, 100),
		MediumQueue: make(chan int, 100),
		LowQueue:    make(chan int, 100),
		RetryDelay:  300 * time.Millisecond,
	}
}

func (w *Worker) Enqueue(job Job) {
	switch job.Priority {
	case JobPriorityHigh:
		w.HighQueue <- job.ID
	case JobPriorityLow:
		w.LowQueue <- job.ID
	default:
		w.MediumQueue <- job.ID
	}
}

func (w *Worker) Start() {
	log.Println("worker started")

	for {
		jobID := w.nextJob()
		w.Process(context.Background(), jobID)
	}
}

func (w *Worker) nextJob() int {
	for {
		select {
		case jobID := <-w.HighQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-w.MediumQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-w.LowQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-w.HighQueue:
			return jobID
		case jobID := <-w.MediumQueue:
			return jobID
		case jobID := <-w.LowQueue:
			return jobID
		}
	}
}

func (w *Worker) Process(ctx context.Context, jobID int) {
	job, exists := w.Repo.Get(jobID)
	if !exists {
		log.Println("job not found:", jobID)
		return
	}

	job.Status = JobStatusRunning
	job.Attempts++
	w.Repo.Save(job)

	log.Println("processing job", job.ID, "type:", job.Type, "priority:", job.Priority, "attempt:", job.Attempts)

	err := w.Dispatcher.Run(ctx, job)

	if err != nil {
		job.Error = err.Error()
		w.handleFailedJob(job, err)
		return
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	w.Repo.Save(job)

	log.Println("job completed", job.ID)
}

func (w *Worker) handleFailedJob(job Job, err error) {
	if job.Attempts <= job.MaxRetries {
		job.Status = JobStatusFailed
		w.Repo.Save(job)

		log.Println("job failed", job.ID, err)
		log.Println("retrying job", job.ID, "after", w.RetryDelay)

		go func() {
			time.Sleep(w.RetryDelay)
			w.Enqueue(job)
		}()

		return
	}

	job.Status = JobStatusDeadLetter
	w.Repo.Save(job)

	log.Println("job moved to dead letter", job.ID, err)
}
