package jobs

import (
	"context"
	"log"
	"time"
)

type Worker struct {
	Repo       *Repository
	Dispatcher *Dispatcher
	Queue      JobQueue
	RetryDelay time.Duration
	TypeLimits map[JobType]chan struct{}
}

func NewWorker(repo *Repository, dispatcher *Dispatcher, queue JobQueue) *Worker {
	return &Worker{
		Repo:       repo,
		Dispatcher: dispatcher,
		Queue:      queue,
		RetryDelay: 300 * time.Millisecond,
		TypeLimits: make(map[JobType]chan struct{}),
	}
}

func (w *Worker) SetConcurrencyLimit(jobType JobType, limit int) {
	if limit < 1 {
		return
	}

	w.TypeLimits[jobType] = make(chan struct{}, limit)
}

func (w *Worker) Enqueue(job Job) {
	job.Enqueued = true
	w.Repo.Save(job)
	w.Queue.Enqueue(job)
}

func (w *Worker) Start(workerID int) {
	log.Println("worker started", workerID)

	for {
		jobID := w.Queue.NextJob()
		w.Process(context.Background(), workerID, jobID)
	}
}

func (w *Worker) Process(ctx context.Context, workerID int, jobID int) {
	job, exists := w.Repo.Get(jobID)
	if !exists {
		log.Println("job not found:", jobID)
		return
	}

	w.acquireTypeSlot(workerID, job)
	defer w.releaseTypeSlot(job)

	job.Enqueued = false
	job.Status = JobStatusRunning
	job.Attempts++
	w.Repo.Save(job)

	log.Println("worker", workerID, "processing job", job.ID, "type:", job.Type, "priority:", job.Priority, "attempt:", job.Attempts)

	err := w.Dispatcher.Run(ctx, job)

	if err != nil {
		job.Error = err.Error()
		w.handleFailedJob(job, err)
		return
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	w.Repo.Save(job)

	log.Println("worker", workerID, "completed job", job.ID)
}

func (w *Worker) acquireTypeSlot(workerID int, job Job) {
	limit, exists := w.TypeLimits[job.Type]
	if !exists {
		return
	}

	log.Println("worker", workerID, "waiting for", job.Type, "slot for job", job.ID)
	limit <- struct{}{}
	log.Println("worker", workerID, "acquired", job.Type, "slot for job", job.ID)
}

func (w *Worker) releaseTypeSlot(job Job) {
	limit, exists := w.TypeLimits[job.Type]
	if !exists {
		return
	}

	<-limit
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
