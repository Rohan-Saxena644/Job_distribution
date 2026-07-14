package jobs

import (
	"context"
	"errors"
	"log"
	"time"
)

type Worker struct {
	Repo       JobRepository
	Dispatcher *Dispatcher
	Queue      JobQueue
	RetryDelay time.Duration
	TypeLimits map[JobType]chan struct{}
}

func NewWorker(repo JobRepository, dispatcher *Dispatcher, queue JobQueue) *Worker {
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

func (w *Worker) Enqueue(ctx context.Context, job Job) error {
	job.Enqueued = true
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}

	if err := w.Queue.Enqueue(ctx, job); err != nil {
		job.Enqueued = false
		_ = w.Repo.Save(ctx, job)
		return err
	}

	return nil
}

func (w *Worker) Start(ctx context.Context, workerID int) {
	log.Println("worker started", workerID)

	for {
		delivery, err := w.Queue.NextJob(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			log.Println("worker", workerID, "queue error:", err)
			time.Sleep(time.Second)
			continue
		}

		err = w.Process(ctx, workerID, delivery.JobID)
		if err != nil {
			log.Println("worker", workerID, "could not process job", delivery.JobID, err)

			if errors.Is(err, ErrJobNotFound) {
				_ = delivery.Ack()
				continue
			}

			if nackErr := delivery.Nack(); nackErr != nil {
				log.Println("worker", workerID, "could not requeue job", delivery.JobID, nackErr)
			}
			continue
		}

		if err := delivery.Ack(); err != nil {
			log.Println("worker", workerID, "could not acknowledge job", delivery.JobID, err)
		}
	}
}

func (w *Worker) Process(ctx context.Context, workerID int, jobID int) error {
	job, err := w.Repo.Get(ctx, jobID)
	if err != nil {
		return err
	}

	if job.Status == JobStatusCompleted || job.Status == JobStatusDeadLetter {
		log.Println("worker", workerID, "skipping finished job", job.ID, "status:", job.Status)
		return nil
	}

	w.acquireTypeSlot(workerID, job)
	defer w.releaseTypeSlot(job)

	job.Enqueued = false
	job.Status = JobStatusRunning
	job.Attempts++
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}

	log.Println("worker", workerID, "processing job", job.ID, "type:", job.Type, "priority:", job.Priority, "attempt:", job.Attempts)

	if err := w.Dispatcher.Run(ctx, job); err != nil {
		job.Error = err.Error()
		return w.handleFailedJob(ctx, job, err)
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}

	log.Println("worker", workerID, "completed job", job.ID)
	return nil
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

func (w *Worker) handleFailedJob(ctx context.Context, job Job, handlerErr error) error {
	if job.Attempts <= job.MaxRetries {
		job.Status = JobStatusFailed
		if err := w.Repo.Save(ctx, job); err != nil {
			return err
		}

		log.Println("job failed", job.ID, handlerErr)
		log.Println("retrying job", job.ID, "after", w.RetryDelay)

		go func() {
			time.Sleep(w.RetryDelay)
			if err := w.Enqueue(context.Background(), job); err != nil {
				log.Println("could not retry job", job.ID, err)
			}
		}()

		return nil
	}

	job.Status = JobStatusDeadLetter
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}

	log.Println("job moved to dead letter", job.ID, handlerErr)
	return nil
}
