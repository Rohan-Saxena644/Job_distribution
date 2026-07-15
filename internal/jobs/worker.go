package jobs

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"time"
)

type Worker struct {
	Repo       JobRepository
	Dispatcher *Dispatcher
	Queue      JobQueue
	Events     EventPublisher
	RetryBase  time.Duration
	RetryMax   time.Duration
	TypeLimits map[JobType]chan struct{}
}

func NewWorker(repo JobRepository, dispatcher *Dispatcher, queue JobQueue) *Worker {
	return &Worker{
		Repo:       repo,
		Dispatcher: dispatcher,
		Queue:      queue,
		Events:     &NoopEventPublisher{},
		RetryBase:  time.Second,
		RetryMax:   30 * time.Second,
		TypeLimits: make(map[JobType]chan struct{}),
	}
}

func (w *Worker) SetEventPublisher(publisher EventPublisher) {
	w.Events = publisher
}

func (w *Worker) SetConcurrencyLimit(jobType JobType, limit int) {
	if limit < 1 {
		return
	}

	w.TypeLimits[jobType] = make(chan struct{}, limit)
}

func (w *Worker) Enqueue(ctx context.Context, job Job) error {
	if err := w.Queue.Enqueue(ctx, job); err != nil {
		return err
	}

	return w.Repo.MarkEnqueued(ctx, job.ID)
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

		err = w.process(ctx, workerID, delivery.JobID, delivery.Redelivered)
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
	return w.process(ctx, workerID, jobID, false)
}

func (w *Worker) process(ctx context.Context, workerID int, jobID int, allowRunning bool) error {
	job, claimed, err := w.Repo.Claim(ctx, jobID, allowRunning)
	if err != nil {
		return err
	}
	if !claimed {
		log.Println("worker", workerID, "skipping already claimed or finished job", jobID)
		return nil
	}

	w.acquireTypeSlot(workerID, job)
	defer w.releaseTypeSlot(job)
	w.publishEvent(ctx, "job.started", job)

	log.Println("worker", workerID, "processing job", job.ID, "type:", job.Type, "priority:", job.Priority, "attempt:", job.Attempts)

	if err := w.Dispatcher.Run(ctx, job); err != nil {
		job.Error = err.Error()
		return w.handleFailedJob(ctx, job, err)
	}

	job.Status = JobStatusCompleted
	job.Error = ""
	job.NextRetryAt = nil
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}
	w.publishEvent(ctx, "job.completed", job)

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
		delay := w.retryDelay(job.Attempts)
		retryAt := time.Now().Add(delay)

		job.Status = JobStatusFailed
		job.NextRetryAt = &retryAt
		if err := w.Repo.Save(ctx, job); err != nil {
			return err
		}
		w.publishEvent(ctx, "job.failed", job)

		log.Println("job failed", job.ID, handlerErr)
		log.Println("retry scheduled for job", job.ID, "after", delay)

		return nil
	}

	job.Status = JobStatusDeadLetter
	job.NextRetryAt = nil
	if err := w.Repo.Save(ctx, job); err != nil {
		return err
	}
	w.publishEvent(ctx, "job.dead_lettered", job)

	log.Println("job moved to dead letter", job.ID, handlerErr)
	return nil
}

func (w *Worker) retryDelay(attempt int) time.Duration {
	delay := w.RetryBase

	for currentAttempt := 1; currentAttempt < attempt; currentAttempt++ {
		if delay >= w.RetryMax/2 {
			delay = w.RetryMax
			break
		}
		delay *= 2
	}

	jitterLimit := delay / 4
	if delay < w.RetryMax && jitterLimit > 0 {
		jitter := time.Duration(rand.Int63n(int64(jitterLimit)))
		delay += jitter
	}

	if delay > w.RetryMax {
		return w.RetryMax
	}

	return delay
}

func (w *Worker) publishEvent(ctx context.Context, name string, job Job) {
	event := JobEvent{
		Name:       name,
		JobID:      job.ID,
		Type:       job.Type,
		Status:     job.Status,
		Attempts:   job.Attempts,
		Error:      job.Error,
		OccurredAt: time.Now(),
	}

	if err := w.Events.Publish(ctx, event); err != nil {
		log.Println("could not publish", name, "for job", job.ID, err)
	}
}
