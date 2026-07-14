package jobs

import "context"

type JobDelivery struct {
	JobID int
	ack   func() error
	nack  func() error
}

func (d JobDelivery) Ack() error {
	return d.ack()
}

func (d JobDelivery) Nack() error {
	return d.nack()
}

type JobQueue interface {
	Enqueue(ctx context.Context, job Job) error
	NextJob(ctx context.Context) (JobDelivery, error)
	Close() error
}

type MemoryQueue struct {
	HighQueue   chan int
	MediumQueue chan int
	LowQueue    chan int
}

func NewMemoryQueue(size int) *MemoryQueue {
	return &MemoryQueue{
		HighQueue:   make(chan int, size),
		MediumQueue: make(chan int, size),
		LowQueue:    make(chan int, size),
	}
}

func (q *MemoryQueue) Enqueue(ctx context.Context, job Job) error {
	var targetQueue chan int

	switch job.Priority {
	case JobPriorityHigh:
		targetQueue = q.HighQueue
	case JobPriorityLow:
		targetQueue = q.LowQueue
	default:
		targetQueue = q.MediumQueue
	}

	select {
	case targetQueue <- job.ID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) NextJob(ctx context.Context) (JobDelivery, error) {
	for {
		select {
		case jobID := <-q.HighQueue:
			return memoryDelivery(jobID), nil
		default:
		}

		select {
		case jobID := <-q.MediumQueue:
			return memoryDelivery(jobID), nil
		default:
		}

		select {
		case jobID := <-q.LowQueue:
			return memoryDelivery(jobID), nil
		default:
		}

		select {
		case jobID := <-q.HighQueue:
			return memoryDelivery(jobID), nil
		case jobID := <-q.MediumQueue:
			return memoryDelivery(jobID), nil
		case jobID := <-q.LowQueue:
			return memoryDelivery(jobID), nil
		case <-ctx.Done():
			return JobDelivery{}, ctx.Err()
		}
	}
}

func (q *MemoryQueue) Close() error {
	return nil
}

func memoryDelivery(jobID int) JobDelivery {
	return JobDelivery{
		JobID: jobID,
		ack:   func() error { return nil },
		nack:  func() error { return nil },
	}
}
