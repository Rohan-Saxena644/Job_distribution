package jobs

type JobQueue interface {
	Enqueue(job Job)
	NextJob() int
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

func (q *MemoryQueue) Enqueue(job Job) {
	switch job.Priority {
	case JobPriorityHigh:
		q.HighQueue <- job.ID
	case JobPriorityLow:
		q.LowQueue <- job.ID
	default:
		q.MediumQueue <- job.ID
	}
}

func (q *MemoryQueue) NextJob() int {
	for {
		select {
		case jobID := <-q.HighQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-q.MediumQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-q.LowQueue:
			return jobID
		default:
		}

		select {
		case jobID := <-q.HighQueue:
			return jobID
		case jobID := <-q.MediumQueue:
			return jobID
		case jobID := <-q.LowQueue:
			return jobID
		}
	}
}
