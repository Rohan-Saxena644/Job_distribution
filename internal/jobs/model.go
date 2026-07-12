package jobs

import "time"

type JobType string

type JobStatus string

const (
	JobStatusQueued     JobStatus = "queued"
	JobStatusRunning    JobStatus = "running"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
	JobStatusDeadLetter JobStatus = "dead_letter"
)

type JobPriority string

const (
	JobPriorityLow    JobPriority = "low"
	JobPriorityMedium JobPriority = "medium"
	JobPriorityHigh   JobPriority = "high"
)

type Job struct {
	ID          int
	Type        JobType
	Payload     map[string]string
	Status      JobStatus
	Priority    JobPriority
	ScheduledAt *time.Time
	Enqueued    bool
	MaxRetries  int
	Attempts    int
	Error       string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type SubmitJobInput struct {
	Type        JobType
	Payload     map[string]string
	Priority    JobPriority
	ScheduledAt *time.Time
	MaxRetries  int
}
