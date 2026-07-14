package jobs

import (
	"context"
	"testing"
	"time"
)

func TestDueJobsIncludesFailedRetry(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	job, err := repo.Create(ctx, Job{Type: JobType("email")})
	if err != nil {
		t.Fatal(err)
	}

	retryAt := time.Now().Add(-time.Second)
	job.Status = JobStatusFailed
	job.NextRetryAt = &retryAt
	if err := repo.Save(ctx, job); err != nil {
		t.Fatal(err)
	}

	dueJobs, err := repo.DueJobs(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	if len(dueJobs) != 1 || dueJobs[0].ID != job.ID {
		t.Fatalf("expected retry job %d to be due", job.ID)
	}
}
