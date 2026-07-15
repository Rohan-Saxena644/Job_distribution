package jobs

import (
	"context"
	"testing"
	"time"
)

func TestDueJobsIncludesFailedRetry(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	job, _, err := repo.Create(ctx, Job{Type: JobType("email")})
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

func TestIdempotencyKeyReturnsExistingJob(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	first, created, err := repo.Create(ctx, Job{Type: JobType("email"), IdempotencyKey: "request-1"})
	if err != nil || !created {
		t.Fatalf("expected first job to be created: %v", err)
	}

	second, created, err := repo.Create(ctx, Job{Type: JobType("email"), IdempotencyKey: "request-1"})
	if err != nil || created {
		t.Fatalf("expected duplicate job to be reused: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected job %d, got %d", first.ID, second.ID)
	}
}

func TestClaimAllowsOnlyOneWorker(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()

	job, _, err := repo.Create(ctx, Job{Type: JobType("email")})
	if err != nil {
		t.Fatal(err)
	}

	claimedJob, claimed, err := repo.Claim(ctx, job.ID, false)
	if err != nil || !claimed {
		t.Fatalf("expected job to be claimed: %v", err)
	}
	if claimedJob.Status != JobStatusRunning || claimedJob.Attempts != 1 {
		t.Fatalf("unexpected claimed job: %+v", claimedJob)
	}

	_, claimed, err = repo.Claim(ctx, job.ID, false)
	if err != nil || claimed {
		t.Fatalf("expected second claim to be skipped: %v", err)
	}
}
