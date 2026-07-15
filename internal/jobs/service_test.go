package jobs

import (
	"context"
	"testing"
)

func TestSubmitJobIsIdempotent(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository()
	queue := NewMemoryQueue(2)
	worker := NewWorker(repo, NewDispatcher(), queue)
	service := NewService(repo, worker)

	input := SubmitJobInput{
		IdempotencyKey: "request-1",
		Type:           JobType("email"),
	}
	first, err := service.SubmitJob(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.SubmitJob(ctx, input)
	if err != nil {
		t.Fatal(err)
	}

	if first.ID != second.ID {
		t.Fatalf("expected the same job, got %d and %d", first.ID, second.ID)
	}
	if len(queue.MediumQueue) != 1 {
		t.Fatalf("expected one queued message, got %d", len(queue.MediumQueue))
	}
}
