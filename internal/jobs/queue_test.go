package jobs

import (
	"context"
	"testing"
)

func TestMemoryQueueReturnsJobsByPriority(t *testing.T) {
	queue := NewMemoryQueue(3)

	ctx := context.Background()
	queue.Enqueue(ctx, Job{ID: 1, Priority: JobPriorityLow})
	queue.Enqueue(ctx, Job{ID: 2, Priority: JobPriorityMedium})
	queue.Enqueue(ctx, Job{ID: 3, Priority: JobPriorityHigh})

	expectedOrder := []int{3, 2, 1}
	for _, expectedID := range expectedOrder {
		delivery, err := queue.NextJob(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if delivery.JobID != expectedID {
			t.Fatalf("expected job %d, got job %d", expectedID, delivery.JobID)
		}
	}
}
