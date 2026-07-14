package jobs

import "testing"

func TestMemoryQueueReturnsJobsByPriority(t *testing.T) {
	queue := NewMemoryQueue(3)

	queue.Enqueue(Job{ID: 1, Priority: JobPriorityLow})
	queue.Enqueue(Job{ID: 2, Priority: JobPriorityMedium})
	queue.Enqueue(Job{ID: 3, Priority: JobPriorityHigh})

	expectedOrder := []int{3, 2, 1}
	for _, expectedID := range expectedOrder {
		jobID := queue.NextJob()
		if jobID != expectedID {
			t.Fatalf("expected job %d, got job %d", expectedID, jobID)
		}
	}
}
