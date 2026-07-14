package jobs

import (
	"testing"
	"time"
)

func TestRetryDelayGrowsWithAttempts(t *testing.T) {
	worker := NewWorker(nil, nil, nil)

	firstDelay := worker.retryDelay(1)
	thirdDelay := worker.retryDelay(3)

	if firstDelay < time.Second || firstDelay >= 1250*time.Millisecond {
		t.Fatalf("unexpected first retry delay: %s", firstDelay)
	}

	if thirdDelay < 4*time.Second || thirdDelay >= 5*time.Second {
		t.Fatalf("unexpected third retry delay: %s", thirdDelay)
	}
}
