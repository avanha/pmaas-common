package queue

import (
	"testing"
	"time"
)

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUnboundedChannel_HighWatermark_TracksLargestBufferedCount(t *testing.T) {
	uc := NewUnboundedChannel[int]()
	defer uc.Close()

	if hwm := uc.HighWatermark(); hwm != 0 {
		t.Fatalf("expected initial high watermark 0, got %d", hwm)
	}

	// Push several items without reading them, so they accumulate in the
	// internal buffer.
	for i := 1; i <= 5; i++ {
		uc.In() <- i
		waitForCondition(t, time.Second, func() bool { return uc.HighWatermark() >= int64(i) })
	}

	if hwm := uc.HighWatermark(); hwm != 5 {
		t.Fatalf("expected high watermark 5, got %d", hwm)
	}

	// Drain the buffer; the high watermark must not decrease.
	for i := 1; i <= 5; i++ {
		if v := <-uc.Out(); v != i {
			t.Fatalf("expected %d, got %d", i, v)
		}
	}

	if hwm := uc.HighWatermark(); hwm != 5 {
		t.Fatalf("expected high watermark to remain 5 after draining, got %d", hwm)
	}

	// Pushing fewer items than the previous peak must not lower the watermark.
	uc.In() <- 100
	if v := <-uc.Out(); v != 100 {
		t.Fatalf("expected 100, got %d", v)
	}

	if hwm := uc.HighWatermark(); hwm != 5 {
		t.Fatalf("expected high watermark to remain 5, got %d", hwm)
	}
}
