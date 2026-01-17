package queue

import "time"

type QueueStats struct {
	CurrentCount  int
	PeakCount     int
	PeakCountTime time.Time
}
