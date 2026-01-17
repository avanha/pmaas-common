package queue

import (
	"fmt"
	"sync"
	"time"
)

// RequestQueue accepts requests and dispatches them to a destination
// channel when the channel has capacity.  The queue stores requests
// in the requests slice when there is no capacity.  The Run method
// continues to run until running is false and the requests slice is empty.
type RequestQueue[T any] struct {
	requests                 []T
	destination              chan T
	running                  bool
	mu                       sync.Mutex
	requestNotEmptyCondition *sync.Cond
	peakCount                int
	peakCountTime            time.Time
}

func NewRequestQueue[T any](destination chan T) *RequestQueue[T] {
	q := &RequestQueue[T]{
		requests:    make([]T, 0, 10),
		destination: destination,
		running:     true,
	}
	q.requestNotEmptyCondition = sync.NewCond(&q.mu)
	return q
}

func (q *RequestQueue[T]) Enqueue(request *T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.running {
		return fmt.Errorf("queue is not running")
	}

	q.requests = append(q.requests, *request)
	currentCount := len(q.requests)

	if currentCount > q.peakCount {
		q.peakCount = currentCount
		q.peakCountTime = time.Now()
	}

	q.requestNotEmptyCondition.Signal()

	return nil
}

func (q *RequestQueue[T]) Run() {
	for {
		q.mu.Lock()
		// Wait while we are running and there are no requests
		for q.running && len(q.requests) == 0 {
			q.requestNotEmptyCondition.Wait()
		}

		// Exit condition: stopped and no more requests to process
		if !q.running && len(q.requests) == 0 {
			q.mu.Unlock()
			close(q.destination)
			return
		}

		// Get the next request
		req := q.requests[0]
		q.requests = q.requests[1:]
		q.mu.Unlock()

		// Dispatch to destination. This may block if destination is full.
		// Note: We unlock before sending to avoid holding the lock during blocking I/O.
		q.destination <- req
	}
}

func (q *RequestQueue[T]) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.running = false
	q.requestNotEmptyCondition.Broadcast()
}

func (q *RequestQueue[T]) Stats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	return QueueStats{CurrentCount: len(q.requests), PeakCount: q.peakCount, PeakCountTime: q.peakCountTime}
}
