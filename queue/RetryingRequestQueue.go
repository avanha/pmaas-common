package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type RetryingQueueStats struct {
	QueueStats
	PeakFailedAttempts     int
	PeakFailedAttemptsTime time.Time
}

type RequestWrapper[T any] struct {
	Request          T
	attemptCount     int
	firstAttemptTime time.Time
	lastAttemptTime  time.Time
}

// RetryingRequestQueue is a wrapper around a regular queue that can queue requests for retry when they
// fail with specific errors.  T is the request type and R is the response type.  Create the RetryingRequestQueue with
// functions for the following tasks:
// getResponseChannelFn A function that obtains the response channel from the request.  This is used to read the
// response so we can check for failures.
// exchangeResponseChannelFn A function that can swap the response channel in the request.  Used to intercept the
// response.
// createErrorResponseFn Factory for error responses when the RetryingRequestQueue itself needs to fail requests.
// isErrorFn Checks whether the response read from the response chanel is an error.
// canRetryFn Checks whether request can be retried after the error.
type RetryingRequestQueue[T any, R any] struct {
	getResponseChannelFn      func(*T) chan R
	exchangeResponseChannelFn func(*T, chan R) chan R
	createErrorResponseFn     func(error) R
	isErrorFn                 func(response *R) bool
	canRetryFn                func(*T, *R, int, time.Time) bool
	innerQueue                *RequestQueue[T]
	retryQueue                []RequestWrapper[T]
	mutex                     sync.Mutex
	ctx                       context.Context
	cancelFn                  context.CancelFunc
	peakCount                 int
	peakCountTime             time.Time
	peakFailedAttempts        int
	peakFailedAttemptsTime    time.Time
}

func NewRetryingRequestQueue[T any, R any](
	getResponseChannelFn func(*T) chan R,
	exchangeResponseChannelFn func(*T, chan R) chan R,
	createErrorResponseFn func(error) R,
	isErrorFn func(*R) bool,
	canRetryFn func(*T, *R, int, time.Time) bool,
	innerQueue *RequestQueue[T]) *RetryingRequestQueue[T, R] {
	queue := &RetryingRequestQueue[T, R]{
		getResponseChannelFn:      getResponseChannelFn,
		exchangeResponseChannelFn: exchangeResponseChannelFn,
		createErrorResponseFn:     createErrorResponseFn,
		isErrorFn:                 isErrorFn,
		canRetryFn:                canRetryFn,
		innerQueue:                innerQueue,
		retryQueue:                make([]RequestWrapper[T], 0, 10),
	}
	queue.ctx, queue.cancelFn = context.WithCancel(context.Background())

	return queue
}

func (q *RetryingRequestQueue[T, R]) Enqueue(request *T) error {
	wrapper := RequestWrapper[T]{
		Request:          *request,
		attemptCount:     0,
		firstAttemptTime: time.Now(),
	}
	return q.internalEnqueue(&wrapper)
}

func (q *RetryingRequestQueue[T, R]) internalEnqueue(wrapper *RequestWrapper[T]) error {
	now := time.Now()
	ourResponseChannel := make(chan R)
	originalResponseCh := q.exchangeResponseChannelFn(&wrapper.Request, ourResponseChannel)
	wrapper.attemptCount++
	wrapper.lastAttemptTime = now

	err := q.innerQueue.Enqueue(&wrapper.Request)

	if err != nil {
		return err
	}

	go func() {
		// Get the response and check whether it's an error
		response := <-ourResponseChannel
		isError := q.isErrorFn(&response)

		if isError && q.canRetryFn(&wrapper.Request, &response, wrapper.attemptCount, wrapper.lastAttemptTime) {
			enqueueRetryErr := q.enqueueForRetry(wrapper, originalResponseCh)

			if enqueueRetryErr == nil {
				return
			}
		}

		// We can't retry so transfer the response now if the original request has a response channel
		if originalResponseCh != nil {
			originalResponseCh <- response
			close(originalResponseCh)
		}
	}()

	return nil
}

func (q *RetryingRequestQueue[T, R]) Run() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	run := true

	for run {
		select {
		case <-ticker.C:
			q.doRetry()
		case <-q.ctx.Done():
			run = false
		}
	}

	q.cancelPending()
}

func (q *RetryingRequestQueue[T, R]) enqueueForRetry(
	wrapper *RequestWrapper[T],
	originalResponseCh chan R) error {
	q.exchangeResponseChannelFn(&wrapper.Request, originalResponseCh)

	// Acquire the mutex since we're manipulating state from the goroutine that reads the response channel
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.retryQueue = append(q.retryQueue, *wrapper)
	currentCount := len(q.retryQueue)

	if currentCount > q.peakCount {
		q.peakCount = currentCount
		q.peakCountTime = time.Now()
	}

	if wrapper.attemptCount > q.peakFailedAttempts {
		q.peakFailedAttempts = wrapper.attemptCount
		q.peakFailedAttemptsTime = time.Now()
	}

	return nil
}

func (q *RetryingRequestQueue[T, R]) doRetry() {
	q.mutex.Lock()
	now := time.Now()
	toRetry := make([]RequestWrapper[T], 0, len(q.retryQueue))

	for i := 0; i < len(q.retryQueue); {
		if q.shouldRetryNow(&q.retryQueue[i], now) {
			toRetry = append(toRetry, q.retryQueue[i])
			q.retryQueue = append(q.retryQueue[:i], q.retryQueue[i+1:]...)
		} else {
			i++
		}
	}

	q.mutex.Unlock()

	for i := 0; i < len(toRetry); i++ {
		err := q.internalEnqueue(&toRetry[i])
		if err != nil {
			q.cancelRequest(&toRetry[i])
		}
	}
}

func (q *RetryingRequestQueue[T, R]) shouldRetryNow(request *RequestWrapper[T], now time.Time) bool {
	threshold := now.Add(-60 * time.Second)
	return request.lastAttemptTime == threshold || request.lastAttemptTime.Before(threshold)
}

func (q *RetryingRequestQueue[T, R]) cancelRequest(wrapper *RequestWrapper[T]) {
	responseCh := q.getResponseChannelFn(&wrapper.Request)

	if responseCh != nil {
		errorResponse := q.createErrorResponseFn(fmt.Errorf("request canceled"))
		responseCh <- errorResponse
		close(responseCh)
	}
}

func (q *RetryingRequestQueue[T, R]) Stop() {
	// Cancel the context so run terminates immediately
	q.cancelFn()
}

func (q *RetryingRequestQueue[T, R]) cancelPending() {
	for {
		q.mutex.Lock()
		itemCount := len(q.retryQueue)

		if itemCount == 0 {
			fmt.Printf("%T No more pending requests to cancel\n", q)
			q.mutex.Unlock()
			return
		}

		// Get the next request
		req := q.retryQueue[0]
		q.retryQueue = q.retryQueue[1:]
		q.mutex.Unlock()

		// Unlock first, so we don't hold the lock while writing to a channel
		q.cancelRequest(&req)
	}
}

func (q *RetryingRequestQueue[T, R]) Stats() RetryingQueueStats {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return RetryingQueueStats{
		QueueStats: QueueStats{
			CurrentCount:  len(q.retryQueue),
			PeakCount:     q.peakCount,
			PeakCountTime: q.peakCountTime,
		},
		PeakFailedAttempts:     q.peakFailedAttempts,
		PeakFailedAttemptsTime: q.peakFailedAttemptsTime,
	}
}
