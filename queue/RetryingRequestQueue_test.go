package queue

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type RetryingRequest struct {
	Name       string
	ResponseCh chan RetryingResponse
}

type RetryingResponse struct {
	Err error
}

func TestRetryingRequestQueue_RetryLogic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dest := make(chan RetryingRequest)
		inner := NewRequestQueue[RetryingRequest](dest)
		retrying := NewRetryingRequestQueue[RetryingRequest, RetryingResponse](
			func(r *RetryingRequest) chan RetryingResponse {
				return r.ResponseCh
			},
			func(request *RetryingRequest, newResponseCh chan RetryingResponse) chan RetryingResponse {
				old := request.ResponseCh
				request.ResponseCh = newResponseCh

				return old
			},
			func(err error) RetryingResponse {
				return RetryingResponse{Err: err}
			},
			func(response *RetryingResponse) bool {
				return response.Err != nil
			},
			func(request *RetryingRequest, response *RetryingResponse, attempt int, lastTry time.Time) bool {
				return attempt < 2 // Retry once
			},
			inner)

		innerDoneCh := make(chan struct{})
		retryingDoneCh := make(chan struct{})

		go func() {
			inner.Run()
			close(innerDoneCh)
		}()

		go func() {
			retrying.Run()
			close(retryingDoneCh)
		}()

		request := RetryingRequest{Name: "RetryMe", ResponseCh: make(chan RetryingResponse)}
		err := retrying.Enqueue(&request)

		if err != nil {
			t.Fatal(err)
		}

		// 1. First attempt: Simulate failure
		synctest.Wait()

		select {
		case r := <-dest:
			if r.Name != "RetryMe" {
				t.Fatalf("Expected RetryMe, got %s", r.Name)
			}
			// Send error response back
			r.ResponseCh <- RetryingResponse{Err: errors.New("temporary failure")}
		default:
			t.Fatal("Request wat not enqueued on dest channel")
		}

		// 2. Advance time past retry delay
		time.Sleep(130 * time.Second)
		synctest.Wait()

		// 3. Second attempt (the retry)
		select {
		case r := <-dest:
			if r.Name != "RetryMe" {
				t.Fatalf("Expected RetryMe on retry, got %s", r.Name)
			}
			// Send success response
			r.ResponseCh <- RetryingResponse{Err: nil}
		default:
			t.Fatal("Request wat not enqueued on dest channel")
		}

		synctest.Wait()

		// Verify success response was received by the sender

		select {
		case r := <-request.ResponseCh:
			if r.Err != nil {
				t.Fatalf("Expected success response, got error: %v", r.Err)
			}
		default:
			t.Fatal("Expected success response on request channel")
		}

		retrying.Stop()
		inner.Stop()

		synctest.Wait()

		select {
		case <-retryingDoneCh:
			break
		default:
			t.Fatal("Retrying queue did not stop")
		}

		select {
		case <-innerDoneCh:
			break
		default:
			t.Fatal("Inner queue did not stop")
		}
	})
}

/*
func TestRetryingRequestQueue_MaxRetriesReached(t *testing.T) {
	synctest.Test(t, func() {

		dest := make(chan Request, 1)
		inner := NewRequestQueue[Request](dest)

		q := &RetryingRequestQueue[Request, TestResponse]{
			getResponseChannelFn:      func(r *Request) chan TestResponse { return make(chan TestResponse, 1) },
			exchangeResponseChannelFn: func(r *Request, ch chan TestResponse) chan TestResponse { return ch },
			createErrorResponseFn:     func(err error) TestResponse { return TestResponse{Err: err} },
			isErrorFn:                 func(resp *TestResponse) bool { return resp.Err != nil },
			canRetryFn: func(req *Request, resp *TestResponse, attempt int, lastTry time.Time) bool {
				return false // No retries
			},
			innerQueue: inner,
			ctx:        ctx,
		}

		go inner.Run()

		req := Request{Name: "FailFast"}
		respCh := q.getResponseChannelFn(&req)
		q.Enqueue(req)

		synctest.Wait()
		<-dest
		respCh <- TestResponse{Err: errors.New("permanent failure")}

		synctest.Wait()
		// Verify no second attempt is made
		select {
		case <-dest:
			t.Error("Request was retried but should not have been")
		default:
			// Success: no retry enqueued
		}

		inner.Stop()
	})
}
*/
