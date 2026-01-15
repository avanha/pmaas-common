package queue

import (
	"testing"
	"testing/synctest"
)

type Request struct {
	Name string
}

func TestRequestQueue_EnqueueAndRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Create a destination channel with limited capacity
		dest := make(chan Request, 1)
		q := NewRequestQueue[Request](dest)

		req1 := Request{Name: "Request1"}
		req2 := Request{Name: "Request2"}

		// Enqueue two requests
		if err := q.Enqueue(&req1); err != nil {
			t.Fatal(err)
		}

		if err := q.Enqueue(&req2); err != nil {
			t.Fatal(err)
		}

		if len(q.requests) != 2 {
			t.Errorf("Expected 2 requests in queue, got %d", len(q.requests))
		}

		// Run in a goroutine within the synctest bubble
		go q.Run()

		// Wait for the worker to process the first item
		synctest.Wait()

		// Verify that the first request is received
		select {
		case r := <-dest:
			if r.Name != "Request1" {
				t.Errorf("Expected Request1, got %s", r.Name)
			}
		default:
			t.Fatal("Expected req1 to be available in dest")
		}

		// Wait for the worker to process the second item
		synctest.Wait()

		// Verify that the second request is received
		select {
		case r := <-dest:
			if r.Name != "Request2" {
				t.Errorf("Expected Request2, got %s", r.Name)
			}
		default:
			t.Fatal("Expected req2 to be available in dest")
		}

		q.Stop()
	})
}

func TestRequestQueue_StopWithPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dest := make(chan Request) // Unbuffered
		q := NewRequestQueue[Request](dest)

		if err := q.Enqueue(&Request{Name: "Request1"}); err != nil {
			t.Fatal(err)
		}

		q.Stop()

		done := make(chan bool)
		go func() {
			q.Run()
			done <- true
		}()

		// The request should still be processed before Run returns
		// synctest.Wait() ensures the goroutine above has a chance to reach the send operation
		synctest.Wait()

		select {
		case <-dest:
			// success
		default:
			t.Error("Request was not processed after Stop")
		}

		// Wait for the Run() goroutine to finish and send to done
		synctest.Wait()

		select {
		case <-done:
			// success
		default:
			t.Error("Run() did not exit after processing pending items")
		}
	})
}
