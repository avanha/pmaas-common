package mailbox

import (
	"testing"
	"time"
)

func TestOutbox_RunsInPostingOrder(t *testing.T) {
	o := NewOutbox()
	var got []int

	for i := 0; i < 100; i++ {
		if err := o.Post(func() { got = append(got, i) }); err != nil {
			t.Fatal(err)
		}
	}

	o.Stop()

	if len(got) != 100 {
		t.Fatalf("expected 100 closures run, got %d", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("out of order at %d: %v", i, got)
		}
	}
}

func TestOutbox_PostNeverBlocksWhileAClosureIsBlocked(t *testing.T) {
	o := NewOutbox()
	release := make(chan struct{})
	ran := make(chan struct{})

	if err := o.Post(func() { <-release }); err != nil {
		t.Fatal(err)
	}

	posted := make(chan struct{})
	go func() {
		defer close(posted)
		for i := 0; i < 1000; i++ {
			if err := o.Post(func() {}); err != nil {
				t.Error(err)
			}
		}
		_ = o.Post(func() { close(ran) })
	}()

	select {
	case <-posted:
	case <-time.After(5 * time.Second):
		t.Fatal("Post blocked while an earlier closure was blocked")
	}

	close(release)

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("queued closures did not run once the blocked one finished")
	}

	o.Stop()
}

func TestOutbox_StopDrainsThenRejectsPosts(t *testing.T) {
	o := NewOutbox()
	ran := false

	if err := o.Post(func() { time.Sleep(10 * time.Millisecond); ran = true }); err != nil {
		t.Fatal(err)
	}

	o.Stop()

	if !ran {
		t.Fatal("Stop returned before the pending closure ran")
	}
	if err := o.Post(func() {}); err == nil {
		t.Fatal("expected Post after Stop to fail")
	}

	o.Stop() // second Stop is a no-op
}
