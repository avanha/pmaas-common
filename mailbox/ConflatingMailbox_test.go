package mailbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avanha/pmaas-common/mailbox"
)

func TestConflatingMailbox_RunsSentClosure(t *testing.T) {
	cm := mailbox.NewConflatingMailbox()
	defer cm.Stop(context.Background())

	var executed atomic.Bool
	done := make(chan struct{})
	err := cm.Send(func() {
		executed.Store(true)
		close(done)
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for closure to run")
	}

	if !executed.Load() {
		t.Fatalf("expected closure to have executed")
	}
}

func TestConflatingMailbox_DiscardsSupersededPending(t *testing.T) {
	cm := mailbox.NewConflatingMailbox()
	defer cm.Stop(context.Background())

	// Occupy the worker with a long-running first closure so subsequent Sends queue up instead of
	// running immediately.
	startedFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	err := cm.Send(func() {
		close(startedFirst)
		<-releaseFirst
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	<-startedFirst

	var supersededRan, latestRan atomic.Bool

	// This one should be discarded once the next Send replaces it.
	if err := cm.Send(func() { supersededRan.Store(true) }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	done := make(chan struct{})
	if err := cm.Send(func() {
		latestRan.Store(true)
		close(done)
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	close(releaseFirst)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for latest closure to run")
	}

	if supersededRan.Load() {
		t.Fatalf("expected superseded closure to have been discarded, but it ran")
	}

	if !latestRan.Load() {
		t.Fatalf("expected latest closure to have run")
	}
}

func TestConflatingMailbox_Send_ClosedReturnsError(t *testing.T) {
	cm := mailbox.NewConflatingMailbox()
	cm.Stop(context.Background())

	err := cm.Send(func() {
		t.Fatalf("should not execute after Stop")
	})

	if err == nil {
		t.Fatalf("expected error sending to a closed mailbox, got nil")
	}
}

func TestConflatingMailbox_Stop_RunsFinalPendingItem(t *testing.T) {
	cm := mailbox.NewConflatingMailbox()

	var ran atomic.Bool
	err := cm.Send(func() { ran.Store(true) })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := cm.Stop(context.Background()); err != nil {
		t.Fatalf("unexpected error from Stop: %v", err)
	}

	if !ran.Load() {
		t.Fatalf("expected the last pending closure to run before Stop returned")
	}
}

func TestConflatingMailbox_Stop_TimesOut(t *testing.T) {
	cm := mailbox.NewConflatingMailbox()

	release := make(chan struct{})
	started := make(chan struct{})
	err := cm.Send(func() {
		close(started)
		<-release
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err = cm.Stop(ctx)
	if err == nil {
		t.Fatalf("expected Stop to time out while a closure is still running")
	}

	close(release)
}
