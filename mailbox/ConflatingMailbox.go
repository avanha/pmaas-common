package mailbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ConflatingMailbox delivers work to a single owning goroutine, like Mailbox, but with different
// queueing semantics: it holds at most one not-yet-started closure at a time. Sending a new one while
// an earlier one is still queued (but hasn't started running) discards the earlier one — it never
// runs. A closure that has already started running is never interrupted; a Send arriving while one is
// in flight simply becomes the next one up.
//
// This fits work where only the most recent state is worth persisting, such as periodically flushing
// the latest value of something to disk: intermediate values that are superseded before they're ever
// written don't need to be written at all.
//
// Concurrent Sends are safe (nothing races or corrupts), but which one ends up as the surviving
// pending item is unspecified if two calls race with no happens-before relationship between them —
// only "at most one pending closure, superseded ones discarded" is guaranteed, not "whichever call
// happened later in wall-clock time always wins". Callers that need the latter should only ever call
// Send from a single goroutine (e.g. from within another actor's own mailbox), so calls are already
// totally ordered before they reach ConflatingMailbox.
type ConflatingMailbox struct {
	// pendingCh holds at most one not-yet-started closure. Send deposits into it, replacing whatever
	// (if anything) was already there; run consumes from it.
	pendingCh chan func()

	// A channel that is closed to signal that no further sends will be accepted. Checked by senders.
	closedCh chan struct{}

	// A WaitGroup that counts in-flight Send calls, so Stop knows once none can possibly still be
	// depositing into pendingCh before it tells run it's safe to do its final check-and-exit.
	sendOps sync.WaitGroup

	// A channel closed only after sendOps has drained to zero: at that point no more sends are
	// possible, so run can safely do one last check of pendingCh and exit.
	drainCh chan struct{}

	// A boolean that tracks whether the mailbox is open. Needed because Stop may be called more than
	// once.
	open atomic.Bool

	// A channel that is closed when the mailbox goroutine is terminating.
	doneCh chan struct{}
}

// NewConflatingMailbox creates and starts a ConflatingMailbox. The returned mailbox is immediately
// ready to accept work via Send.
func NewConflatingMailbox() *ConflatingMailbox {
	cm := &ConflatingMailbox{
		pendingCh: make(chan func(), 1),
		closedCh:  make(chan struct{}),
		drainCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	go cm.run()

	// Flip open last, once every field the receive loop and senders depend on is fully constructed
	// and the goroutine is scheduled.
	cm.open.Store(true)

	return cm
}

func (cm *ConflatingMailbox) run() {
	defer close(cm.doneCh)

	for {
		select {
		case f := <-cm.pendingCh:
			f()
		case <-cm.drainCh:
			// sendOps has already drained to zero by the time drainCh is closed (see Stop), so no
			// further deposits into pendingCh are possible: at most one final item can be sitting
			// there. Run it, if present, then exit for good.
			select {
			case f := <-cm.pendingCh:
				f()
			default:
			}
			return
		}
	}
}

// Send replaces any not-yet-started pending closure with target. If one was already queued, it is
// discarded and never runs; a closure already running is unaffected. Returns an error if the mailbox
// is closed or closing.
//
// If Send is called concurrently by multiple goroutines with no ordering between them, which call's
// closure ends up surviving is unspecified — see the type doc.
func (cm *ConflatingMailbox) Send(target func()) error {
	cm.sendOps.Add(1)
	defer cm.sendOps.Done()

	select {
	case <-cm.closedCh:
		return errors.New("conflating mailbox closed")
	default:
	}

	for {
		select {
		case cm.pendingCh <- target:
			return nil
		default:
		}

		// The slot is occupied by a stale, not-yet-started item: discard it and retry the deposit.
		select {
		case <-cm.pendingCh:
		default:
		}
	}
}

// Stop closes the mailbox for new sends, then waits for the worker to run whatever closure is
// currently pending (if any) and exit, or for ctx to be done first. A timed-out Stop does not abort
// the worker: if a closure is still pending, it still runs to completion in the background — Stop
// merely stops waiting for it from the caller's perspective. Safe to call multiple times; only the
// first call does anything.
func (cm *ConflatingMailbox) Stop(ctx context.Context) error {
	if !cm.open.CompareAndSwap(true, false) {
		return nil
	}

	close(cm.closedCh)
	cm.sendOps.Wait()
	close(cm.drainCh)

	select {
	case <-cm.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
