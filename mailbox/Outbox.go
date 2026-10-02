package mailbox

import (
	"errors"
	"sync"
)

// Outbox runs closures, in the order they were posted, on its own goroutine, without ever making
// the poster wait.
//
// It exists to break wait cycles between actors. Mailbox is unbuffered, so if actor A blocks
// sending to actor B while B blocks sending to A, both wait forever. Giving one side an Outbox
// means that side's goroutine only ever appends to a slice; the Outbox's goroutine is the one that
// waits on the other actor (inside the posted closure, e.g. a blocking Mailbox.Send). Nothing waits
// on the Outbox goroutine, so it can't be part of a cycle. The queue is unbounded, so a slow
// receiver makes it grow rather than push back on the poster; use it where the poster must not be
// slowed down.
//
// A panic in a posted closure is deliberately not recovered: it crashes the process, so the cause
// gets fixed rather than silently killing delivery.
type Outbox struct {
	mu      sync.Mutex
	cond    *sync.Cond
	pending []func()
	closed  bool
	done    chan struct{}
}

// NewOutbox creates and starts an Outbox.
func NewOutbox() *Outbox {
	o := &Outbox{done: make(chan struct{})}
	o.cond = sync.NewCond(&o.mu)

	go o.run()

	return o
}

func (o *Outbox) run() {
	defer close(o.done)

	for {
		o.mu.Lock()

		for len(o.pending) == 0 && !o.closed {
			o.cond.Wait()
		}

		if len(o.pending) == 0 {
			// Closed and fully drained.
			o.mu.Unlock()
			return
		}

		next := o.pending[0]
		o.pending[0] = nil
		o.pending = o.pending[1:]
		o.mu.Unlock()

		next()
	}
}

// Post queues target to run on the Outbox's goroutine. It never blocks. Returns an error once Stop
// has been called.
func (o *Outbox) Post(target func()) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.closed {
		return errors.New("outbox closed")
	}

	o.pending = append(o.pending, target)
	o.cond.Signal()

	return nil
}

// Stop stops accepting new posts, then waits for everything already posted to run. Safe to call
// more than once. It must not be called from a closure running on this Outbox, which would wait
// on itself.
func (o *Outbox) Stop() {
	o.mu.Lock()
	o.closed = true
	o.cond.Broadcast()
	o.mu.Unlock()

	<-o.done
}
