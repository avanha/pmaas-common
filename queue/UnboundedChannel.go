package queue

import "sync/atomic"

type UnboundedChannel[T any] struct {
	in  chan T
	out chan T

	// highWatermark tracks the largest internal buffer length ever observed,
	// so callers can monitor how far the channel has had to grow.  It's
	// updated by the run goroutine and read concurrently, hence atomic.
	highWatermark atomic.Int64
}

func NewUnboundedChannel[T any]() *UnboundedChannel[T] {
	uc := &UnboundedChannel[T]{
		in:  make(chan T),
		out: make(chan T),
	}
	go uc.run()
	return uc
}

func (uc *UnboundedChannel[T]) run() {
	defer close(uc.out)
	var buffer []T

	for {
		if len(buffer) == 0 {
			v, ok := <-uc.in
			if !ok {
				return
			}
			buffer = append(buffer, v)
			uc.recordHighWatermark(len(buffer))
			continue
		}

		select {
		case v, ok := <-uc.in:
			if !ok {
				for _, item := range buffer {
					uc.out <- item
				}
				return
			}
			buffer = append(buffer, v)
			uc.recordHighWatermark(len(buffer))
		case uc.out <- buffer[0]:
			buffer = buffer[1:]
		}
	}
}

// recordHighWatermark updates highWatermark if the given length is the
// largest observed so far.  It's only ever called from the run goroutine, so
// there's a single writer and no need for a CAS loop; Store is still used
// (rather than a plain field) because HighWatermark reads concurrently.
func (uc *UnboundedChannel[T]) recordHighWatermark(length int) {
	if int64(length) > uc.highWatermark.Load() {
		uc.highWatermark.Store(int64(length))
	}
}

func (uc *UnboundedChannel[T]) In() chan<- T  { return uc.in }
func (uc *UnboundedChannel[T]) Out() <-chan T { return uc.out }
func (uc *UnboundedChannel[T]) Close()        { close(uc.in) }

// HighWatermark returns the largest internal buffer length ever observed by
// this channel, i.e. the most items that were ever queued (waiting to be
// consumed) at the same time.
func (uc *UnboundedChannel[T]) HighWatermark() int64 {
	return uc.highWatermark.Load()
}
