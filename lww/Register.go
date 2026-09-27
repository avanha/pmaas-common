// Package lww provides a last-write-wins register: a generic, reusable primitive for a value that's
// updated from an unordered or concurrently-delivered stream, where only the most recent write (by its
// own reported time, not the order it happens to be processed in) should ever take effect.
package lww

import "time"

// Register pairs a value with the time it was last set. Set only accepts a new value if its timestamp
// is after what's already recorded, so concurrent or out-of-order writes converge on whichever one
// actually happened most recently, regardless of the order they're applied in.
type Register[T any] struct {
	Value      T
	UpdateTime time.Time
}

// Set applies newValue if timestamp is after r's current UpdateTime. Returns true if it was applied.
func (r *Register[T]) Set(timestamp time.Time, newValue T) bool {
	if !timestamp.After(r.UpdateTime) {
		return false
	}

	r.Value = newValue
	r.UpdateTime = timestamp
	return true
}

// Timestamped is satisfied by *Register[T] regardless of T.
type Timestamped interface {
	Timestamp() time.Time
}

// Timestamp returns when r's Value was last set. It's named Timestamp, not UpdateTime, only because
// UpdateTime is already the exported field name — a method can't share it.
func (r *Register[T]) Timestamp() time.Time {
	return r.UpdateTime
}

var _ Timestamped = (*Register[struct{}])(nil)
