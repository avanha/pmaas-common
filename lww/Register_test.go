package lww_test

import (
	"testing"
	"time"

	"github.com/avanha/pmaas-common/lww"
)

func TestRegister_SetAppliesNewerValue(t *testing.T) {
	var r lww.Register[int]
	now := time.Now()

	if !r.Set(now, 42) {
		t.Fatalf("expected Set to apply the first value")
	}

	if r.Value != 42 || !r.UpdateTime.Equal(now) {
		t.Fatalf("expected Value=42, UpdateTime=%v, got Value=%v, UpdateTime=%v", now, r.Value, r.UpdateTime)
	}

	later := now.Add(time.Second)

	if !r.Set(later, 43) {
		t.Fatalf("expected Set to apply a newer value")
	}

	if r.Value != 43 || !r.UpdateTime.Equal(later) {
		t.Fatalf("expected Value=43, UpdateTime=%v, got Value=%v, UpdateTime=%v", later, r.Value, r.UpdateTime)
	}
}

func TestRegister_SetRejectsStaleValue(t *testing.T) {
	var r lww.Register[int]
	now := time.Now()
	r.Set(now, 42)

	earlier := now.Add(-time.Second)

	if r.Set(earlier, 99) {
		t.Fatalf("expected Set to reject a value older than what's already recorded")
	}

	if r.Value != 42 || !r.UpdateTime.Equal(now) {
		t.Fatalf("expected stale Set to leave Value/UpdateTime unchanged, got Value=%v, UpdateTime=%v", r.Value, r.UpdateTime)
	}
}

func TestRegister_SetRejectsEqualTimestamp(t *testing.T) {
	var r lww.Register[int]
	now := time.Now()
	r.Set(now, 42)

	if r.Set(now, 99) {
		t.Fatalf("expected Set to reject a value with the same timestamp as what's already recorded")
	}
}
