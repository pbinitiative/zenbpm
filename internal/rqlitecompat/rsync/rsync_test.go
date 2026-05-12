package rsync

import (
	"strings"
	"testing"
	"time"
)

func TestReadyTarget(t *testing.T) {
	rt := NewReadyTarget[int]()
	ch2 := rt.Subscribe(2)
	ch5 := rt.Subscribe(5)

	rt.Signal(3)
	select {
	case <-ch2:
	case <-time.After(time.Second):
		t.Fatal("target 2 was not signalled")
	}
	select {
	case <-ch5:
		t.Fatal("target 5 should not be signalled yet")
	default:
	}

	rt.Signal(5)
	select {
	case <-ch5:
	case <-time.After(time.Second):
		t.Fatal("target 5 was not signalled")
	}
}

func TestReadyTargetSubscribeAlreadyReached(t *testing.T) {
	rt := NewReadyTarget[int]()
	rt.Signal(4)
	ch := rt.Subscribe(3)

	select {
	case <-ch:
	default:
		t.Fatal("expected subscription below current target to be closed")
	}
}

func TestReadyTargetUnsubscribe(t *testing.T) {
	rt := NewReadyTarget[int]()
	ch := rt.Subscribe(2)
	rt.Unsubscribe(ch)

	if got := rt.Len(); got != 0 {
		t.Fatalf("expected no subscribers, got %d", got)
	}
}

func TestPollTrue(t *testing.T) {
	calls := 0
	err := NewPollTrue(func() bool {
		calls++
		return calls == 2
	}, time.Millisecond, time.Second).Run("test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPollTrueTimeout(t *testing.T) {
	err := NewPollTrue(func() bool {
		return false
	}, time.Millisecond, 5*time.Millisecond).Run("never")
	if err == nil || !strings.Contains(err.Error(), "timeout waiting for never") {
		t.Fatalf("unexpected error: %v", err)
	}
}
