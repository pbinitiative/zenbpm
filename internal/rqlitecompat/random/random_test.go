package random

import (
	"testing"
	"time"
)

func TestStringN(t *testing.T) {
	got := StringN(12)
	if len(got) != 12 {
		t.Fatalf("expected length 12, got %d", len(got))
	}
	for _, r := range got {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')) {
			t.Fatalf("unexpected character %q in %q", r, got)
		}
	}
}

func TestString(t *testing.T) {
	if got := String(); len(got) != 20 {
		t.Fatalf("expected length 20, got %d", len(got))
	}
}

func TestJitter(t *testing.T) {
	base := 10 * time.Millisecond
	got := Jitter(base)
	if got < base || got >= 2*base {
		t.Fatalf("expected jitter in [%s,%s), got %s", base, 2*base, got)
	}
}
