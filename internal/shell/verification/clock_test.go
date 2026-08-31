package verification

import (
	"testing"
	"time"

	"github.com/msivraj/swarm/internal/model"
)

func TestFakeClock_AfterFiresOnAdvance(t *testing.T) {
	c := NewFakeClock(100)
	ch := c.After(50)

	select {
	case <-ch:
		t.Fatal("After fired before Advance — a fake clock must never fire on its own")
	default:
	}

	c.Advance(49)
	select {
	case <-ch:
		t.Fatal("After fired before its deadline (advanced 49 of 50)")
	default:
	}

	c.Advance(1)
	select {
	case got := <-ch:
		if got != 150 {
			t.Fatalf("After fired with instant %d, want 150", got)
		}
	default:
		t.Fatal("After did not fire once its deadline elapsed")
	}
}

func TestFakeClock_AfterNonPositiveFiresImmediately(t *testing.T) {
	c := NewFakeClock(10)
	ch := c.After(0)
	select {
	case got := <-ch:
		if got != 10 {
			t.Fatalf("After(0) fired with %d, want 10 (now)", got)
		}
	default:
		t.Fatal("After(0) did not fire immediately")
	}
}

func TestFakeClock_SetFiresDueWaiters(t *testing.T) {
	c := NewFakeClock(0)
	ch := c.After(1000)
	c.Set(500)
	select {
	case <-ch:
		t.Fatal("After fired after Set moved the clock short of its deadline")
	default:
	}
	c.Set(1000)
	select {
	case <-ch:
	default:
		t.Fatal("After did not fire once Set reached its deadline")
	}
}

func TestFakeClock_BlockUntilUnblocksOnceRegistered(t *testing.T) {
	c := NewFakeClock(0)
	registered := make(chan struct{})
	go func() {
		c.After(10)
		close(registered)
	}()
	c.BlockUntil(1)
	<-registered // must already be closed; BlockUntil only returns after registration
}

func TestFakeClock_Now(t *testing.T) {
	c := NewFakeClock(42)
	if got := c.Now(); got != 42 {
		t.Fatalf("Now() = %d, want 42", got)
	}
	c.Advance(8)
	if got := c.Now(); got != 50 {
		t.Fatalf("Now() after Advance(8) = %d, want 50", got)
	}
}

// TestRealClock is a light sanity check of the production Clock: Now
// advances with real time and After eventually fires. A short real sleep is
// appropriate here — it is testing the wall-clock adapter itself, not
// driving business-logic timing (see coordinator_test.go for that, which
// uses FakeClock exclusively).
func TestRealClock(t *testing.T) {
	var c RealClock
	before := c.Now()

	ch := c.After(model.Duration(time.Millisecond))
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("RealClock.After(1ms) did not fire within 2s")
	}

	after := c.Now()
	if after < before {
		t.Fatalf("RealClock.Now() went backwards: %d -> %d", before, after)
	}
}
