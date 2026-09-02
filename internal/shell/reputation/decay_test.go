package reputation

import (
	"sync"
	"sync/atomic"
	"testing"

	repcore "github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// testClock is a controllable fake clock: it never reads a real clock, so
// tests drive DecayPass/RecordVerdict timestamping deterministically, and it
// is safe to read/advance concurrently (needed by the -race tests below).
type testClock struct {
	ns atomic.Int64
}

func (c *testClock) now() model.Instant  { return model.Instant(c.ns.Load()) }
func (c *testClock) set(v model.Instant) { c.ns.Store(int64(v)) }

// oneDay mirrors the core's decayUnit (one day, in ns) so tests can express
// elapsed spans in the same unit Decay steps by, without importing the
// core's unexported constant.
const oneDay = model.Duration(24 * 60 * 60 * 1_000_000_000)

func TestDecayPassFadesStaleIdentity(t *testing.T) {
	clock := &testClock{}
	s := NewDecayingStore(clock.now)
	id := model.SpiffeID("spiffe://swarm/open/stale")

	before := s.RecordVerdict(id, true) // Score raised off the zero floor

	clock.set(model.Instant(100 * oneDay)) // long idle stretch
	s.DecayPass(clock.now())

	after := s.Get(id)
	if after.Score >= before.Score {
		t.Fatalf("DecayPass Score = %d, want < %d (before decay)", after.Score, before.Score)
	}
	if after.Observations != before.Observations {
		t.Fatalf("DecayPass Observations = %d, want unchanged %d", after.Observations, before.Observations)
	}
}

func TestDecayPassRecentlyTouchedBarelyDecays(t *testing.T) {
	clock := &testClock{}
	s := NewDecayingStore(clock.now)
	id := model.SpiffeID("spiffe://swarm/open/fresh")

	before := s.RecordVerdict(id, true)

	clock.set(model.Instant(oneDay - 1)) // just under one decay unit
	s.DecayPass(clock.now())

	after := s.Get(id)
	if after.Score != before.Score {
		t.Fatalf("DecayPass on a recently-touched identity Score = %d, want unchanged %d", after.Score, before.Score)
	}
}

func TestDecayPassNeverNegativeClampsAtFloor(t *testing.T) {
	clock := &testClock{}
	s := NewDecayingStore(clock.now)
	id := model.SpiffeID("spiffe://swarm/open/chronic-absent")

	s.Put(id, model.Reputation{Score: 100, Observations: 5})

	// Repeated decay passes, each stepping the clock forward, should drive
	// Score down to zero and hold it there — never negative.
	elapsed := model.Instant(0)
	for i := 0; i < 20; i++ {
		elapsed += model.Instant(50 * oneDay)
		clock.set(elapsed)
		s.DecayPass(clock.now())

		got := s.Get(id)
		if got.Score < 0 {
			t.Fatalf("pass %d: Score = %d, want >= 0", i, got.Score)
		}
	}

	final := s.Get(id)
	if final.Score != 0 {
		t.Fatalf("Score after repeated decay = %d, want 0 (clamped floor)", final.Score)
	}
}

func TestDecayFreezeCrossingAtShell(t *testing.T) {
	tests := []struct {
		name         string
		verdicts     int // number of honest RecordVerdict calls before decay
		wantEligible bool
	}{
		{
			name:         "once-good high-observation identity decays into the freeze",
			verdicts:     10, // Observations=10 >= minObservations, Score=100
			wantEligible: false,
		},
		{
			name:         "fresh low-observation identity stays eligible even at Score 0",
			verdicts:     1, // Observations=1 < minObservations
			wantEligible: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := &testClock{}
			s := NewDecayingStore(clock.now)
			id := model.SpiffeID("spiffe://swarm/open/" + tt.name)

			for i := 0; i < tt.verdicts; i++ {
				s.RecordVerdict(id, true)
			}

			clock.set(model.Instant(1000 * oneDay)) // long enough to zero any Score
			s.DecayPass(clock.now())

			got := s.Get(id)
			if got.Score != 0 {
				t.Fatalf("Score after decay = %d, want 0", got.Score)
			}
			if got.Score < 0 {
				t.Fatalf("Score after decay = %d, want >= 0", got.Score)
			}

			if eligible := repcore.Eligible(got); eligible != tt.wantEligible {
				t.Fatalf("Eligible(%+v) = %v, want %v", got, eligible, tt.wantEligible)
			}
		})
	}
}

// TestDecayingStoreVerdictPathIntact asserts RecordVerdict on DecayingStore
// still behaves exactly like memStore's: honest raises Score, Observations
// increments, and the result round-trips through Get.
func TestDecayingStoreVerdictPathIntact(t *testing.T) {
	clock := &testClock{}
	s := NewDecayingStore(clock.now)
	id := model.SpiffeID("spiffe://swarm/open/verdict-path")

	got := s.RecordVerdict(id, true)
	if got.Score <= 0 {
		t.Fatalf("RecordVerdict(true).Score = %d, want > 0", got.Score)
	}
	if got.Observations != 1 {
		t.Fatalf("RecordVerdict(true).Observations = %d, want 1", got.Observations)
	}

	reread := s.Get(id)
	if reread != got {
		t.Fatalf("Get(%v) after RecordVerdict = %+v, want %+v", id, reread, got)
	}
}

// TestConcurrentDecayPassAndVerdictsAreRaceFree drives RecordVerdict and
// DecayPass concurrently against the same identity — run with -race. Score
// is contended by both writers so no property holds on it, but Observations
// is only ever touched by RecordVerdict: the total must equal exactly the
// number of RecordVerdict calls made, proving no verdict is lost under a
// racing decay pass and that DecayPass/RecordVerdict never deadlock.
func TestConcurrentDecayPassAndVerdictsAreRaceFree(t *testing.T) {
	clock := &testClock{}
	s := NewDecayingStore(clock.now)
	id := model.SpiffeID("spiffe://swarm/open/contended")

	const verdictGoroutines = 20
	const verdictsPerGoroutine = 25
	const totalVerdicts = verdictGoroutines * verdictsPerGoroutine
	const decayGoroutines = 10
	const passesPerGoroutine = 25

	var wg sync.WaitGroup
	wg.Add(verdictGoroutines + decayGoroutines)

	for g := 0; g < verdictGoroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < verdictsPerGoroutine; i++ {
				s.RecordVerdict(id, true)
			}
		}()
	}
	for g := 0; g < decayGoroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < passesPerGoroutine; i++ {
				clock.set(model.Instant(int64(g*passesPerGoroutine+i) * int64(oneDay)))
				s.DecayPass(clock.now())
			}
		}(g)
	}
	wg.Wait()

	got := s.Get(id)
	if got.Observations != totalVerdicts {
		t.Fatalf("Get(%v).Observations = %d, want %d (no verdict should be lost to a racing decay pass)", id, got.Observations, totalVerdicts)
	}
	if got.Score < 0 {
		t.Fatalf("Get(%v).Score = %d, want >= 0", id, got.Score)
	}
}
