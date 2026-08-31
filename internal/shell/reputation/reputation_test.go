package reputation

import (
	"sync"
	"testing"

	repcore "github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// TestGetNeverSeenIsZeroValue asserts the zero-start property at the shell
// boundary: an id the store has never Put reads back model.Reputation{} —
// Score 0, Observations 0 — never a defaulted-trusted value.
func TestGetNeverSeenIsZeroValue(t *testing.T) {
	s := NewMemStore()

	got := s.Get("spiffe://swarm/open/never-seen")

	want := model.Reputation{}
	if got != want {
		t.Fatalf("Get(never-seen) = %+v, want zero value %+v", got, want)
	}
	if got.Score != 0 {
		t.Fatalf("Get(never-seen).Score = %d, want 0", got.Score)
	}
	if got.Observations != 0 {
		t.Fatalf("Get(never-seen).Observations = %d, want 0", got.Observations)
	}
}

func TestPutGetRoundTrips(t *testing.T) {
	s := NewMemStore()
	id := model.SpiffeID("spiffe://swarm/open/a")
	rep := model.Reputation{Score: 42, Observations: 3}

	s.Put(id, rep)

	got := s.Get(id)
	if got != rep {
		t.Fatalf("Get(%v) = %+v, want %+v", id, got, rep)
	}
}

func TestPutOverwritesPriorValue(t *testing.T) {
	s := NewMemStore()
	id := model.SpiffeID("spiffe://swarm/open/a")

	s.Put(id, model.Reputation{Score: 10, Observations: 1})
	s.Put(id, model.Reputation{Score: 20, Observations: 2})

	got := s.Get(id)
	want := model.Reputation{Score: 20, Observations: 2}
	if got != want {
		t.Fatalf("Get(%v) after overwrite = %+v, want %+v", id, got, want)
	}
}

// TestManualReadUpdateWriteAppliesCoreUpdate exercises the documented
// coordinator pattern: rep := store.Get(id); rep = reputation.Update(rep,
// agreed); store.Put(id, rep) — verifying the shell composes correctly with
// the pure core it calls.
func TestManualReadUpdateWriteAppliesCoreUpdate(t *testing.T) {
	s := NewMemStore()
	id := model.SpiffeID("spiffe://swarm/open/manual")

	rep := s.Get(id)
	rep = repcore.Update(rep, true)
	s.Put(id, rep)

	got := s.Get(id)
	if got.Score <= 0 {
		t.Fatalf("Get(%v).Score = %d after honest agreement, want > 0", id, got.Score)
	}
	if got.Observations != 1 {
		t.Fatalf("Get(%v).Observations = %d, want 1", id, got.Observations)
	}
}

// TestRecordVerdictHonestRaisesLieLowers is the acceptance-criteria table
// test: after a verdict via RecordVerdict, an honest agreement raises Score
// and a lie lowers it (relative to a prior nonzero baseline), respecting
// the core's clamp/floor, and the new value durably reads back via Get.
func TestRecordVerdictHonestRaisesLieLowers(t *testing.T) {
	tests := []struct {
		name    string
		seed    model.Reputation
		agreed  bool
		wantCmp func(before, after int64) bool
	}{
		{
			name:    "honest agreement from zero-start raises score",
			seed:    model.Reputation{},
			agreed:  true,
			wantCmp: func(before, after int64) bool { return after > before },
		},
		{
			name:    "lie from zero-start never lowers score below the floor",
			seed:    model.Reputation{},
			agreed:  false,
			wantCmp: func(before, after int64) bool { return after >= 0 && after <= before },
		},
		{
			name:    "lie from a nonzero baseline lowers score",
			seed:    model.Reputation{Score: 100, Observations: 5},
			agreed:  false,
			wantCmp: func(before, after int64) bool { return after < before },
		},
		{
			name:    "honest agreement from a nonzero baseline raises score",
			seed:    model.Reputation{Score: 100, Observations: 5},
			agreed:  true,
			wantCmp: func(before, after int64) bool { return after > before },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewMemStore()
			id := model.SpiffeID("spiffe://swarm/open/" + tt.name)
			s.Put(id, tt.seed)

			got := s.RecordVerdict(id, tt.agreed)

			if !tt.wantCmp(tt.seed.Score, got.Score) {
				t.Fatalf("RecordVerdict(%v, agreed=%v) Score %d -> %d, comparison failed", id, tt.agreed, tt.seed.Score, got.Score)
			}
			if got.Observations != tt.seed.Observations+1 {
				t.Fatalf("RecordVerdict(%v) Observations = %d, want %d", id, got.Observations, tt.seed.Observations+1)
			}

			// Durably read back: RecordVerdict's return value must match
			// what Get sees afterward.
			reread := s.Get(id)
			if reread != got {
				t.Fatalf("Get(%v) after RecordVerdict = %+v, want %+v", id, reread, got)
			}
		})
	}
}

// TestConcurrentDistinctIDsDoNotClobber drives concurrent Get/RecordVerdict
// calls across many DISTINCT ids and asserts each id ends with exactly the
// expected number of honest updates applied to it — run with -race.
func TestConcurrentDistinctIDsDoNotClobber(t *testing.T) {
	s := NewMemStore()
	const numIDs = 50
	const updatesPerID = 20

	ids := make([]model.SpiffeID, numIDs)
	for i := range ids {
		ids[i] = model.SpiffeID(rune('a' + i%26))
	}

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id model.SpiffeID) {
			defer wg.Done()
			for i := 0; i < updatesPerID; i++ {
				s.RecordVerdict(id, true)
				_ = s.Get(id)
			}
		}(id)
	}
	wg.Wait()

	seen := map[model.SpiffeID]int{}
	for _, id := range ids {
		seen[id]++
	}
	for id, count := range seen {
		got := s.Get(id)
		want := updatesPerID * count
		if got.Observations != want {
			t.Fatalf("Get(%v).Observations = %d, want %d (id updated by %d goroutine(s))", id, got.Observations, want, count)
		}
	}
}

// TestConcurrentRecordVerdictSameIDIsAtomic drives many goroutines calling
// RecordVerdict on the SAME id concurrently and asserts no update is lost —
// Observations must equal exactly the number of calls made. This guards the
// atomic read-modify-write RecordVerdict promises. Run with -race.
func TestConcurrentRecordVerdictSameIDIsAtomic(t *testing.T) {
	s := NewMemStore()
	id := model.SpiffeID("spiffe://swarm/open/contended")
	const goroutines = 100
	const callsPerGoroutine = 20
	const total = goroutines * callsPerGoroutine

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < callsPerGoroutine; i++ {
				s.RecordVerdict(id, true)
			}
		}()
	}
	wg.Wait()

	got := s.Get(id)
	if got.Observations != total {
		t.Fatalf("Get(%v).Observations = %d, want %d (no update should be lost)", id, got.Observations, total)
	}
}
