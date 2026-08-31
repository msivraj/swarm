package verification

import (
	"fmt"
	"sort"
	"testing"

	"github.com/msivraj/swarm/internal/core/reputation"
	"github.com/msivraj/swarm/internal/model"
)

// testPool builds a pool of n distinct machine IDs "m0".."m(n-1)".
func testPool(n int) []model.MachineID {
	pool := make([]model.MachineID, n)
	for i := range pool {
		pool[i] = model.MachineID(fmt.Sprintf("m%d", i))
	}
	return pool
}

// assertDistinctSubset fails t if got has duplicates or any element not in
// pool.
func assertDistinctSubset(t *testing.T, pool, got []model.MachineID) {
	t.Helper()
	inPool := make(map[model.MachineID]bool, len(pool))
	for _, m := range pool {
		inPool[m] = true
	}
	seen := make(map[model.MachineID]bool, len(got))
	for _, m := range got {
		if seen[m] {
			t.Fatalf("Assign returned duplicate machine %q in %v", m, got)
		}
		seen[m] = true
		if !inPool[m] {
			t.Fatalf("Assign returned %q, not a member of pool %v", m, pool)
		}
	}
}

// TestAssignTable is a table-driven test over Assign's edge cases: k <= 0,
// an empty pool, k within the pool, and k >= len(pool).
func TestAssignTable(t *testing.T) {
	pool5 := testPool(5)

	tests := []struct {
		name    string
		task    model.TaskID
		pool    []model.MachineID
		k       int
		seed    uint64
		wantN   int
		wantNil bool
	}{
		{"k zero returns nil", "t1", pool5, 0, 42, 0, true},
		{"k negative returns nil", "t1", pool5, -3, 42, 0, true},
		{"empty pool returns nil", "t1", nil, 3, 42, 0, true},
		{"k within pool", "t1", pool5, 3, 42, 3, false},
		{"k equals pool size returns whole pool", "t1", pool5, 5, 42, 5, false},
		{"k exceeds pool size returns whole pool", "t1", pool5, 50, 42, 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Assign(tt.task, tt.pool, tt.k, tt.seed)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("Assign(...) = %v, want nil", got)
				}
				return
			}
			if len(got) != tt.wantN {
				t.Fatalf("Assign(...) returned %d machines, want %d", len(got), tt.wantN)
			}
			assertDistinctSubset(t, tt.pool, got)
		})
	}
}

// TestAssignWholePoolIsPermutation checks that when k >= len(pool), Assign
// returns every pool member exactly once (a permutation of pool), not
// merely len(pool) arbitrary entries.
func TestAssignWholePoolIsPermutation(t *testing.T) {
	pool := testPool(8)
	got := Assign("job-1", pool, 100, 7)

	if len(got) != len(pool) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(pool))
	}
	assertDistinctSubset(t, pool, got)

	sortedGot := append([]model.MachineID(nil), got...)
	sortedPool := append([]model.MachineID(nil), pool...)
	sort.Slice(sortedGot, func(i, j int) bool { return sortedGot[i] < sortedGot[j] })
	sort.Slice(sortedPool, func(i, j int) bool { return sortedPool[i] < sortedPool[j] })
	for i := range sortedGot {
		if sortedGot[i] != sortedPool[i] {
			t.Fatalf("got is not a permutation of pool: got=%v pool=%v", got, pool)
		}
	}
}

// TestAssignDeterministic is the seed-determinism property test (mirrors
// mitosis/barrier's determinism tests): the same (t, pool, k, seed) always
// returns byte-identical output, called repeatedly and in any order, and
// distinct seeds generally produce distinct selections.
func TestAssignDeterministic(t *testing.T) {
	pool := testPool(20)

	seeds := []uint64{0, 1, 42, 1337, 999999937}
	for _, seed := range seeds {
		first := Assign("job-42", pool, 5, seed)
		for i := 0; i < 20; i++ {
			got := Assign("job-42", pool, 5, seed)
			if !equalMachineIDs(got, first) {
				t.Fatalf("seed=%d: Assign not deterministic: %v vs %v", seed, first, got)
			}
		}
	}

	// Different seeds generally yield different selections. With 5 seeds
	// over a 20-machine pool, requiring at least one pairwise difference is
	// a very weak, non-flaky bar.
	results := make([][]model.MachineID, len(seeds))
	for i, seed := range seeds {
		results[i] = Assign("job-42", pool, 5, seed)
	}
	anyDifferent := false
	for i := 1; i < len(results); i++ {
		if !equalMachineIDs(results[i], results[0]) {
			anyDifferent = true
			break
		}
	}
	if !anyDifferent {
		t.Fatalf("all %d distinct seeds produced the identical selection %v", len(seeds), results[0])
	}

	// Distinct tasks sharing the same base seed also generally diverge, so
	// one seed does not assign every task in a job to the same K-set.
	byTask1 := Assign("task-1", pool, 5, 42)
	byTask2 := Assign("task-2", pool, 5, 42)
	if equalMachineIDs(byTask1, byTask2) {
		t.Fatalf("two distinct tasks under the same seed produced the identical selection %v", byTask1)
	}
}

func equalMachineIDs(a, b []model.MachineID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestAssignUniformity is the §03 "assign" property test: assignment is
// (approximately) uniform over the pool across many seeds. Every pool
// member should be selected with frequency close to k/len(pool), so
// colluding Sybils rarely co-occupy a single task's K-set. Seeds are a
// fixed, deterministic counter — never a random source (fcischeck forbids
// math/rand and crypto/rand in every .go file under internal/core,
// including tests).
func TestAssignUniformity(t *testing.T) {
	const poolSize = 10
	const k = 3
	const trials = 20000

	pool := testPool(poolSize)
	counts := make(map[model.MachineID]int, poolSize)

	for seed := uint64(0); seed < trials; seed++ {
		got := Assign("uniformity-task", pool, k, seed)
		assertDistinctSubset(t, pool, got)
		if len(got) != k {
			t.Fatalf("seed=%d: Assign returned %d machines, want %d", seed, len(got), k)
		}
		for _, m := range got {
			counts[m]++
		}
	}

	wantFreq := float64(trials*k) / float64(poolSize)
	const tolerance = 0.15 // 15% relative tolerance
	for _, m := range pool {
		got := float64(counts[m])
		lo, hi := wantFreq*(1-tolerance), wantFreq*(1+tolerance)
		if got < lo || got > hi {
			t.Fatalf("machine %q selected %v times over %d trials, want within [%v, %v] (uniform ~%v)",
				m, got, trials, lo, hi, wantFreq)
		}
	}
}

// TestAssignNoPositionalBias checks Assign does not systematically favor
// early pool positions: each pool member's overall selection frequency
// (checked above) already implies this, but this test additionally checks
// no single early-index machine dominates by comparing the first and second
// halves of the pool's aggregate selection counts.
func TestAssignNoPositionalBias(t *testing.T) {
	const poolSize = 12
	const k = 4
	const trials = 12000

	pool := testPool(poolSize)
	var firstHalf, secondHalf int

	for seed := uint64(0); seed < trials; seed++ {
		got := Assign("positional-bias-task", pool, k, seed)
		for _, m := range got {
			idx := indexOf(pool, m)
			if idx < poolSize/2 {
				firstHalf++
			} else {
				secondHalf++
			}
		}
	}

	total := firstHalf + secondHalf
	frac := float64(firstHalf) / float64(total)
	if frac < 0.4 || frac > 0.6 {
		t.Fatalf("first-half share of selections = %v, want close to 0.5 (first=%d second=%d)", frac, firstHalf, secondHalf)
	}
}

func indexOf(pool []model.MachineID, m model.MachineID) int {
	for i, p := range pool {
		if p == m {
			return i
		}
	}
	return -1
}

// TestRedundancyDelegates is the "Redundancy delegates, not diverges" table
// test: for representative (tier, rep) pairs, Redundancy must equal
// reputation.NeedsK(rep, tier) exactly.
func TestRedundancyDelegates(t *testing.T) {
	reps := []model.Reputation{
		{},
		{Score: 1, Observations: 1},
		{Score: 200, Observations: 20},
		{Score: 999, Observations: 500},
		{Score: 1000, Observations: 9999},
	}
	tiers := []model.Tier{model.Core, model.Open}

	for _, rep := range reps {
		for _, tier := range tiers {
			want := reputation.NeedsK(rep, tier)
			got := Redundancy(tier, rep)
			if got != want {
				t.Errorf("Redundancy(%v, %+v) = %d, want reputation.NeedsK(...) = %d", tier, rep, got, want)
			}
			if got <= 0 {
				t.Errorf("Redundancy(%v, %+v) = %d, not positive", tier, rep, got)
			}
			if got%2 == 0 {
				t.Errorf("Redundancy(%v, %+v) = %d, not odd", tier, rep, got)
			}
		}
	}
}

// TestVerdictTable is a table-driven test of Verdict's tally, including the
// exact-majority boundary, a no-majority split, an even tie, and the
// empty/too-few-results cases.
func TestVerdictTable(t *testing.T) {
	honest := []byte("honest-answer")
	lieA := []byte("lie-a")
	lieB := []byte("lie-b")

	tests := []struct {
		name string
		rs   []model.Result
		want model.Verdict
	}{
		{"empty is insufficient", nil, model.Verdict{Kind: model.Insufficient}},
		{"empty slice is insufficient", []model.Result{}, model.Verdict{Kind: model.Insufficient}},
		{
			"single result is a trivial majority of one",
			[]model.Result{{ID: "a", Value: honest, OK: true}},
			model.Verdict{Kind: model.Agreed, Value: honest},
		},
		{
			"exact majority 3 of 5 agrees",
			[]model.Result{
				{ID: "a", Value: honest, OK: true},
				{ID: "b", Value: honest, OK: true},
				{ID: "c", Value: honest, OK: true},
				{ID: "d", Value: lieA, OK: true},
				{ID: "e", Value: lieB, OK: true},
			},
			model.Verdict{Kind: model.Agreed, Value: honest},
		},
		{
			"even split 2 of 4 is disputed, never agreed",
			[]model.Result{
				{ID: "a", Value: honest, OK: true},
				{ID: "b", Value: honest, OK: true},
				{ID: "c", Value: lieA, OK: true},
				{ID: "d", Value: lieA, OK: true},
			},
			model.Verdict{Kind: model.Disputed},
		},
		{
			"three-way split with no majority is disputed",
			[]model.Result{
				{ID: "a", Value: honest, OK: true},
				{ID: "b", Value: lieA, OK: true},
				{ID: "c", Value: lieB, OK: true},
			},
			model.Verdict{Kind: model.Disputed},
		},
		{
			"exact half plus one of six agrees",
			[]model.Result{
				{ID: "a", Value: honest, OK: true},
				{ID: "b", Value: honest, OK: true},
				{ID: "c", Value: honest, OK: true},
				{ID: "d", Value: honest, OK: true},
				{ID: "e", Value: lieA, OK: true},
				{ID: "f", Value: lieB, OK: true},
			},
			model.Verdict{Kind: model.Agreed, Value: honest},
		},
		{
			"exact half of six is not a strict majority, disputed",
			[]model.Result{
				{ID: "a", Value: honest, OK: true},
				{ID: "b", Value: honest, OK: true},
				{ID: "c", Value: honest, OK: true},
				{ID: "d", Value: lieA, OK: true},
				{ID: "e", Value: lieA, OK: true},
				{ID: "f", Value: lieA, OK: true},
			},
			model.Verdict{Kind: model.Disputed},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Verdict(tt.rs)
			if got.Kind != tt.want.Kind {
				t.Fatalf("Verdict(%+v).Kind = %v, want %v", tt.rs, got.Kind, tt.want.Kind)
			}
			if got.Kind == model.Agreed && string(got.Value) != string(tt.want.Value) {
				t.Fatalf("Verdict(%+v).Value = %q, want %q", tt.rs, got.Value, tt.want.Value)
			}
			if got.Kind != model.Agreed && got.Value != nil {
				t.Fatalf("Verdict(%+v).Value = %q, want nil for a non-Agreed verdict", tt.rs, got.Value)
			}
		})
	}
}

// TestVerdictMinorityCannotFlip is the §03 "verdict" property test: whenever
// the honest machines hold a strict majority of the K-sample, an arbitrary,
// varied minority of liars can never change the Agreed value away from the
// honest one. The headline case from the doc (K=5, 3 honest agree, 2 lie)
// is included as one of the swept configurations.
func TestVerdictMinorityCannotFlip(t *testing.T) {
	honest := []byte("the-honest-value")

	// Sweep (honestCount, liarCount) pairs where honestCount is a strict
	// majority of the total, and vary the liars' values adversarially
	// (each liar picks a distinct, non-honest value; some liars collude on
	// a shared lie) across several configurations.
	type cfg struct {
		honestCount int
		liarValues  [][]byte
	}
	cfgs := []cfg{
		{3, [][]byte{[]byte("lie-1"), []byte("lie-2")}},                                   // K=5, doc headline
		{2, [][]byte{[]byte("lie-1")}},                                                    // K=3
		{4, [][]byte{[]byte("lie-1"), []byte("lie-1"), []byte("lie-2")}},                  // liars collude on one value, still a minority
		{5, [][]byte{[]byte("lie-1"), []byte("lie-2"), []byte("lie-3"), []byte("lie-4")}}, // K=9, every liar distinct
		{6, [][]byte{ // liars all collude on the SAME lie value, still can't outnumber
			[]byte("collude"), []byte("collude"), []byte("collude"), []byte("collude"), []byte("collude"),
		}},
	}

	for i, c := range cfgs {
		var rs []model.Result
		for h := 0; h < c.honestCount; h++ {
			rs = append(rs, model.Result{ID: model.SpiffeID(fmt.Sprintf("honest-%d", h)), Value: honest, OK: true})
		}
		for j, lv := range c.liarValues {
			rs = append(rs, model.Result{ID: model.SpiffeID(fmt.Sprintf("liar-%d", j)), Value: lv, OK: true})
		}
		total := len(rs)
		if c.honestCount*2 <= total {
			t.Fatalf("cfg[%d]: test setup bug, honest count %d is not a strict majority of %d", i, c.honestCount, total)
		}

		got := Verdict(rs)
		if got.Kind != model.Agreed {
			t.Fatalf("cfg[%d] (honest=%d, liars=%d): Verdict.Kind = %v, want Agreed", i, c.honestCount, len(c.liarValues), got.Kind)
		}
		if string(got.Value) != string(honest) {
			t.Fatalf("cfg[%d]: Verdict.Value = %q, want the honest value %q — a minority flipped the result", i, got.Value, honest)
		}
	}
}

// TestVerdictDeterministic asserts Verdict is pure: identical input always
// produces an identical Verdict, regardless of call order.
func TestVerdictDeterministic(t *testing.T) {
	rs := []model.Result{
		{ID: "a", Value: []byte("v1"), OK: true},
		{ID: "b", Value: []byte("v1"), OK: true},
		{ID: "c", Value: []byte("v2"), OK: true},
	}
	first := Verdict(rs)
	for i := 0; i < 10; i++ {
		got := Verdict(rs)
		if got.Kind != first.Kind || string(got.Value) != string(first.Value) {
			t.Fatalf("Verdict not deterministic: %+v vs %+v", first, got)
		}
	}
}
