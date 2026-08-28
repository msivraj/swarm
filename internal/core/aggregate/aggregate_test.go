package aggregate

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
)

// -----------------------------------------------------------------------
// test helpers
//
// aggregate cannot import templates' unexported byte-layout helpers or
// internal/e2e's exported mirrors (e2e sits outside the core import
// allow-list), so — like internal/e2e/wire.go does for its own callers —
// these helpers hand-mirror the layouts templates.KeyspaceMerge /
// templates.MonteCarloMerge already produce, for test purposes only.
// -----------------------------------------------------------------------

// ksHit builds a keyspace-search hit Aggregate: Value is the matching key as
// a big-endian uint64, the layout templates.KeyspaceCombine decodes.
func ksHit(jobID model.JobID, key uint64) model.Aggregate {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, key)
	return model.Aggregate{JobID: jobID, Value: b}
}

// ksNoHit builds a keyspace-search partial with no hit: the identity Value.
func ksNoHit(jobID model.JobID) model.Aggregate {
	return model.Aggregate{JobID: jobID}
}

// ksRaw builds one raw keyspace-search TaskResult: OK and a hit key, or a
// miss.
func ksRaw(id model.TaskID, hit bool, key uint64) model.TaskResult {
	if !hit {
		return model.TaskResult{TaskID: id, OK: false}
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, key)
	return model.TaskResult{TaskID: id, OK: true, Output: b}
}

// mcStats is the decoded form of a monte-carlo partial's Value, mirroring
// templates' unexported mcAggregate.
type mcStats struct {
	Count    int64
	Sum      float64
	Mean     float64
	Variance float64
}

func decodeMC(t *testing.T, agg model.Aggregate) mcStats {
	t.Helper()
	b := agg.Value
	if len(b) == 0 {
		return mcStats{} // the zero Aggregate's Value is the identity
	}
	if len(b) != 32 {
		t.Fatalf("Value is not a valid mc aggregate (%d bytes)", len(b))
	}
	return mcStats{
		Count:    int64(binary.BigEndian.Uint64(b[0:8])),
		Sum:      math.Float64frombits(binary.BigEndian.Uint64(b[8:16])),
		Mean:     math.Float64frombits(binary.BigEndian.Uint64(b[16:24])),
		Variance: math.Float64frombits(binary.BigEndian.Uint64(b[24:32])),
	}
}

// mcRaw builds one raw monte-carlo block TaskResult.Output: the layout
// templates.MonteCarloMerge decodes (big-endian Count, then the bits of
// float64 Sum and SumSq).
func mcRaw(id model.TaskID, count int64, sum, sumSq float64) model.TaskResult {
	out := make([]byte, 24)
	binary.BigEndian.PutUint64(out[0:8], uint64(count))
	binary.BigEndian.PutUint64(out[8:16], math.Float64bits(sum))
	binary.BigEndian.PutUint64(out[16:24], math.Float64bits(sumSq))
	return model.TaskResult{TaskID: id, OK: true, Output: out}
}

// mcPartial builds a real monte-carlo partial Aggregate the way a leaf tier
// would: by running templates.MonteCarloMerge over one raw block.
func mcPartial(jobID model.JobID, count int64, sum, sumSq float64) model.Aggregate {
	agg := templates.MonteCarloMerge([]model.TaskResult{mcRaw("t", count, sum, sumSq)})
	agg.JobID = jobID
	return agg
}

func mcApproxEqual(a, b mcStats) bool {
	const eps = 1e-9
	return a.Count == b.Count &&
		math.Abs(a.Sum-b.Sum) <= eps &&
		math.Abs(a.Mean-b.Mean) <= eps &&
		math.Abs(a.Variance-b.Variance) <= eps
}

// permutations returns every ordering of xs, via Heap's algorithm — a
// deterministic enumeration (no randomness), mirroring
// internal/core/routing/routing_test.go's approach for the same purpose.
func permutations(xs []model.Aggregate) [][]model.Aggregate {
	var out [][]model.Aggregate
	n := len(xs)
	buf := make([]model.Aggregate, n)
	copy(buf, xs)
	c := make([]int, n)

	snapshot := func() []model.Aggregate {
		cp := make([]model.Aggregate, n)
		copy(cp, buf)
		return cp
	}

	out = append(out, snapshot())
	for i := 0; i < n; {
		if c[i] < i {
			if i%2 == 0 {
				buf[0], buf[i] = buf[i], buf[0]
			} else {
				buf[c[i]], buf[i] = buf[i], buf[c[i]]
			}
			out = append(out, snapshot())
			c[i]++
			i = 0
		} else {
			c[i] = 0
			i++
		}
	}
	return out
}

// -----------------------------------------------------------------------
// Merge — table-driven, per template
// -----------------------------------------------------------------------

func TestMergeKeyspace(t *testing.T) {
	tests := []struct {
		name string
		a, b model.Aggregate
		want model.Aggregate
	}{
		{"both zero is the identity", model.Aggregate{}, model.Aggregate{}, model.Aggregate{}},
		{"a has a hit, b is zero", ksHit("j1", 5), model.Aggregate{}, model.Aggregate{JobID: "j1", Value: ksHit("", 5).Value}},
		{"b has a hit, a is zero", model.Aggregate{}, ksHit("j1", 5), model.Aggregate{JobID: "j1", Value: ksHit("", 5).Value}},
		{"smaller key wins", ksHit("j1", 9), ksHit("j1", 3), model.Aggregate{JobID: "j1", Value: ksHit("", 3).Value}},
		{
			name: "Done combines by OR",
			a:    model.Aggregate{JobID: "j1", Done: true},
			b:    model.Aggregate{JobID: "j1", Done: false},
			want: model.Aggregate{JobID: "j1", Done: true},
		},
		{
			name: "non-empty JobID propagates over an empty one",
			a:    model.Aggregate{JobID: "j1"},
			b:    model.Aggregate{},
			want: model.Aggregate{JobID: "j1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Merge(admission.TemplateKeyspaceSearch, tt.a, tt.b)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Merge() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestMergeMonteCarlo(t *testing.T) {
	tests := []struct {
		name string
		a, b model.Aggregate
		want mcStats
	}{
		{"both zero is the identity", model.Aggregate{}, model.Aggregate{}, mcStats{}},
		{"a is the identity", model.Aggregate{}, mcPartial("j1", 4, 8, 20), mcStats{Count: 4, Sum: 8, Mean: 2, Variance: 1}},
		{"b is the identity", mcPartial("j1", 4, 8, 20), model.Aggregate{}, mcStats{Count: 4, Sum: 8, Mean: 2, Variance: 1}},
		{
			name: "sums sufficient statistics across two partials",
			a:    mcPartial("j1", 2, 4, 8),
			b:    mcPartial("j1", 2, 4, 8),
			want: mcStats{Count: 4, Sum: 8, Mean: 2, Variance: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Merge(admission.TemplateMonteCarlo, tt.a, tt.b)
			gotStats := decodeMC(t, got)
			if gotStats != tt.want {
				t.Fatalf("Merge() stats = %+v, want %+v", gotStats, tt.want)
			}
		})
	}
}

func TestMergeUnknownTemplate(t *testing.T) {
	tests := []struct {
		name string
		a, b model.Aggregate
		want model.Aggregate
	}{
		{"both zero is the identity", model.Aggregate{}, model.Aggregate{}, model.Aggregate{}},
		{
			name: "a's Value wins over an empty b",
			a:    model.Aggregate{Value: []byte("apple")},
			b:    model.Aggregate{},
			want: model.Aggregate{Value: []byte("apple")},
		},
		{
			name: "smaller Value wins when b is smaller",
			a:    model.Aggregate{Value: []byte("banana")},
			b:    model.Aggregate{Value: []byte("apple")},
			want: model.Aggregate{Value: []byte("apple")},
		},
		{
			name: "smaller Value wins when a is smaller",
			a:    model.Aggregate{Value: []byte("apple")},
			b:    model.Aggregate{Value: []byte("banana")},
			want: model.Aggregate{Value: []byte("apple")},
		},
		{
			name: "distinct non-empty JobIDs pick the lexicographically smaller, a smaller",
			a:    model.Aggregate{JobID: "job-a"},
			b:    model.Aggregate{JobID: "job-b"},
			want: model.Aggregate{JobID: "job-a"},
		},
		{
			name: "distinct non-empty JobIDs pick the lexicographically smaller, b smaller",
			a:    model.Aggregate{JobID: "job-b"},
			b:    model.Aggregate{JobID: "job-a"},
			want: model.Aggregate{JobID: "job-a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Merge("some-unknown-template", tt.a, tt.b)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Merge() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// MergeAll
// -----------------------------------------------------------------------

func TestMergeAllEmptyIsZeroAggregate(t *testing.T) {
	for _, tmpl := range []string{admission.TemplateKeyspaceSearch, admission.TemplateMonteCarlo, "unknown"} {
		t.Run(tmpl, func(t *testing.T) {
			got := MergeAll(tmpl, nil)
			if !reflect.DeepEqual(got, model.Aggregate{}) {
				t.Fatalf("MergeAll(%q, nil) = %+v, want the zero Aggregate", tmpl, got)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Algebraic laws (issue #48) — commutative, associative, identity
// -----------------------------------------------------------------------

func TestMergeCommutativeKeyspace(t *testing.T) {
	pairs := [][2]model.Aggregate{
		{ksHit("j1", 3), ksHit("j1", 9)},
		{ksHit("j1", 5), ksNoHit("j1")},
		{ksNoHit("j1"), ksNoHit("j1")},
		{ksHit("j1", 4), ksHit("j1", 4)},
		{ksHit("j1", 7), model.Aggregate{}},
	}
	for i, p := range pairs {
		ab := Merge(admission.TemplateKeyspaceSearch, p[0], p[1])
		ba := Merge(admission.TemplateKeyspaceSearch, p[1], p[0])
		if !reflect.DeepEqual(ab, ba) {
			t.Fatalf("pair %d not commutative: Merge(a,b)=%+v Merge(b,a)=%+v", i, ab, ba)
		}
	}
}

func TestMergeCommutativeMonteCarlo(t *testing.T) {
	pairs := [][2]model.Aggregate{
		{mcPartial("j1", 4, 8, 20), mcPartial("j1", 3, 6, 14)},
		{mcPartial("j1", 4, 8, 20), model.Aggregate{}},
		{model.Aggregate{}, model.Aggregate{}},
	}
	for i, p := range pairs {
		ab := decodeMC(t, Merge(admission.TemplateMonteCarlo, p[0], p[1]))
		ba := decodeMC(t, Merge(admission.TemplateMonteCarlo, p[1], p[0]))
		if !mcApproxEqual(ab, ba) {
			t.Fatalf("pair %d not commutative: Merge(a,b)=%+v Merge(b,a)=%+v", i, ab, ba)
		}
	}
}

func TestMergeAssociativeKeyspace(t *testing.T) {
	a := ksHit("j1", 9)
	b := ksNoHit("j1")
	c := ksHit("j1", 3)

	want := Merge(admission.TemplateKeyspaceSearch, Merge(admission.TemplateKeyspaceSearch, a, b), c)
	for _, order := range permutations([]model.Aggregate{a, b, c}) {
		lr := Merge(admission.TemplateKeyspaceSearch, Merge(admission.TemplateKeyspaceSearch, order[0], order[1]), order[2])
		if !reflect.DeepEqual(lr, want) {
			t.Fatalf("not associative/grouping-independent for order %+v: got %+v, want %+v", order, lr, want)
		}
	}
}

func TestMergeAssociativeMonteCarlo(t *testing.T) {
	a := mcPartial("j1", 2, 4, 8)
	b := mcPartial("j1", 3, 9, 27)
	c := mcPartial("j1", 1, 2, 4)

	want := decodeMC(t, Merge(admission.TemplateMonteCarlo, Merge(admission.TemplateMonteCarlo, a, b), c))
	for _, order := range permutations([]model.Aggregate{a, b, c}) {
		lr := decodeMC(t, Merge(admission.TemplateMonteCarlo, Merge(admission.TemplateMonteCarlo, order[0], order[1]), order[2]))
		if !mcApproxEqual(lr, want) {
			t.Fatalf("not associative/grouping-independent for order %+v: got %+v, want %+v", order, lr, want)
		}
	}
}

func TestMergeIdentityKeyspace(t *testing.T) {
	for _, a := range []model.Aggregate{ksHit("j1", 42), ksNoHit("j1"), {}} {
		if got := Merge(admission.TemplateKeyspaceSearch, a, model.Aggregate{}); !reflect.DeepEqual(got, a) {
			t.Fatalf("Merge(a, zero) = %+v, want %+v", got, a)
		}
		if got := Merge(admission.TemplateKeyspaceSearch, model.Aggregate{}, a); !reflect.DeepEqual(got, a) {
			t.Fatalf("Merge(zero, a) = %+v, want %+v", got, a)
		}
	}
}

func TestMergeIdentityMonteCarlo(t *testing.T) {
	for _, a := range []model.Aggregate{mcPartial("j1", 4, 8, 20), {JobID: "j1"}, {}} {
		want := decodeMC(t, a)
		if got := decodeMC(t, Merge(admission.TemplateMonteCarlo, a, model.Aggregate{})); !mcApproxEqual(got, want) {
			t.Fatalf("Merge(a, zero) stats = %+v, want %+v", got, want)
		}
		if got := decodeMC(t, Merge(admission.TemplateMonteCarlo, model.Aggregate{}, a)); !mcApproxEqual(got, want) {
			t.Fatalf("Merge(zero, a) stats = %+v, want %+v", got, want)
		}
	}
}

// -----------------------------------------------------------------------
// Hierarchical == flat (issue #48): rolling up partials in ANY tree
// grouping must equal one flat merge over all leaves.
// -----------------------------------------------------------------------

func TestHierarchicalEqualsFlatKeyspace(t *testing.T) {
	// Exactly one hit among the raw results, so no partition can produce an
	// ambiguous winner regardless of how it groups the miss/hit results.
	results := []model.TaskResult{
		ksRaw("t0", false, 0),
		ksRaw("t1", false, 0),
		ksRaw("t2", true, 42),
		ksRaw("t3", false, 0),
		ksRaw("t4", false, 0),
		ksRaw("t5", false, 0),
	}

	flat := templates.KeyspaceMerge(results)

	partitions := [][][]model.TaskResult{
		{results},
		{results[:3], results[3:]},
		{results[:1], results[1:3], results[3:5], results[5:]},
		{{results[0]}, {results[1]}, {results[2]}, {results[3]}, {results[4]}, {results[5]}},
	}

	for i, groups := range partitions {
		var parts []model.Aggregate
		for _, g := range groups {
			parts = append(parts, templates.KeyspaceMerge(g))
		}
		got := MergeAll(admission.TemplateKeyspaceSearch, parts)
		got.JobID, flat.JobID = "", "" // JobID is not under test here
		got.Done, flat.Done = false, false
		if !reflect.DeepEqual(got, flat) {
			t.Fatalf("partition %d: hierarchical merge = %+v, want (flat) %+v", i, got, flat)
		}
	}
}

func TestHierarchicalEqualsFlatMonteCarlo(t *testing.T) {
	results := []model.TaskResult{
		mcRaw("t0", 1, 2, 4),
		mcRaw("t1", 1, 4, 16),
		mcRaw("t2", 1, 6, 36),
		mcRaw("t3", 1, 8, 64),
		mcRaw("t4", 1, 10, 100),
		mcRaw("t5", 1, 12, 144),
		mcRaw("t6", 1, 14, 196),
		mcRaw("t7", 1, 16, 256),
	}

	flat := decodeMC(t, templates.MonteCarloMerge(results))

	partitions := [][][]model.TaskResult{
		{results},
		{results[:4], results[4:]},
		{results[:1], results[1:3], results[3:6], results[6:]},
		{
			{results[0]}, {results[1]}, {results[2]}, {results[3]},
			{results[4]}, {results[5]}, {results[6]}, {results[7]},
		},
	}

	for i, groups := range partitions {
		var parts []model.Aggregate
		for _, g := range groups {
			parts = append(parts, templates.MonteCarloMerge(g))
		}
		got := decodeMC(t, MergeAll(admission.TemplateMonteCarlo, parts))
		if !mcApproxEqual(got, flat) {
			t.Fatalf("partition %d: hierarchical merge = %+v, want (flat) %+v", i, got, flat)
		}
	}
}

// -----------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------

func TestMergeIsDeterministic(t *testing.T) {
	a, b := ksHit("j1", 9), ksHit("j1", 3)
	first := Merge(admission.TemplateKeyspaceSearch, a, b)
	for i := 0; i < 100; i++ {
		if got := Merge(admission.TemplateKeyspaceSearch, a, b); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestMergeAllIsDeterministic(t *testing.T) {
	parts := []model.Aggregate{mcPartial("j1", 2, 4, 8), mcPartial("j1", 3, 9, 27), mcPartial("j1", 1, 2, 4)}
	first := MergeAll(admission.TemplateMonteCarlo, parts)
	for i := 0; i < 100; i++ {
		if got := MergeAll(admission.TemplateMonteCarlo, parts); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
