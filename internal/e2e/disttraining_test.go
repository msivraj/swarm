package e2e

import (
	"bytes"
	"os/exec"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/templates"
)

// TestDTCodecRoundTrip checks Decode(Encode(x)) == x for both the
// dist-training stdin wire format and the gradient wire format, across a
// table covering step 0's empty-incoming case, a single-parameter vector,
// and a multi-parameter vector.
func TestDTCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name             string
		start, end, step uint64
		incoming         []float64
	}{
		{name: "step 0, no incoming gradient", start: 0, end: 100, step: 0, incoming: nil},
		{name: "single parameter", start: 10, end: 20, step: 1, incoming: []float64{3.5}},
		{name: "multiple parameters", start: 100, end: 137, step: 7, incoming: []float64{1, -2.25, 3.75, 0}},
		{name: "negative and zero values", start: 0, end: 1, step: 42, incoming: []float64{0, -0.001, 1e10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wire := EncodeDTStdin(tt.start, tt.end, tt.step, tt.incoming)
			gotStart, gotEnd, gotStep, gotIncoming, ok := DecodeDTStdin(wire)
			if !ok {
				t.Fatalf("DecodeDTStdin(EncodeDTStdin(...)) ok = false")
			}
			if gotStart != tt.start || gotEnd != tt.end || gotStep != tt.step {
				t.Fatalf("DecodeDTStdin() range/step = (%d, %d, %d), want (%d, %d, %d)",
					gotStart, gotEnd, gotStep, tt.start, tt.end, tt.step)
			}
			if !reflect.DeepEqual(normalizeVec(gotIncoming), normalizeVec(tt.incoming)) {
				t.Fatalf("DecodeDTStdin() incoming = %v, want %v", gotIncoming, tt.incoming)
			}

			gradWire := EncodeGradient(tt.incoming)
			gotGrad, ok := DecodeGradient(gradWire)
			if !ok {
				t.Fatalf("DecodeGradient(EncodeGradient(...)) ok = false")
			}
			if !reflect.DeepEqual(normalizeVec(gotGrad), normalizeVec(tt.incoming)) {
				t.Fatalf("DecodeGradient(EncodeGradient()) = %v, want %v", gotGrad, tt.incoming)
			}
		})
	}
}

// TestDTStdinDecodeRejectsMalformed checks DecodeDTStdin and DecodeGradient
// reject inputs that are too short or not a whole number of float64s.
func TestDTStdinDecodeRejectsMalformed(t *testing.T) {
	if _, _, _, _, ok := DecodeDTStdin(nil); ok {
		t.Fatalf("DecodeDTStdin(nil) ok = true, want false")
	}
	if _, _, _, _, ok := DecodeDTStdin(make([]byte, 23)); ok {
		t.Fatalf("DecodeDTStdin(23 bytes) ok = true, want false")
	}
	if _, _, _, _, ok := DecodeDTStdin(make([]byte, 27)); ok {
		t.Fatalf("DecodeDTStdin(27 bytes, trailing gradient not a multiple of 8) ok = true, want false")
	}
	if _, ok := DecodeGradient(make([]byte, 5)); ok {
		t.Fatalf("DecodeGradient(5 bytes) ok = true, want false")
	}
}

// normalizeVec treats nil and empty slices the same, since EncodeGradient
// intentionally produces a non-nil zero-length slice for a nil input.
func normalizeVec(v []float64) []float64 {
	if len(v) == 0 {
		return []float64{}
	}
	return v
}

// TestDTWorkerDeterministic builds the disttraining worker and pipes it the
// same stdin twice, asserting the process's stdout is byte-identical both
// times and equals EncodeGradient(DTPartial(...)) computed directly — the
// worker does no more and no less than the pure DTPartial function.
func TestDTWorkerDeterministic(t *testing.T) {
	worker := buildWorker(t, "./workers/disttraining", "disttraining")

	const start, end, step = 1000, 1137, 3
	incoming := []float64{0.5, -1.25, 2.0, 10}
	stdin := EncodeDTStdin(start, end, step, incoming)

	want := EncodeGradient(DTPartial(start, end, step, incoming))

	first := runWorker(t, worker, stdin)
	second := runWorker(t, worker, stdin)

	if !bytes.Equal(first, second) {
		t.Fatalf("worker stdout not byte-identical across runs:\n%x\nvs\n%x", first, second)
	}
	if !bytes.Equal(first, want) {
		t.Fatalf("worker stdout = %x, want EncodeGradient(DTPartial(...)) = %x", first, want)
	}
}

// TestDTWorkerStepZeroNoIncoming checks the worker handles step 0's empty
// incoming gradient (the case DTPartial falls back to the fixed
// dtDimension for) end to end through the real process.
func TestDTWorkerStepZeroNoIncoming(t *testing.T) {
	worker := buildWorker(t, "./workers/disttraining", "disttraining")

	const start, end, step = 0, 50, 0
	stdin := EncodeDTStdin(start, end, step, nil)

	got := runWorker(t, worker, stdin)
	want := EncodeGradient(DTPartial(start, end, step, nil))

	if !bytes.Equal(got, want) {
		t.Fatalf("worker stdout = %x, want %x", got, want)
	}
	gotVec, ok := DecodeGradient(got)
	if !ok || len(gotVec) != dtDimension {
		t.Fatalf("DecodeGradient(worker stdout) = %v, %v, want a %d-element vector", gotVec, ok, dtDimension)
	}
}

// TestDTWorkerSumMatchesCombine proves the worker binary and the pure
// templates.DistTrainingCombine core agree: it runs the worker once per
// shard, feeds all the workers' real stdout into DistTrainingCombine, and
// checks the result equals ExpectedAllReducedGradient — the same shards and
// step, computed independently via DTPartial without spawning anything.
func TestDTWorkerSumMatchesCombine(t *testing.T) {
	worker := buildWorker(t, "./workers/disttraining", "disttraining")

	shards := [][2]uint64{
		{0, 25},
		{25, 60},
		{60, 137},
		{137, 200},
	}
	const step = 2
	incoming := []float64{1, 2, 3, 4}

	gradients := make([][]byte, 0, len(shards))
	for _, shard := range shards {
		stdin := EncodeDTStdin(shard[0], shard[1], step, incoming)
		gradients = append(gradients, runWorker(t, worker, stdin))
	}

	combined := templates.DistTrainingCombine(gradients)
	gotVec, ok := decodeGradientForTest(combined)
	if !ok {
		t.Fatalf("DistTrainingCombine() output is not a valid gradient")
	}

	want := ExpectedAllReducedGradient(shards, step, incoming)
	if !reflect.DeepEqual(gotVec, want) {
		t.Fatalf("sum of worker gradients via DistTrainingCombine = %v, want %v (ExpectedAllReducedGradient)", gotVec, want)
	}
}

// decodeGradientForTest is DecodeGradient without the "empty is valid"
// carve-out, since templates.DistTrainingCombine's own doc says a nil
// result means "no vector to report" rather than a zero-length one.
func decodeGradientForTest(b []byte) ([]float64, bool) {
	if len(b) == 0 {
		return nil, false
	}
	return DecodeGradient(b)
}

// runWorker execs the built worker binary at path with stdin piped in and
// returns its stdout, failing the test if the process exits non-zero.
func runWorker(t *testing.T, path string, stdin []byte) []byte {
	t.Helper()
	cmd := exec.Command(path)
	cmd.Stdin = bytes.NewReader(stdin)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("running %s: %v", path, err)
	}
	return out
}
