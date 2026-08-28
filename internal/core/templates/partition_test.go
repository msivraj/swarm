package templates

import (
	"reflect"
	"testing"
)

func TestPartitionRange(t *testing.T) {
	tests := []struct {
		name    string
		total   uint64
		parts   int
		wantErr bool
		want    []idRange
	}{
		{
			name:  "even split",
			total: 9, parts: 3,
			want: []idRange{{0, 3}, {3, 6}, {6, 9}},
		},
		{
			name:  "uneven split spreads remainder over first parts",
			total: 10, parts: 3,
			want: []idRange{{0, 4}, {4, 7}, {7, 10}},
		},
		{
			name:  "single part covers the whole space",
			total: 20, parts: 1,
			want: []idRange{{0, 20}},
		},
		{
			name:  "parts equals total: one item per part",
			total: 3, parts: 3,
			want: []idRange{{0, 1}, {1, 2}, {2, 3}},
		},
		{name: "total zero is rejected", total: 0, parts: 4, wantErr: true},
		{name: "parts zero is rejected", total: 10, parts: 0, wantErr: true},
		{name: "negative parts is rejected", total: 10, parts: -1, wantErr: true},
		{name: "parts exceeding total is rejected", total: 3, parts: 4, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := partitionRange(tt.total, tt.parts, "test")
			if tt.wantErr {
				if err == nil {
					t.Fatalf("partitionRange() error = nil, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("partitionRange() unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("partitionRange() = %+v, want %+v", got, tt.want)
			}
			assertTiling(t, tt.total, got)
		})
	}
}

// assertTiling checks the property every partition must satisfy: ranges are
// contiguous, non-overlapping, and together cover exactly [0, total) with no
// gaps — the same tiling law assertKeyspaceTiling checks for KeyspaceJob.
func assertTiling(t *testing.T, total uint64, ranges []idRange) {
	t.Helper()
	var cur uint64
	for i, r := range ranges {
		if r.Start != cur {
			t.Fatalf("range %d starts at %d, want %d (gap or overlap)", i, r.Start, cur)
		}
		if r.End <= r.Start {
			t.Fatalf("range %d is empty or inverted: [%d, %d)", i, r.Start, r.End)
		}
		cur = r.End
	}
	if cur != total {
		t.Fatalf("ranges cover up to %d, want %d", cur, total)
	}
}

func TestIDRangeBytesRoundTrip(t *testing.T) {
	r := idRange{Start: 7, End: 1009}
	got, ok := decodeIDRange(r.bytes())
	if !ok {
		t.Fatalf("decodeIDRange rejected a well-formed range")
	}
	if got != r {
		t.Fatalf("decodeIDRange() = %+v, want %+v", got, r)
	}
}

func TestDecodeIDRangeRejectsWrongLength(t *testing.T) {
	if _, ok := decodeIDRange([]byte("too-short")); ok {
		t.Fatal("decodeIDRange accepted a malformed input")
	}
}

func TestPartitionRangeIsDeterministic(t *testing.T) {
	first, err := partitionRange(137, 11, "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := partitionRange(137, 11, "test")
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}
