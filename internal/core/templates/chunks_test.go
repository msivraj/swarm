package templates

import (
	"reflect"
	"testing"
)

func TestPackUnpackChunksRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		chunks [][]byte
	}{
		{"no chunks", nil},
		{"one chunk", [][]byte{[]byte("hello")}},
		{"several chunks, including an empty one", [][]byte{[]byte("a"), {}, []byte("ccc")}},
		{"all empty chunks", [][]byte{{}, {}, {}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := packChunks(tt.chunks)
			got, ok := unpackChunks(packed)
			if !ok {
				t.Fatalf("unpackChunks rejected packChunks's own output")
			}
			want := tt.chunks
			if want == nil {
				want = [][]byte{}
			}
			if len(got) != len(want) {
				t.Fatalf("unpackChunks() = %d chunks, want %d", len(got), len(want))
			}
			for i := range want {
				if !reflect.DeepEqual(got[i], want[i]) && !(len(got[i]) == 0 && len(want[i]) == 0) {
					t.Fatalf("chunk %d = %q, want %q", i, got[i], want[i])
				}
			}
		})
	}
}

func TestUnpackChunksRejectsMalformed(t *testing.T) {
	tests := []struct {
		name   string
		packed []byte
	}{
		{"truncated length prefix", []byte{0, 0}},
		{"length overruns remaining bytes", []byte{0, 0, 0, 10, 'a', 'b'}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := unpackChunks(tt.packed); ok {
				t.Fatal("unpackChunks accepted a malformed stream")
			}
		})
	}
}

func TestPackChunksIsDeterministic(t *testing.T) {
	chunks := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	first := packChunks(chunks)
	for i := 0; i < 100; i++ {
		if got := packChunks(chunks); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d", i)
		}
	}
}
