package sandbox

import (
	"crypto/ed25519"
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestGrants_Table(t *testing.T) {
	tests := []struct {
		name string
		task model.Task
		want model.WasiCaps
	}{
		{
			name: "no declaration => zero caps, no ambient authority",
			task: model.Task{ID: "t1"},
			want: model.WasiCaps{},
		},
		{
			name: "full declaration => exactly what was declared",
			task: model.Task{
				ID: "t2",
				Declared: model.WasiCaps{
					ReadPaths:  []string{"/data"},
					WritePaths: []string{"/out"},
					Env:        []string{"PATH"},
					Stdio:      true,
					Clock:      true,
				},
			},
			want: model.WasiCaps{
				ReadPaths:  []string{"/data"},
				WritePaths: []string{"/out"},
				Env:        []string{"PATH"},
				Stdio:      true,
				Clock:      true,
			},
		},
		{
			name: "partial declaration => only the declared fields are granted",
			task: model.Task{
				ID: "t3",
				Declared: model.WasiCaps{
					ReadPaths: []string{"/data", "/models"},
				},
			},
			want: model.WasiCaps{
				ReadPaths: []string{"/data", "/models"},
			},
		},
		{
			name: "stdio only",
			task: model.Task{
				ID:       "t4",
				Declared: model.WasiCaps{Stdio: true},
			},
			want: model.WasiCaps{Stdio: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Grants(tt.task)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Grants(%+v) = %+v, want %+v", tt.task, got, tt.want)
			}
		})
	}
}

// TestGrants_Property checks the ticket's named "grants" law: a task
// receives exactly the WASI capabilities it declared, no others. For an
// arbitrary declared-cap input, every declared capability must be present
// in the grant, and no undeclared capability may appear (no ambient
// authority picked up by omission).
func TestGrants_Property(t *testing.T) {
	declarations := []model.WasiCaps{
		{},
		{ReadPaths: []string{"/a"}},
		{WritePaths: []string{"/b"}},
		{Env: []string{"X", "Y"}},
		{Stdio: true},
		{Clock: true},
		{ReadPaths: []string{"/a", "/c"}, WritePaths: []string{"/b"}, Env: []string{"X"}, Stdio: true, Clock: true},
		{ReadPaths: []string{"/only-read"}},
	}

	for i, d := range declarations {
		task := model.Task{ID: model.TaskID("prop"), Declared: d}
		got := Grants(task)

		// Every declared capability must be present.
		if !sameStrings(got.ReadPaths, d.ReadPaths) {
			t.Errorf("case %d: ReadPaths = %v, want %v (declared cap missing/altered)", i, got.ReadPaths, d.ReadPaths)
		}
		if !sameStrings(got.WritePaths, d.WritePaths) {
			t.Errorf("case %d: WritePaths = %v, want %v", i, got.WritePaths, d.WritePaths)
		}
		if !sameStrings(got.Env, d.Env) {
			t.Errorf("case %d: Env = %v, want %v", i, got.Env, d.Env)
		}
		if got.Stdio != d.Stdio {
			t.Errorf("case %d: Stdio = %v, want %v", i, got.Stdio, d.Stdio)
		}
		if got.Clock != d.Clock {
			t.Errorf("case %d: Clock = %v, want %v", i, got.Clock, d.Clock)
		}

		// No ambient authority: an empty declaration must yield a
		// completely zero grant.
		if isZeroCaps(d) && !isZeroCaps(got) {
			t.Errorf("case %d: empty declaration granted non-zero caps: %+v", i, got)
		}
	}
}

// TestGrants_NoAliasing ensures the grant does not share backing arrays
// with the task's declaration: mutating one must not affect the other.
func TestGrants_NoAliasing(t *testing.T) {
	task := model.Task{
		Declared: model.WasiCaps{
			ReadPaths: []string{"/data"},
		},
	}
	got := Grants(task)
	got.ReadPaths[0] = "/mutated"

	if task.Declared.ReadPaths[0] != "/data" {
		t.Fatalf("mutating the grant mutated the task's declaration: %v", task.Declared.ReadPaths)
	}
}

func TestGrants_Deterministic(t *testing.T) {
	task := model.Task{
		ID: "det",
		Declared: model.WasiCaps{
			ReadPaths:  []string{"/data"},
			WritePaths: []string{"/out"},
			Env:        []string{"PATH", "HOME"},
			Stdio:      true,
			Clock:      true,
		},
	}

	first := Grants(task)
	for i := 0; i < 50; i++ {
		got := Grants(task)
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("Grants is not deterministic: iteration %d = %+v, first = %+v", i, got, first)
		}
	}
}

func sameStrings(a, b []string) bool {
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

func isZeroCaps(c model.WasiCaps) bool {
	return len(c.ReadPaths) == 0 && len(c.WritePaths) == 0 && len(c.Env) == 0 && !c.Stdio && !c.Clock
}

func TestVerifyModule_Table(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	mod := []byte("wasm module bytes go here")
	sig := ed25519.Sign(priv, mod)

	tampered := append([]byte(nil), mod...)
	tampered[0] ^= 0xFF

	tamperedSig := append([]byte(nil), sig...)
	tamperedSig[0] ^= 0xFF

	tests := []struct {
		name string
		mod  []byte
		sig  model.Sig
		key  model.PubKey
		want bool
	}{
		{
			name: "genuine signature => true",
			mod:  mod,
			sig:  model.Sig(sig),
			key:  model.PubKey(pub),
			want: true,
		},
		{
			name: "wrong key => false",
			mod:  mod,
			sig:  model.Sig(sig),
			key:  model.PubKey(otherPub),
			want: false,
		},
		{
			name: "flipped module byte => false",
			mod:  tampered,
			sig:  model.Sig(sig),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "flipped signature byte => false",
			mod:  mod,
			sig:  model.Sig(tamperedSig),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "truncated signature => false, no panic",
			mod:  mod,
			sig:  model.Sig(sig[:len(sig)-1]),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "truncated key => false, no panic",
			mod:  mod,
			sig:  model.Sig(sig),
			key:  model.PubKey(pub[:len(pub)-1]),
			want: false,
		},
		{
			name: "nil module => false",
			mod:  nil,
			sig:  model.Sig(sig),
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "nil signature => false, no panic",
			mod:  mod,
			sig:  nil,
			key:  model.PubKey(pub),
			want: false,
		},
		{
			name: "nil key => false, no panic",
			mod:  mod,
			sig:  model.Sig(sig),
			key:  nil,
			want: false,
		},
		{
			name: "all nil => false, no panic",
			mod:  nil,
			sig:  nil,
			key:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := VerifyModule(tt.mod, tt.sig, tt.key)
			if got != tt.want {
				t.Fatalf("VerifyModule(...) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestVerifyModule_Deterministic(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	mod := []byte("deterministic module bytes")
	sig := ed25519.Sign(priv, mod)

	first := VerifyModule(mod, model.Sig(sig), model.PubKey(pub))
	for i := 0; i < 50; i++ {
		got := VerifyModule(mod, model.Sig(sig), model.PubKey(pub))
		if got != first {
			t.Fatalf("VerifyModule is not deterministic: iteration %d = %v, first = %v", i, got, first)
		}
	}
}
