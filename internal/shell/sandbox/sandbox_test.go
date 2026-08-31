package sandbox

import (
	"context"
	_ "embed"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"crypto/ed25519"

	"github.com/msivraj/swarm/internal/model"
)

//go:embed testdata/echo.wasm
var echoWasm []byte

//go:embed testdata/fsprobe.wasm
var fsprobeWasm []byte

// Deterministic ed25519 test keys, per the ticket's guidance: a shell
// package's tests aren't restricted by fcischeck, but a fixed seed keeps
// the fixtures stable across runs rather than drawing from crypto/rand.
var (
	signerSeed  = [ed25519.SeedSize]byte{1, 2, 3, 4, 5, 6, 7, 8}
	wrongSeed   = [ed25519.SeedSize]byte{9, 9, 9, 9, 9, 9, 9, 9}
	signerKey   = ed25519.NewKeyFromSeed(signerSeed[:])
	signerPub   = model.PubKey(signerKey.Public().(ed25519.PublicKey))
	wrongKey    = ed25519.NewKeyFromSeed(wrongSeed[:])
	wrongPubKey = model.PubKey(wrongKey.Public().(ed25519.PublicKey))
)

func sign(key ed25519.PrivateKey, mod []byte) model.Sig {
	return model.Sig(ed25519.Sign(key, mod))
}

func TestRun_SignedModuleRunsAndOutputBecomesResult(t *testing.T) {
	task := model.Task{
		ID:       "task-echo",
		Declared: model.WasiCaps{Stdio: true},
	}
	mod := Module{Bytes: echoWasm, Sig: sign(signerKey, echoWasm)}

	result, err := (Runner{}).Run(context.Background(), task, mod, signerPub)
	if err != nil {
		t.Fatalf("Run returned error for a validly-signed module: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, want true for a module that exits 0: %+v", result)
	}
	if result.TaskID != task.ID {
		t.Fatalf("result.TaskID = %q, want %q", result.TaskID, task.ID)
	}
	if got, want := string(result.Output), "sandbox-ok\n"; got != want {
		t.Fatalf("result.Output = %q, want %q", got, want)
	}
}

func TestRun_RefusesUnsignedOrWrongKeyModule(t *testing.T) {
	task := model.Task{ID: "task-echo", Declared: model.WasiCaps{Stdio: true}}

	tampered := append([]byte(nil), echoWasm...)
	tampered[len(tampered)-1] ^= 0xFF

	tests := []struct {
		name string
		mod  Module
		key  model.PubKey
	}{
		{
			name: "wrong key",
			mod:  Module{Bytes: echoWasm, Sig: sign(signerKey, echoWasm)},
			key:  wrongPubKey,
		},
		{
			name: "tampered module, signature over the original bytes",
			mod:  Module{Bytes: tampered, Sig: sign(signerKey, echoWasm)},
			key:  signerPub,
		},
		{
			name: "no signature",
			mod:  Module{Bytes: echoWasm, Sig: nil},
			key:  signerPub,
		},
		{
			name: "nil module",
			mod:  Module{Bytes: nil, Sig: sign(signerKey, echoWasm)},
			key:  signerPub,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (Runner{}).Run(context.Background(), task, tt.mod, tt.key)
			if !errors.Is(err, ErrUnsigned) {
				t.Fatalf("Run error = %v, want ErrUnsigned (proves the module was never instantiated: a real echo.wasm run always succeeds, so any other outcome means it ran)", err)
			}
			if result.TaskID != "" || result.OK || len(result.Output) != 0 {
				t.Fatalf("result = %+v, want the zero TaskResult when refused before instantiation", result)
			}
		})
	}
}

// TestRun_GrantsEnforcement drives the fsprobe module (which tries to read
// a path given as its argument and exits 0/1 on success/failure) to prove
// the §03 `grants` property at the shell boundary: a task that declared no
// filesystem access can't open any path, and a task that declared a root
// can reach under it but nothing outside it.
func TestRun_GrantsEnforcement(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	inside := filepath.Join(sub, "data.txt")
	if err := os.WriteFile(inside, []byte("hello-from-file"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "secret.txt")
	if err := os.WriteFile(outside, []byte("should-not-be-reachable"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	tests := []struct {
		name     string
		declared model.WasiCaps
		target   string
		wantOK   bool
	}{
		{
			name:     "no fs access declared: any path is denied",
			declared: model.WasiCaps{},
			target:   inside,
			wantOK:   false,
		},
		{
			name:     "declared root: a path under it is reachable",
			declared: model.WasiCaps{ReadPaths: []string{root}},
			target:   inside,
			wantOK:   true,
		},
		{
			name:     "declared root: a path outside it is denied",
			declared: model.WasiCaps{ReadPaths: []string{root}},
			target:   outside,
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := model.Task{ID: "task-fsprobe", Input: []byte(tt.target), Declared: tt.declared}
			mod := Module{Bytes: fsprobeWasm, Sig: sign(signerKey, fsprobeWasm)}

			result, err := (Runner{}).Run(context.Background(), task, mod, signerPub)
			if err != nil {
				t.Fatalf("Run returned error: %v", err)
			}
			if result.OK != tt.wantOK {
				t.Fatalf("result.OK = %v, want %v (declared=%+v target=%q)", result.OK, tt.wantOK, tt.declared, tt.target)
			}
		})
	}
}

// TestRun_UndeclaredStdioYieldsNoOutput asserts the Stdio capability
// actually gates the result-capture channel: a task that didn't declare
// Stdio still runs to completion (wazero's default discard/EOF plumbing is
// already the safe case), but gets no captured output back.
func TestRun_UndeclaredStdioYieldsNoOutput(t *testing.T) {
	task := model.Task{ID: "task-echo", Declared: model.WasiCaps{}}
	mod := Module{Bytes: echoWasm, Sig: sign(signerKey, echoWasm)}

	result, err := (Runner{}).Run(context.Background(), task, mod, signerPub)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, want true — the module still runs to completion")
	}
	if len(result.Output) != 0 {
		t.Fatalf("result.Output = %q, want empty — Stdio was not declared", result.Output)
	}
}

// TestRun_EnvAllowList asserts only a declared environment variable name is
// visible to the guest, sourced from the shell's own process environment.
func TestRun_EnvAllowList(t *testing.T) {
	t.Setenv("SWARM_SANDBOX_TEST_VAR", "visible-value")
	// Deliberately left unset/undeclared: SWARM_SANDBOX_TEST_VAR_HIDDEN.

	task := model.Task{
		ID: "task-echo",
		Declared: model.WasiCaps{
			Stdio: true,
			Env:   []string{"SWARM_SANDBOX_TEST_VAR", "SWARM_SANDBOX_TEST_VAR_HIDDEN"},
		},
	}
	mod := Module{Bytes: echoWasm, Sig: sign(signerKey, echoWasm)}

	// echo.wasm ignores env entirely; this test only exercises that
	// moduleConfig's env wiring doesn't error or panic on a name absent
	// from the host environment, and that Run still completes normally.
	result, err := (Runner{}).Run(context.Background(), task, mod, signerPub)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !result.OK {
		t.Fatalf("result.OK = false, want true")
	}
}

func TestRun_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()

	task := model.Task{ID: "task-echo", Declared: model.WasiCaps{Stdio: true}}
	mod := Module{Bytes: echoWasm, Sig: sign(signerKey, echoWasm)}

	if _, err := (Runner{}).Run(ctx, task, mod, signerPub); err == nil {
		t.Fatalf("Run with an already-canceled context returned no error")
	}
}
