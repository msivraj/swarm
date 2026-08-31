// Package sandbox is a pure core: it computes the least-privilege WASI
// capability grant for a task and verifies a WASM module's signature. It
// performs no I/O and reads no clock — instantiating wazero with the
// computed grant, running the module, and refusing to run when
// VerifyModule is false all belong to the shell (a separate ticket).
//
// See docs/phases/swarm-p3-components.txt §02 (WASM SANDBOX RUNNER) and §03
// (the `grants` property: "a task receives exactly the WASI capabilities it
// declared, no others").
package sandbox

import (
	"crypto/ed25519"

	"github.com/msivraj/swarm/internal/model"
)

// Grants returns the least-privilege WASI capability set for task: exactly
// what task.Declared carries, no more and no less. A task that declares
// nothing (the zero-value model.WasiCaps) is granted nothing — there is no
// ambient/default authority a task can pick up by omission.
//
// Grants copies every slice field so the returned WasiCaps shares no
// backing array with the task's declaration: callers can't widen a task's
// privilege after the fact by mutating the grant, or vice versa.
func Grants(task model.Task) model.WasiCaps {
	d := task.Declared
	return model.WasiCaps{
		ReadPaths:  copyStrings(d.ReadPaths),
		WritePaths: copyStrings(d.WritePaths),
		Env:        copyStrings(d.Env),
		Stdio:      d.Stdio,
		Clock:      d.Clock,
	}
}

func copyStrings(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

// VerifyModule reports whether mod is validly signed by sig under key,
// using stdlib ed25519. Any malformed input — a key or signature of the
// wrong length, or a nil module — is refused (false) rather than causing a
// panic.
func VerifyModule(mod []byte, sig model.Sig, key model.PubKey) bool {
	if len(key) != ed25519.PublicKeySize {
		return false
	}
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(key), mod, []byte(sig))
}
