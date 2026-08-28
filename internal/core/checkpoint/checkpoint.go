// Package checkpoint is a pure core: it decides when a driver should
// checkpoint (cadence) and it (de)serializes a driver-agnostic checkpoint
// payload to and from bytes. It performs no I/O and reads no clock — writing
// the resulting bytes to an object store, and reading them back on rollback
// or resume, is a shell concern (a later ticket).
package checkpoint

import "encoding/json"

// State is a driver-agnostic checkpoint payload: the step it pins plus the
// opaque serialized driver state. It is deliberately decoupled from any
// specific driver's own checkpoint type (e.g. barrier's Checkpoint) so the
// round-trip law — Restore(Snapshot(s)) == s — is a single law over a single,
// self-contained type that any driver can reuse.
type State struct {
	Step       int               // the step this checkpoint pins
	Members    []string          // membership at the checkpoint
	DriverBlob []byte            // opaque driver-specific bytes
	Meta       map[string]string // optional small key/values; nil-safe
}

// Due reports whether `step` is a checkpoint-cadence step under K: a
// checkpoint is due when step%K==0. K<=0 means "never checkpoint" rather than
// a divide-by-zero panic — the same guard barrier's isCheckpointStep uses
// (decision D), and step 0 is a checkpoint step under the literal rule
// (decision F).
func Due(step int, k int) bool {
	if k <= 0 {
		return false
	}
	return step%k == 0
}

// Snapshot serializes State to bytes using encoding/json. Two properties this
// choice buys for free:
//
//   - Determinism: encoding/json always emits object keys in a struct's
//     declared field order, and always sorts map[string]V keys lexically
//     before writing them. So Snapshot(s) is byte-identical across repeated
//     calls, with no map-iteration nondeterminism leaking through.
//   - Exact round trip: encoding/json distinguishes a nil slice/map/[]byte
//     (encodes as JSON null, decodes back to nil) from a non-nil empty one
//     (encodes as "[]"/"{}"/ "", decodes back to a non-nil empty value) for
//     both directions. Members, Meta, and DriverBlob therefore preserve the
//     nil-vs-empty distinction across Snapshot/Restore without any extra
//     normalization step.
//
// Snapshot never fails: State's fields (int, []string, []byte,
// map[string]string) are all unconditionally JSON-encodable, so a Marshal
// error here would indicate a bug in this package rather than a bad input;
// that case degrades to a nil payload rather than a panic.
func Snapshot(s State) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		return nil
	}
	return b
}

// Restore deserializes bytes produced by Snapshot back into a State. It is
// Snapshot's exact inverse for any State produced by this package: for any
// State s, Restore(Snapshot(s)) == s. A malformed or empty payload decodes to
// the zero State rather than panicking, since Restore has no error return to
// surface a decode failure.
func Restore(b []byte) State {
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}
	}
	return s
}
