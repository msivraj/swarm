// This file adds whole-value serialization to Registry, beside the P0
// Apply/Snapshot and the P4 ShardOf above: the FCIS proof this ticket (#166)
// establishes is that persisting a Registry to a real store adds exactly a
// pure encode/decode pair — Apply, Snapshot and ShardOf stay untouched.
//
// Registry is opaque by design (see registry.go): its per-cell state,
// including agent membership, is held in unexported fields so nothing
// outside the package can construct or mutate it except through Apply. A
// store that persists the registry whole-value (rather than replaying
// events) still needs to get the exact same unexported state back out —
// including which AgentIDs belong to which cell, not just the derived
// Size/Free a Snapshot exposes — so a later AgentJoined/AgentLeft for a real
// agent resolves identically whether the Registry came from Apply or from a
// decode. That requires reaching the unexported fields, so this lives inside
// the registry package rather than in a shell adapter.
//
// GobEncode/GobDecode project Registry to and from a serializable shape
// (cellRecord, keyed by CellID) and hand it to encoding/gob — stdlib only,
// no I/O, no clock, no randomness, so this file stays fcischeck-clean.
package registry

import (
	"bytes"
	"encoding/gob"

	"github.com/msivraj/swarm/internal/model"
)

// cellRecord is the serializable projection of one cell's state: its
// capacity and the set of AgentIDs currently members, as a slice (gob can't
// encode a map keyed by a package-local named string type registered only
// implicitly, and a slice makes the wire shape explicit and stable).
type cellRecord struct {
	Capacity int
	Agents   []AgentID
}

// registryWire is the serializable projection of an entire Registry: one
// cellRecord per cell, keyed by CellID.
type registryWire struct {
	Cells map[model.CellID]cellRecord
}

// GobEncode implements gob.GobEncoder: it projects r's unexported state into
// registryWire and gob-encodes that. A nil/empty Registry encodes to a valid,
// empty registryWire — GobDecode of the result restores an empty Registry,
// not an error.
func (r Registry) GobEncode() ([]byte, error) {
	wire := registryWire{Cells: make(map[model.CellID]cellRecord, len(r.cells))}
	for id, cs := range r.cells {
		agents := make([]AgentID, 0, len(cs.agents))
		for a := range cs.agents {
			agents = append(agents, a)
		}
		wire.Cells[id] = cellRecord{Capacity: cs.capacity, Agents: agents}
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(wire); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GobDecode implements gob.GobDecoder: it decodes data into a registryWire
// and rebuilds r's unexported state from it, the reverse of GobEncode. It
// overwrites r's receiver, matching the encoding/gob.GobDecoder contract.
func (r *Registry) GobDecode(data []byte) error {
	var wire registryWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&wire); err != nil {
		return err
	}

	cells := make(map[model.CellID]cellState, len(wire.Cells))
	for id, rec := range wire.Cells {
		agents := make(map[AgentID]struct{}, len(rec.Agents))
		for _, a := range rec.Agents {
			agents[a] = struct{}{}
		}
		cells[id] = cellState{capacity: rec.Capacity, agents: agents}
	}
	r.cells = cells
	return nil
}
