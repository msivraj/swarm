// Package rendezvous is a pure core: it decides where a dialing agent lands.
// It performs no I/O and reads no clock — it is a pure function of a join
// request and a point-in-time snapshot of the fleet's cells. This package
// follows the shape set by internal/core/mitosis: take data, return a
// decision, never execute an effect.
package rendezvous

import "github.com/msivraj/swarm/internal/model"

// AgentID uniquely identifies a dialing agent.
type AgentID string

// JoinReq is a dialing agent's request to join the fleet: its identity, the
// region it dialed from, and the capacity it needs from a cell.
type JoinReq struct {
	Agent  AgentID
	Region string
	Caps   int // capacity slots the agent will occupy once admitted
}

// Kind is the tag of an AdmitDecision's tagged union.
type Kind int

const (
	// Reject refuses the request; Reason says why.
	Reject Kind = iota
	// Accept admits the agent into Cell.
	Accept
	// NewCell forms a new cell for this agent.
	NewCell
)

// AdmitDecision is a rendezvous decision the shell will execute. It is a
// tagged union: Accept{Cell} | Reject{Reason} | NewCell. Cores return
// AdmitDecisions; they never carry them out.
type AdmitDecision struct {
	Kind   Kind
	Cell   model.CellID // set when Kind == Accept
	Reason string       // set when Kind == Reject
}

// AdmitAgent decides where a dialing agent goes given a cell snapshot.
//
// P0 admission policy (the doc leaves the exact rule unspecified; this
// resolves that ambiguity):
//  1. Reject an ineligible request outright — an empty Agent identity — before
//     looking at any cell. This is the only rejection rule in P0; a request
//     with a valid identity that simply finds no room is not "ineligible".
//  2. Otherwise Accept into the first cell in slice order with Free >= the
//     agent's requested Caps — deterministic first-fit, mirroring
//     placement.Place's convention of deciding on slice order rather than
//     cell identity or load.
//  3. If no cell has room (including the empty-registry case), return
//     NewCell rather than Reject: a fleet with no space should grow, not
//     turn agents away. Reject is reserved for ineligible requests.
func AdmitAgent(req JoinReq, reg []model.CellView) AdmitDecision {
	if req.Agent == "" {
		return AdmitDecision{Kind: Reject, Reason: "empty agent identity"}
	}

	for _, c := range reg {
		if c.Free >= req.Caps {
			return AdmitDecision{Kind: Accept, Cell: c.ID}
		}
	}
	return AdmitDecision{Kind: NewCell}
}
