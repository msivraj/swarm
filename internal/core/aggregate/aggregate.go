// Package aggregate is a pure core: the level-agnostic, associative and
// commutative monoid that combines partial Aggregates at every tier of the
// roll-up tree (worker -> cell -> region -> global). It performs no I/O and
// reads no clock or randomness — Merge is a pure function of two
// model.Aggregates and the job's template name. It follows the shape set by
// internal/core/mitosis: take data, return a value, never execute an effect.
//
// aggregate is a thin dispatcher: it owns the cross-template algebra (how
// JobID and Done combine, and that the zero Aggregate is the identity), and
// delegates the template-specific Value combine to internal/core/templates,
// which is where each template's byte layout is single-sourced (issue #48).
package aggregate

import (
	"bytes"

	"github.com/msivraj/swarm/internal/core/admission"
	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
)

// combiners maps a known JobSpec.Template to the function that merges two
// partial Aggregates' Value for that template. Merge's template dispatch is
// this map's membership test — adding a template means adding one entry
// here, mirroring admission.templateDecomposers' shape.
var combiners = map[string]func(a, b model.Aggregate) model.Aggregate{
	admission.TemplateKeyspaceSearch: templates.KeyspaceCombine,
	admission.TemplateMonteCarlo:     templates.MonteCarloCombine,
}

// Merge combines two partial Aggregates for a job of the given template into
// one. It is a commutative monoid whose identity is the zero Aggregate
// (JobID=="", Value==nil, Done==false). template is one of
// admission.TemplateKeyspaceSearch / admission.TemplateMonteCarlo — an
// Aggregate carries no template tag of its own, so the tier (which always
// knows the job's template) passes it in. Merge is the same function at
// every tier: cell-partials merge into a region-partial, region-partials
// merge into the global final, by repeated calls to this one function.
//
// JobID propagates (mergeJobID): the non-empty JobID wins, so empty (the
// zero Aggregate's) is the identity; a real caller only ever merges partials
// of the same job, so the tiebreak between two different non-empty JobIDs
// only matters for keeping Merge total.
//
// Done combines by OR, so the zero Aggregate (Done==false) is a true
// identity for it — the tier stamps a partial's own Done==true when its own
// fan-in completes (a distinct-count gate), not Merge.
//
// Value is dispatched to the named template's combine function
// (templates.KeyspaceCombine / templates.MonteCarloCombine). An unrecognized
// template has no known algebra, so its Value combines via identityValue: a
// min-reduction over the raw bytes that keeps Merge total, commutative, and
// associative even for a template name it does not recognize. The two known
// templates are what issue #48's required law tests exercise.
func Merge(template string, a, b model.Aggregate) model.Aggregate {
	out := model.Aggregate{
		JobID: mergeJobID(a.JobID, b.JobID),
		Done:  a.Done || b.Done,
	}

	if combine, ok := combiners[template]; ok {
		out.Value = combine(a, b).Value
		return out
	}
	out.Value = identityValue(a.Value, b.Value)
	return out
}

// MergeAll folds parts into one partial via repeated Merge. Because Merge is
// commutative and associative with the zero Aggregate as its identity, the
// result does not depend on parts' order or on how they are grouped —
// MergeAll(t, nil) == the zero Aggregate, so an empty tier contributes
// nothing to the level above it.
func MergeAll(template string, parts []model.Aggregate) model.Aggregate {
	var out model.Aggregate
	for _, p := range parts {
		out = Merge(template, out, p)
	}
	return out
}

// mergeJobID picks the JobID two partials propagate upward. A real caller
// only ever merges partials of the same job, so this only decides an
// outcome when one side is the zero Aggregate's empty JobID: the non-empty
// side wins, and empty is the identity. If both are non-empty (which should
// not happen for a well-formed merge, but keeps mergeJobID total, commutative,
// and associative regardless), the lexicographically smaller JobID wins — a
// fixed, order-independent tiebreak, the same total-order-pick shape as
// routing.summaryWins.
func mergeJobID(a, b model.JobID) model.JobID {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// identityValue combines two raw Values for a template Merge does not
// recognize: the lexicographically smaller non-empty Value wins (a
// min-reduction, so the result is commutative and associative regardless of
// grouping), and nil/empty is its identity — the same shape as the zero
// Aggregate being Merge's identity for every template, known or not. This
// keeps Merge total for an unrecognized template name rather than panicking
// or silently dropping data.
func identityValue(a, b []byte) []byte {
	switch {
	case len(a) == 0:
		return b
	case len(b) == 0:
		return a
	case bytes.Compare(a, b) <= 0:
		return a
	default:
		return b
	}
}
