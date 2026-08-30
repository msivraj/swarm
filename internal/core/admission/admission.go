// Package admission is a pure core: it decides whether a submitted JobSpec is
// accepted and, if so, decomposes it into Tasks. It performs no I/O and reads
// no clock or randomness — every decision is a pure function of the JobSpec
// and the template decompose functions in internal/core/templates. This
// package follows the shape set by internal/core/mitosis: take data, return a
// decision, never execute an effect.
package admission

import (
	"strconv"

	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
)

// Reject carries why a job was refused; the zero value (Rejected == false)
// means the job was admitted.
type Reject struct {
	Rejected bool
	Reason   string
}

// templateDecomposers maps a known JobSpec.Template (templates.TemplateKeyspaceSearch,
// templates.TemplateMonteCarlo — the names are defined in internal/core/templates,
// the package that owns the decompose functions they name) to the function
// that validates its Params and decomposes it into Tasks. Admit's template
// lookup is this map's membership test, so adding a template means adding
// one entry here — no other branch to keep in sync.
var templateDecomposers = map[string]func(model.JobSpec) ([]model.Task, Reject){
	templates.TemplateKeyspaceSearch: decomposeKeyspace,
	templates.TemplateMonteCarlo:     decomposeMonteCarlo,
}

// Admit validates spec and, if valid, decomposes it into Tasks via the named
// template. It returns (tasks, zero Reject) on success, or (nil, Reject) on
// failure.
//
// The phase doc pins admit's signature ("validate, then template.decompose()")
// but not its validation rules. This is the minimal, documented set from
// issue #4:
//   - Template must name one of the templates this package knows how to
//     decompose (templates.TemplateKeyspaceSearch, templates.TemplateMonteCarlo).
//   - Coupling must be model.Independent — P0 only runs independent tasks;
//     later phases (Barrier, Leader, MessagePassing) need a driver this
//     package does not yet have.
//   - The template's required Params must be present in spec.Params and
//     parse as the expected type (see decomposeKeyspace, decomposeMonteCarlo).
func Admit(spec model.JobSpec) ([]model.Task, Reject) {
	decompose, ok := templateDecomposers[spec.Template]
	if !ok {
		return reject("unknown template: " + spec.Template)
	}
	if spec.Coupling != model.Independent {
		return reject("coupling must be Independent in P0")
	}
	return decompose(spec)
}

func reject(reason string) ([]model.Task, Reject) {
	return nil, Reject{Rejected: true, Reason: reason}
}

// decomposeKeyspace validates a keyspace-search JobSpec's Params and
// decomposes it via templates.KeyspaceDecompose.
//
// JobSpec.Params has no typed mapping to templates.KeyspaceJob in the phase
// doc's §03 model (issue #4 "Notes / ambiguities"); this is the minimal,
// documented mapping: the three string keys "start", "end", and "shards"
// parse to KeyspaceJob's Start (uint64), End (uint64), and Shards (int)
// fields respectively — the same fields a caller parsing Params from a job
// submission would need to fill in.
func decomposeKeyspace(spec model.JobSpec) ([]model.Task, Reject) {
	start, ok := parseUint(spec.Params, "start")
	if !ok {
		return reject("keyspace-search: missing or invalid \"start\" param")
	}
	end, ok := parseUint(spec.Params, "end")
	if !ok {
		return reject("keyspace-search: missing or invalid \"end\" param")
	}
	shards, ok := parseInt(spec.Params, "shards")
	if !ok {
		return reject("keyspace-search: missing or invalid \"shards\" param")
	}

	tasks := templates.KeyspaceDecompose(templates.KeyspaceJob{
		JobID:  spec.ID,
		Start:  start,
		End:    end,
		Shards: shards,
	})
	if len(tasks) == 0 {
		return reject("keyspace-search: params produced no tasks (end <= start)")
	}
	return tasks, Reject{}
}

// decomposeMonteCarlo validates a monte-carlo JobSpec's Params and decomposes
// it via templates.MonteCarloDecompose.
//
// As with decomposeKeyspace, the Params mapping is not pinned by the phase
// doc; this is the minimal, documented mapping: the string keys "trials",
// "blockSize", and "seed" parse to MCJob's Trials (int64), BlockSize (int64),
// and BaseSeed (int64) fields respectively.
func decomposeMonteCarlo(spec model.JobSpec) ([]model.Task, Reject) {
	trials, ok := parseInt64(spec.Params, "trials")
	if !ok {
		return reject("monte-carlo: missing or invalid \"trials\" param")
	}
	blockSize, ok := parseInt64(spec.Params, "blockSize")
	if !ok {
		return reject("monte-carlo: missing or invalid \"blockSize\" param")
	}
	baseSeed, ok := parseInt64(spec.Params, "seed")
	if !ok {
		return reject("monte-carlo: missing or invalid \"seed\" param")
	}

	tasks := templates.MonteCarloDecompose(templates.MCJob{
		JobID:     spec.ID,
		Trials:    trials,
		BlockSize: blockSize,
		BaseSeed:  baseSeed,
	})
	if len(tasks) == 0 {
		return reject("monte-carlo: params produced no tasks (trials or blockSize <= 0)")
	}
	return tasks, Reject{}
}

func parseUint(params map[string]string, key string) (uint64, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return n, err == nil
}

func parseInt(params map[string]string, key string) (int, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(v)
	return n, err == nil
}

func parseInt64(params map[string]string, key string) (int64, bool) {
	v, ok := params[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	return n, err == nil
}
