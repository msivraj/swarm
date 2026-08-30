package templates

// Template names admission.Admit recognizes and dispatches on, and aggregate.Merge
// looks up to pick a template's Value-combine function. They live here — the
// package that owns the decompose/combine functions they name
// (KeyspaceDecompose/KeyspaceCombine, MonteCarloDecompose/MonteCarloCombine)
// — so that both admission and aggregate can depend on templates for them
// without aggregate needing to import admission (issue #115).
const (
	TemplateKeyspaceSearch = "keyspace-search"
	TemplateMonteCarlo     = "monte-carlo"
)
