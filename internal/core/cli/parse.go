// Package cli is a pure core: it turns argv into a Command, checks a JobSpec
// before it crosses the wire, and renders a control-plane Reply into text.
// It performs no I/O and reads no clock — the shell owns argv, the job file,
// the gRPC channel, and stdout. This package follows the shape set by
// internal/core/mitosis: take data, return a decision, never execute it.
package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/msivraj/swarm/internal/model"
)

// Kind is the subcommand a Command represents.
type Kind int

const (
	// Unknown is the zero value; a real parse yields Submit, Ps, or Logs.
	Unknown Kind = iota
	Submit
	Ps
	Logs
)

// String names a Kind for logging and error messages.
func (k Kind) String() string {
	switch k {
	case Submit:
		return "submit"
	case Ps:
		return "ps"
	case Logs:
		return "logs"
	default:
		return "unknown"
	}
}

// Command is a parsed CLI invocation the shell will execute against the
// control plane. Cores return Commands; they never dial anything.
type Command struct {
	Kind Kind

	// Submit: exactly one of JobFile or Spec is populated by Parse.
	// JobFile is a path to a JSON job file (see DecodeJobSpec for the
	// documented format); the shell reads it and decodes it. Spec is used
	// when submit was given flags instead of a file.
	JobFile string
	Spec    model.JobSpec

	// Logs: the job being queried. Rendered from the control plane's
	// JobStatus RPC (the design doc's streaming "Logs" RPC has no generated
	// transport method in P0, so `logs`/`status` both resolve to JobStatus —
	// see the PR description).
	JobID model.JobID
}

// Parse turns argv (excluding the program name) into a Command.
//
// Supported forms:
//
//	submit <job-file.json>
//	submit --template T [--coupling independent|barrier|leader|message-passing] [--param k=v]...
//	ps
//	logs <job-id>       (alias: status <job-id>)
func Parse(argv []string) (Command, error) {
	if len(argv) == 0 {
		return Command{}, fmt.Errorf("cli: no subcommand given")
	}
	sub, rest := argv[0], argv[1:]
	switch sub {
	case "submit":
		return parseSubmit(rest)
	case "ps":
		return parsePs(rest)
	case "logs", "status":
		return parseLogs(rest)
	default:
		return Command{}, fmt.Errorf("cli: unknown subcommand %q", sub)
	}
}

func parseSubmit(args []string) (Command, error) {
	var positional []string
	spec := model.JobSpec{Params: map[string]string{}}
	haveFlags := false

	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "--template":
			v, err := flagValue(args, &i, a)
			if err != nil {
				return Command{}, err
			}
			spec.Template = v
			haveFlags = true
		case "--coupling":
			v, err := flagValue(args, &i, a)
			if err != nil {
				return Command{}, err
			}
			c, err := parseCoupling(v)
			if err != nil {
				return Command{}, err
			}
			spec.Coupling = c
			haveFlags = true
		case "--param":
			v, err := flagValue(args, &i, a)
			if err != nil {
				return Command{}, err
			}
			k, val, err := parseParam(v)
			if err != nil {
				return Command{}, err
			}
			spec.Params[k] = val
			haveFlags = true
		default:
			if strings.HasPrefix(a, "-") {
				return Command{}, fmt.Errorf("cli: unknown flag %q", a)
			}
			positional = append(positional, a)
		}
	}

	switch {
	case len(positional) == 1 && !haveFlags:
		return Command{Kind: Submit, JobFile: positional[0]}, nil
	case len(positional) == 0 && haveFlags:
		return Command{Kind: Submit, Spec: spec}, nil
	case len(positional) == 0 && !haveFlags:
		return Command{}, fmt.Errorf("cli: submit requires a job file or --template")
	default:
		return Command{}, fmt.Errorf("cli: submit takes either a job file or flags, not both")
	}
}

func parsePs(args []string) (Command, error) {
	if len(args) != 0 {
		return Command{}, fmt.Errorf("cli: ps takes no arguments")
	}
	return Command{Kind: Ps}, nil
}

func parseLogs(args []string) (Command, error) {
	if len(args) != 1 {
		return Command{}, fmt.Errorf("cli: logs requires exactly one job id")
	}
	return Command{Kind: Logs, JobID: model.JobID(args[0])}, nil
}

// flagValue consumes and returns the value following a flag at args[*i],
// advancing *i past it.
func flagValue(args []string, i *int, flag string) (string, error) {
	*i++
	if *i >= len(args) {
		return "", fmt.Errorf("cli: %s requires a value", flag)
	}
	return args[*i], nil
}

func parseCoupling(s string) (model.Coupling, error) {
	switch s {
	case "independent":
		return model.Independent, nil
	case "barrier":
		return model.Barrier, nil
	case "leader":
		return model.Leader, nil
	case "message-passing":
		return model.MessagePassing, nil
	default:
		return 0, fmt.Errorf("cli: unknown coupling %q", s)
	}
}

func parseParam(s string) (key, value string, err error) {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return "", "", fmt.Errorf("cli: --param must be key=value, got %q", s)
	}
	return k, v, nil
}

// jobFileDoc is the on-disk JSON shape a submit job file decodes into:
//
//	{"template": "...", "coupling": "independent", "params": {"k": "v"}}
//
// "coupling" is optional and defaults to "independent"; "params" is optional.
type jobFileDoc struct {
	Template string            `json:"template"`
	Coupling string            `json:"coupling,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
}

// DecodeJobSpec parses a submit job file's bytes into a JobSpec. The shell
// reads the file (I/O); decoding bytes it already holds is pure.
func DecodeJobSpec(data []byte) (model.JobSpec, error) {
	var doc jobFileDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return model.JobSpec{}, fmt.Errorf("cli: decode job file: %w", err)
	}
	spec := model.JobSpec{Template: doc.Template, Params: doc.Params}
	if doc.Coupling == "" {
		spec.Coupling = model.Independent
		return spec, nil
	}
	c, err := parseCoupling(doc.Coupling)
	if err != nil {
		return model.JobSpec{}, err
	}
	spec.Coupling = c
	return spec, nil
}
