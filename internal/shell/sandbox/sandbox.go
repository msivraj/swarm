// Package sandbox is the P3 WASM sandbox runner shell: it instantiates a
// verified WASM module in a wazero runtime configured with exactly the
// least-privilege WASI capabilities internal/core/sandbox.Grants computed
// for the task, runs it to completion, and reports the captured output as a
// model.TaskResult.
//
// Wire location: this is a new internal/shell/sandbox package rather than an
// internal/shell/agent delta. The open-tier dispatch/collect coordinator
// (#140 — the shell that owns "assign K -> run sandboxed -> verdict ->
// re-run") is the intended caller: it dispatches a task to K machines, and
// on an open-tier machine calls Runner.Run to execute it. Keeping the
// sandbox host self-contained here means P0's native run state machine in
// internal/shell/agent/runner.go is untouched by this ticket — only HOW an
// open-tier task executes (this package) is new, exactly as the design doc
// requires.
//
// wazero appears only under internal/shell — this package never imports
// into internal/core, and internal/core/sandbox never imports wazero.
package sandbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"

	coresandbox "github.com/msivraj/swarm/internal/core/sandbox"
	"github.com/msivraj/swarm/internal/model"
)

// ErrUnsigned is returned by Run when the module's signature fails
// core/sandbox.VerifyModule. The module is never instantiated in this case
// — Run returns before any wazero runtime is created.
var ErrUnsigned = errors.New("sandbox: module signature verification failed, refusing to run")

// Module is a WASM workload awaiting execution: its raw bytes and the
// detached signature over them that VerifyModule checks before Run will
// instantiate it.
type Module struct {
	Bytes []byte
	Sig   model.Sig
}

// Runner executes verified WASM modules inside a least-privilege wazero
// sandbox. The zero value is ready to use; Runner holds no state between
// calls, so a single Runner can serve concurrent Run calls.
type Runner struct{}

// Run verifies mod against key using core/sandbox.VerifyModule and, only if
// that succeeds, instantiates it in a fresh wazero runtime configured with
// exactly core/sandbox.Grants(task)'s WASI capabilities — mounted
// directories for the declared ReadPaths/WritePaths (nothing else is
// reachable), the declared Env allow-list (sourced from the shell's own
// environment, filtered to that allow-list), and stdio/clock wired only if
// declared. wazero's own ModuleConfig/FSConfig defaults are already the
// safe/empty case (no fs access, stdin reads EOF, stdout/stderr discarded,
// a deterministic fake clock) — Run only ever adds capabilities on top of
// those defaults, never subtracts, so a task that declares nothing gets no
// ambient authority.
//
// It runs the module's _start function to completion and returns the
// captured stdout as the result's Output. A module that calls proc_exit
// with a non-zero code (wazero surfaces this as *sys.ExitError) is reported
// as TaskResult.OK == false, exactly as a non-zero exit is reported for a
// native task by internal/shell/agent's process runner — a task-level
// failure, not a shell error. Any other failure (bad wasm, wasi setup, a
// trap, or ctx being canceled/timing out mid-run — see
// WithCloseOnContextDone below) is returned as a Go error instead.
func (Runner) Run(ctx context.Context, task model.Task, mod Module, key model.PubKey) (model.TaskResult, error) {
	if !coresandbox.VerifyModule(mod.Bytes, mod.Sig, key) {
		return model.TaskResult{}, ErrUnsigned
	}

	caps := coresandbox.Grants(task)

	// WithCloseOnContextDone makes wazero periodically check ctx during
	// guest execution and abort (rather than run forever) once it's
	// canceled or its deadline passes — the untrusted module can't block
	// the caller past what ctx allows.
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
	defer rt.Close(ctx)

	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		return model.TaskResult{}, fmt.Errorf("sandbox: instantiate wasi: %w", err)
	}

	var stdout bytes.Buffer
	cfg := moduleConfig(caps, task.Input, &stdout)

	guest, err := rt.InstantiateWithConfig(ctx, mod.Bytes, cfg)
	if guest != nil {
		defer guest.Close(ctx)
	}
	if err != nil {
		var exitErr *sys.ExitError
		if errors.As(err, &exitErr) {
			switch exitErr.ExitCode() {
			case sys.ExitCodeContextCanceled, sys.ExitCodeDeadlineExceeded:
				// The shell aborted the run via ctx, not the guest calling
				// proc_exit — an infra-level error, not a task outcome.
				return model.TaskResult{}, fmt.Errorf("sandbox: %w", err)
			default:
				return model.TaskResult{
					TaskID: task.ID,
					Output: stdout.Bytes(),
					OK:     exitErr.ExitCode() == 0,
				}, nil
			}
		}
		return model.TaskResult{}, fmt.Errorf("sandbox: run module: %w", err)
	}

	return model.TaskResult{TaskID: task.ID, Output: stdout.Bytes(), OK: true}, nil
}

// moduleConfig maps caps onto a wazero ModuleConfig, adding exactly the
// capabilities caps declares on top of wazero's already-safe defaults.
//
//   - The task's Input is always visible to the guest as its first argument
//     (skipped when empty — wazero errs on an empty arg string) so a probe
//     module can act on it without needing the Stdio capability; args are
//     not a WasiCaps-gated channel, they're how the shell tells the module
//     which task it's running, the same role Input plays for a native task.
//   - Stdio, when declared, wires Input onto stdin and wires stdout/stderr
//     to the capture buffer; undeclared, stdin reads EOF and stdout/stderr
//     are discarded (wazero's default), so a task that didn't ask for
//     stdio gets an empty result rather than a failure.
//   - Clock, when declared, switches from wazero's default deterministic
//     fake clock to the real wall/monotonic clock.
//   - ReadPaths/WritePaths mount host directories at the same guest path;
//     nothing outside a declared root is reachable (see fsConfigFrom).
//   - Env exposes only the declared names, sourced from the shell's own
//     process environment — a name with no host value is simply absent.
func moduleConfig(caps model.WasiCaps, input []byte, capture *bytes.Buffer) wazero.ModuleConfig {
	args := []string{"task"}
	if len(input) > 0 {
		args = append(args, string(input))
	}

	cfg := wazero.NewModuleConfig().
		WithArgs(args...).
		WithFSConfig(fsConfigFrom(caps))

	if caps.Stdio {
		cfg = cfg.WithStdin(bytes.NewReader(input)).WithStdout(capture).WithStderr(capture)
	}
	if caps.Clock {
		cfg = cfg.WithSysWalltime().WithSysNanotime()
	}
	for _, name := range caps.Env {
		if v, ok := os.LookupEnv(name); ok {
			cfg = cfg.WithEnv(name, v)
		}
	}
	return cfg
}

// fsConfigFrom mounts exactly the directories caps declares: WritePaths
// read-write, and any ReadPaths not already covered by a WritePaths mount
// read-only. A task that declares neither gets wazero's default FSConfig,
// under which every path_open fails — there is no ambient filesystem.
func fsConfigFrom(caps model.WasiCaps) wazero.FSConfig {
	fsc := wazero.NewFSConfig()

	writable := make(map[string]bool, len(caps.WritePaths))
	for _, p := range caps.WritePaths {
		fsc = fsc.WithDirMount(p, p)
		writable[p] = true
	}
	for _, p := range caps.ReadPaths {
		if writable[p] {
			continue
		}
		fsc = fsc.WithReadOnlyDirMount(p, p)
	}
	return fsc
}
