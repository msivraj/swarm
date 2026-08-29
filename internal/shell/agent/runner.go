package agent

import (
	"bytes"
	"context"
	"os/exec"
	"time"

	corerunner "github.com/msivraj/swarm/internal/core/runner"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// runRunner drives runner's task-runner core: it seeds the loop with an Idle
// pulse (per the doc, the same pulse both kicks off the loop and resumes it
// after a report), executes every RunCmd the core returns, and feeds the
// resulting events back into Step.
//
// Ambiguities resolved here (see the PR description for the full write-up):
//   - The P0 proto has no dedicated re-queue RPC. ReQueue is executed as a
//     ReportResult call with Ok=false, reusing the field the proto already
//     has for exactly this purpose rather than inventing a new endpoint.
//   - Pull returning no task (or erroring) waits PullInterval and re-arms an
//     Idle event, which is what turns "pull" into a poll loop.
//   - Per runner's own doc comment, Failed's ReQueue command has no paired
//     Pull, so after executing ReQueue the shell sends the same Idle pulse
//     it uses to start the loop.
func (a *Agent) runRunner(ctx context.Context) error {
	state := corerunner.StateIdle
	queue := []corerunner.RunEvent{{Kind: corerunner.Idle}}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(queue) == 0 {
			// Unreachable: Pull's retry Idle, Execute's Done/Failed, and the
			// synthetic Idle after ReQueue always re-arm at least one event.
			// Kept as a safe terminal exit rather than a busy loop.
			return nil
		}

		ev := queue[0]
		queue = queue[1:]

		var cmds []corerunner.RunCmd
		state, cmds = corerunner.Step(state, ev)

		for _, cmd := range cmds {
			next, err := a.execRunCmd(ctx, cmd)
			if err != nil {
				return err
			}
			queue = append(queue, next...)
		}
	}
}

func (a *Agent) execRunCmd(ctx context.Context, cmd corerunner.RunCmd) ([]corerunner.RunEvent, error) {
	switch cmd.Kind {
	case corerunner.Pull:
		return a.execPull(ctx)
	case corerunner.Execute:
		return a.execExecute(ctx, cmd.Task)
	case corerunner.Report:
		return nil, a.execReport(ctx, cmd.Result)
	case corerunner.ReQueue:
		return a.execReQueue(ctx, cmd.Task)
	default:
		return nil, nil
	}
}

func (a *Agent) execPull(ctx context.Context) ([]corerunner.RunEvent, error) {
	client, err := a.clients.get(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := client.PullTask(ctx, &transport.PullTaskRequest{Agent: a.cfg.AgentID})
	if err != nil || !resp.HasTask {
		// A PullTask error is already recoverable as written: it just waits
		// and re-arms Idle to try again, on the same cadence as "no task
		// ready." A transient RPC failure and an empty queue look identical
		// from here, and both resolve the same way — retry later, picking
		// up whatever client is current (the registration loop redials on
		// its own next Heartbeat/JoinCell failure if the connection is
		// actually dead; see rpcRetry's doc comment on Report/ReQueue for
		// why those two can't be this relaxed about it).
		if err := a.sleep(ctx, a.cfg.PullInterval); err != nil {
			return nil, err
		}
		return []corerunner.RunEvent{{Kind: corerunner.Idle}}, nil
	}
	return []corerunner.RunEvent{{Kind: corerunner.Pulled, Task: taskFromProto(resp.Task)}}, nil
}

func (a *Agent) execExecute(ctx context.Context, task model.Task) ([]corerunner.RunEvent, error) {
	result, ok := a.runProcess(ctx, task)
	if !ok {
		return []corerunner.RunEvent{{Kind: corerunner.Failed, Task: task}}, nil
	}
	return []corerunner.RunEvent{{Kind: corerunner.Done, Result: result}}, nil
}

// runProcess spawns the configured native process for task, piping
// task.Input to its stdin and capturing stdout as the result Output. It
// reports ok=false for a missing Argv, a spawn error, or a non-zero exit —
// the shell's Execute -> Done/Failed boundary.
func (a *Agent) runProcess(ctx context.Context, task model.Task) (model.TaskResult, bool) {
	out, ok := a.execProcess(ctx, task.Input)
	if !ok {
		return model.TaskResult{}, false
	}
	return model.TaskResult{TaskID: task.ID, Output: out, OK: true}, true
}

// execProcess spawns cfg.Process.Argv, piping in to its stdin and capturing
// stdout verbatim. It reports ok=false for a missing Argv, a spawn error, or
// a non-zero exit. runProcess wraps this for the P0/P1 task runner (Task ->
// TaskResult); followerServer.AssignWork (issue #96) uses it directly as the
// default Follower.Worker, D5's exec-once-per-step model.
func (a *Agent) execProcess(ctx context.Context, in []byte) ([]byte, bool) {
	argv := a.cfg.Process.Argv
	if len(argv) == 0 {
		return nil, false
	}

	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) //nolint:gosec // Argv is operator-configured, not attacker input
	cmd.Stdin = bytes.NewReader(in)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return nil, false
	}
	return stdout.Bytes(), true
}

func (a *Agent) execReport(ctx context.Context, result model.TaskResult) error {
	return a.rpcRetry(ctx, func(client transport.ControlPlaneClient) error {
		_, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: string(result.TaskID),
			Output: result.Output,
			Ok:     result.OK,
		})
		return err
	})
}

func (a *Agent) execReQueue(ctx context.Context, task model.Task) ([]corerunner.RunEvent, error) {
	err := a.rpcRetry(ctx, func(client transport.ControlPlaneClient) error {
		_, err := client.ReportResult(ctx, &transport.ReportResultRequest{
			TaskId: string(task.ID),
			Ok:     false,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return []corerunner.RunEvent{{Kind: corerunner.Idle}}, nil
}

// rpcRetry calls fn with the current control-plane client until it succeeds
// or ctx is actually done (a real cancellation, checked via ctx.Err() —
// mirroring execRegCommand's "RPC failures are reported as events, never as
// Go errors" contract). A transient RPC failure is not returned to the
// caller: unlike Pull, a Report or ReQueue that returns an error would lose
// the outcome it is carrying, so it must keep retrying rather than fold into
// an Idle pulse and move on. It re-fetches the client on every attempt
// (a.clients.get), so once the registration loop's own failure detection —
// unaffected by this loop — clears and redials a dead connection, the next
// retry automatically picks up the fresh client without this loop touching
// clientHolder itself. That keeps the two loops from racing to clear/redial
// the same client, which is why this stays a "retry with what's current"
// loop rather than one that also signals or clears on its own.
func (a *Agent) rpcRetry(ctx context.Context, fn func(transport.ControlPlaneClient) error) error {
	for {
		client, err := a.clients.get(ctx)
		if err != nil {
			return err
		}
		if err := fn(client); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := a.sleep(ctx, a.cfg.PullInterval); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (a *Agent) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func taskFromProto(t *transport.Task) model.Task {
	if t == nil {
		return model.Task{}
	}
	return model.Task{
		ID:      model.TaskID(t.Id),
		JobID:   model.JobID(t.JobId),
		Input:   t.Input,
		Attempt: int(t.Attempt),
	}
}
