// Command swarmd runs a Swarm node: the agent and, where configured, the
// regional control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/msivraj/swarm/internal/shell/agent"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "agent" {
		if err := runAgent(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "swarmd agent:", err)
			os.Exit(1)
		}
		return
	}
	fmt.Println("swarmd — Swarm node daemon (bootstrap)")
}

// runAgent parses agent-mode flags, constructs an agent.Agent wired to a
// real clock and a math/rand jitter source (randomness lives in the shell,
// never in core — see internal/shell/agent), and runs it until interrupted.
func runAgent(args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	controlPlane := fs.String("control-plane", "", "control-plane dial target(s), comma-separated for failover (host:port,host:port,...)")
	agentID := fs.String("agent-id", "", "this agent's identity")
	region := fs.String("region", "", "region this agent asks to join")
	caps := fs.Int("caps", 1, "capacity units this agent offers")
	exec := fs.String("exec", "", "native process to run per task, e.g. \"/usr/bin/my-worker --flag\"; the task's input is piped to its stdin and its stdout is captured as the result")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *controlPlane == "" {
		return fmt.Errorf("--control-plane is required")
	}
	if *agentID == "" {
		return fmt.Errorf("--agent-id is required")
	}

	cfg := agent.Config{
		AgentID: *agentID,
		Region:  *region,
		Caps:    int32(*caps),
		Targets: strings.Split(*controlPlane, ","),
		Dialer:  agent.GRPCDialer(),
	}
	if *exec != "" {
		cfg.Process.Argv = strings.Fields(*exec)
	}

	a := agent.New(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return a.Run(ctx)
}
