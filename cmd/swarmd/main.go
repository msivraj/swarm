// Command swarmd runs a Swarm node: the agent and, where configured, the
// regional control plane.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/agent"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	"github.com/msivraj/swarm/internal/shell/store"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "agent":
			if err := runAgent(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "swarmd agent:", err)
				os.Exit(1)
			}
			return
		case "control-plane":
			runControlPlane(os.Args[2:])
			return
		}
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

// runControlPlane starts the P0 control-plane gRPC server: an in-memory store,
// a real wall clock fed into controlplane.Server as the now data dependency
// (the clock is read here, in the shell — never inside a core), and
// controlplane.DefaultConfig's tunables.
func runControlPlane(args []string) {
	fs := flag.NewFlagSet("control-plane", flag.ExitOnError)
	listen := fs.String("listen", ":7070", "address for the control-plane gRPC server to listen on")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("control-plane: %v", err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("control-plane: listen on %s: %v", *listen, err)
	}

	srv := controlplane.New(store.NewMemStore(), controlplane.DefaultConfig(), now)
	log.Printf("control-plane: listening on %s", *listen)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("control-plane: serve: %v", err)
	}
}

// now supplies the wall clock as model.Instant data to the control plane — the
// one place in this binary allowed to call time.Now.
func now() model.Instant {
	return model.Instant(time.Now().UnixNano())
}
