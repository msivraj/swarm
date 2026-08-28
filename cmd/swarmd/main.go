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
	"github.com/msivraj/swarm/internal/shell/global"
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
		case "global":
			runGlobal(os.Args[2:])
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
	homeRegion := fs.String("home-region", "", "this agent's home RegionID; enables cross-region failover together with --peer-regions, --region-targets and --global-router")
	peerRegions := fs.String("peer-regions", "", "comma-separated peer RegionIDs in nearest-first order (cross-region failover only)")
	regionTargets := fs.String("region-targets", "", "comma-separated region=host:port pairs mapping a RegionID to its control-plane dial address (cross-region failover only)")
	globalRouter := fs.String("global-router", "", "GlobalRouter dial address polled for cross-region health; empty disables cross-region failover")
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

	if err := configureFailover(&cfg, *homeRegion, *peerRegions, *regionTargets, *globalRouter); err != nil {
		return err
	}

	a := agent.New(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return a.Run(ctx)
}

// configureFailover fills in cfg's cross-region failover fields from the
// agent-mode flags. It is a no-op (single-region P0 behavior) when
// homeRegion is empty; --peer-regions, --region-targets and --global-router
// are meaningless without it and are ignored in that case, matching
// Config.GlobalRouter's "empty disables multi-region failover" contract.
func configureFailover(cfg *agent.Config, homeRegion, peerRegions, regionTargets, globalRouter string) error {
	if homeRegion == "" {
		return nil
	}

	cfg.HomeRegion = model.RegionID(homeRegion)
	cfg.KnownRegions = []model.RegionID{cfg.HomeRegion}
	if peerRegions != "" {
		for _, p := range strings.Split(peerRegions, ",") {
			cfg.KnownRegions = append(cfg.KnownRegions, model.RegionID(p))
		}
	}

	cfg.RegionTargets = map[model.RegionID]string{}
	if regionTargets != "" {
		for _, pair := range strings.Split(regionTargets, ",") {
			k, v, ok := strings.Cut(pair, "=")
			if !ok {
				return fmt.Errorf("--region-targets: invalid pair %q, want region=host:port", pair)
			}
			cfg.RegionTargets[model.RegionID(k)] = v
		}
	}

	cfg.GlobalRouter = globalRouter
	cfg.GlobalViewDialer = agent.GRPCGlobalViewDialer()
	return nil
}

// runControlPlane starts the control-plane gRPC server: an in-memory store,
// a real wall clock fed into controlplane.Server as the now data dependency
// (the clock is read here, in the shell — never inside a core), and
// controlplane.DefaultConfig's tunables, optionally extended into S2's
// regional mode by the --region-id/--global-router/--summary-interval/
// --peer-targets/--advertise-addr flags. Leaving --global-router empty (the
// default) keeps this exactly P0/S1's single-region behavior —
// controlplane.Config.GlobalRouter empty disables publish, spill, and
// upward roll-up entirely.
func runControlPlane(args []string) {
	fs := flag.NewFlagSet("control-plane", flag.ExitOnError)
	listen := fs.String("listen", ":7070", "address for the control-plane gRPC server to listen on")
	regionID := fs.String("region-id", "", "this region's RegionID; stamps published summaries and reported partials (regional mode only)")
	globalRouter := fs.String("global-router", "", "GlobalRouter dial address for publishing summaries, spilling, and rolling up partials; empty keeps this control plane in standalone P0 mode")
	summaryInterval := fs.Duration("summary-interval", 0, "how often to publish a RegionalSummary to --global-router; 0 uses controlplane.DefaultConfig's default")
	peerTargets := fs.String("peer-targets", "", "comma-separated region=host:port pairs mapping a peer RegionID to its control-plane dial address, for spill")
	advertiseAddr := fs.String("advertise-addr", "", "dial address other regions use to reach this control plane (result_sink for a spilled task's result); defaults to --listen")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("control-plane: %v", err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("control-plane: listen on %s: %v", *listen, err)
	}

	cfg := controlplane.DefaultConfig()
	cfg.RegionID = model.RegionID(*regionID)
	cfg.GlobalRouter = *globalRouter
	if *summaryInterval > 0 {
		cfg.SummaryInterval = *summaryInterval
	}
	cfg.SelfAddress = *advertiseAddr
	if cfg.SelfAddress == "" {
		cfg.SelfAddress = *listen
	}
	cfg.PeerTargets, err = parsePeerTargets(*peerTargets)
	if err != nil {
		log.Fatalf("control-plane: %v", err)
	}

	srv := controlplane.New(store.NewMemStore(), cfg, now)
	log.Printf("control-plane: listening on %s", *listen)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("control-plane: serve: %v", err)
	}
}

// runGlobal starts the P1 global routing layer's gRPC server (S3, issue
// #45): an in-memory store, a real wall clock fed into global.Server as the
// now data dependency (never read inside a core), and global.DefaultConfig's
// tunables, extended by the --region-targets/--advertise-addr/
// --diverge-sweep flags. --region-targets is required — with no known
// regions to dial, every Submit would immediately reject ResourceExhausted.
func runGlobal(args []string) {
	fs := flag.NewFlagSet("global", flag.ExitOnError)
	listen := fs.String("listen", ":7080", "address for the global routing layer's gRPC server to listen on")
	regionTargets := fs.String("region-targets", "", "comma-separated region=host:port pairs mapping a RegionID to its control-plane dial address")
	advertiseAddr := fs.String("advertise-addr", "", "dial address regions use to reach this global layer (result_sink for a Spread partition, matched against each region's own --global-router); defaults to --listen")
	divergeSweep := fs.Duration("diverge-sweep", 0, "how often to recompute diverged (stale) regions; 0 uses global.DefaultConfig's default")
	if err := fs.Parse(args); err != nil {
		log.Fatalf("global: %v", err)
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("global: listen on %s: %v", *listen, err)
	}

	cfg := global.DefaultConfig()
	cfg.RegionTargets, err = parseRegionTargets(*regionTargets)
	if err != nil {
		log.Fatalf("global: %v", err)
	}
	cfg.SelfAddress = *advertiseAddr
	if cfg.SelfAddress == "" {
		cfg.SelfAddress = *listen
	}
	if *divergeSweep > 0 {
		cfg.DivergeSweep = *divergeSweep
	}

	srv := global.New(store.NewMemStore(), cfg, now)
	log.Printf("global: listening on %s", *listen)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("global: serve: %v", err)
	}
}

// parseRegionTargets parses --region-targets' "region=host:port,..." form
// into global.Config.RegionTargets, mirroring parsePeerTargets/
// configureFailover's identical parsing for controlplane/agent — kept
// separate rather than shared since each builds a different Config's map and
// the three flag sets evolve independently.
func parseRegionTargets(s string) (map[model.RegionID]string, error) {
	targets := map[model.RegionID]string{}
	if s == "" {
		return targets, nil
	}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("--region-targets: invalid pair %q, want region=host:port", pair)
		}
		targets[model.RegionID(k)] = v
	}
	return targets, nil
}

// parsePeerTargets parses --peer-targets' "region=host:port,region=host:port"
// form into controlplane.Config.PeerTargets, mirroring configureFailover's
// --region-targets parsing above (kept separate rather than shared: that one
// builds an agent.Config map, this one a controlplane.Config map, and the two
// flag sets evolve independently).
func parsePeerTargets(s string) (map[model.RegionID]string, error) {
	targets := map[model.RegionID]string{}
	if s == "" {
		return targets, nil
	}
	for _, pair := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("--peer-targets: invalid pair %q, want region=host:port", pair)
		}
		targets[model.RegionID(k)] = v
	}
	return targets, nil
}

// now supplies the wall clock as model.Instant data to the control plane — the
// one place in this binary allowed to call time.Now.
func now() model.Instant {
	return model.Instant(time.Now().UnixNano())
}
