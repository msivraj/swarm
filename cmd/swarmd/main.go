// Command swarmd runs a Swarm node: the agent and, where configured, the
// regional control plane.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/controlplane"
	"github.com/msivraj/swarm/internal/shell/store"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "control-plane" {
		runControlPlane(os.Args[2:])
		return
	}
	fmt.Println("swarmd — Swarm node daemon (bootstrap)")
}

// runControlPlane starts the P0 control-plane gRPC server: an in-memory
// store, a real wall clock fed into controlplane.Server as the `now` data
// dependency (the clock is read here, in the shell — never inside a core),
// and controlplane.DefaultConfig's tunables.
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

// now supplies the wall clock as model.Instant data to the control plane —
// the one place in this binary allowed to call time.Now.
func now() model.Instant {
	return model.Instant(time.Now().UnixNano())
}
