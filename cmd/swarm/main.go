// Command swarm is the Swarm CLI — a thin gRPC client over the control-plane
// API. All I/O (argv, the job file, the network, stdout) lives here; parsing,
// validation, and rendering are delegated to the pure internal/core/cli core.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/msivraj/swarm/internal/core/cli"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/transport"
)

// defaultAddr is used when SWARM_CONTROL_PLANE_ADDR is unset.
const defaultAddr = "localhost:7777"

func main() {
	addr := os.Getenv("SWARM_CONTROL_PLANE_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "swarm: dial:", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := transport.NewControlPlaneClient(conn)
	if err := run(context.Background(), os.Args[1:], client, os.Stdout, os.ReadFile); err != nil {
		fmt.Fprintln(os.Stderr, "swarm:", err)
		os.Exit(1)
	}
}

// run executes one CLI invocation: parse argv with the pure cli core,
// perform the I/O the command needs (reading a job file, calling the control
// plane), and render the reply to w. Splitting the RPC and file-read as
// parameters is what makes this loop testable without a real network or
// filesystem.
func run(ctx context.Context, argv []string, client transport.ControlPlaneClient, w io.Writer, readFile func(string) ([]byte, error)) error {
	cmd, err := cli.Parse(argv)
	if err != nil {
		return err
	}

	var reply cli.Reply
	switch cmd.Kind {
	case cli.Submit:
		reply, err = runSubmit(ctx, cmd, client, readFile)
	case cli.Ps:
		reply, err = runPs(ctx, client)
	case cli.Logs:
		reply, err = runLogs(ctx, cmd, client)
	default:
		err = errors.New("swarm: unhandled command")
	}
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(w, cli.Render(reply))
	return err
}

func runSubmit(ctx context.Context, cmd cli.Command, client transport.ControlPlaneClient, readFile func(string) ([]byte, error)) (cli.Reply, error) {
	spec := cmd.Spec
	if cmd.JobFile != "" {
		data, err := readFile(cmd.JobFile)
		if err != nil {
			return cli.Reply{}, fmt.Errorf("read job file: %w", err)
		}
		decoded, err := cli.DecodeJobSpec(data)
		if err != nil {
			return cli.Reply{}, err
		}
		spec = decoded
	}

	if err := cli.Validate(spec); err != nil {
		return cli.Reply{}, err
	}

	resp, err := client.SubmitJob(ctx, &transport.SubmitJobRequest{
		Template: spec.Template,
		Coupling: toTransportCoupling(spec.Coupling),
		Params:   spec.Params,
	})
	if err != nil {
		return cli.Reply{}, err
	}
	return cli.Reply{Kind: cli.ReplySubmit, JobID: model.JobID(resp.GetJobId())}, nil
}

func runPs(ctx context.Context, client transport.ControlPlaneClient) (cli.Reply, error) {
	resp, err := client.Ps(ctx, &transport.PsRequest{})
	if err != nil {
		return cli.Reply{}, err
	}
	return cli.Reply{
		Kind:     cli.ReplyPs,
		Cells:    int(resp.GetCells()),
		Machines: int(resp.GetMachines()),
		Jobs:     int(resp.GetJobs()),
	}, nil
}

func runLogs(ctx context.Context, cmd cli.Command, client transport.ControlPlaneClient) (cli.Reply, error) {
	resp, err := client.JobStatus(ctx, &transport.JobStatusRequest{JobId: string(cmd.JobID)})
	if err != nil {
		return cli.Reply{}, err
	}
	return cli.Reply{
		Kind:      cli.ReplyJobStatus,
		JobID:     cmd.JobID,
		Done:      resp.GetDone(),
		Aggregate: resp.GetAggregate(),
	}, nil
}

func toTransportCoupling(c model.Coupling) transport.Coupling {
	switch c {
	case model.Barrier:
		return transport.Coupling_COUPLING_BARRIER
	case model.Leader:
		return transport.Coupling_COUPLING_LEADER
	case model.MessagePassing:
		return transport.Coupling_COUPLING_MESSAGE_PASSING
	default:
		return transport.Coupling_COUPLING_INDEPENDENT
	}
}
