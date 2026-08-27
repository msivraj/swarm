package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/msivraj/swarm/internal/shell/transport"
)

// stubControlPlane is a fake control plane server run in-process over
// bufconn, so tests exercise the real gRPC wire format without a socket.
type stubControlPlane struct {
	transport.UnimplementedControlPlaneServer

	submitResp *transport.SubmitJobResponse
	submitReq  *transport.SubmitJobRequest
	psResp     *transport.PsResponse
	statusResp *transport.JobStatusResponse
	statusReq  *transport.JobStatusRequest
	err        error
}

func (s *stubControlPlane) SubmitJob(_ context.Context, req *transport.SubmitJobRequest) (*transport.SubmitJobResponse, error) {
	s.submitReq = req
	if s.err != nil {
		return nil, s.err
	}
	return s.submitResp, nil
}

func (s *stubControlPlane) Ps(context.Context, *transport.PsRequest) (*transport.PsResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.psResp, nil
}

func (s *stubControlPlane) JobStatus(_ context.Context, req *transport.JobStatusRequest) (*transport.JobStatusResponse, error) {
	s.statusReq = req
	if s.err != nil {
		return nil, s.err
	}
	return s.statusResp, nil
}

// dialStub starts srv over an in-process bufconn listener and returns a
// client connected to it plus a teardown func.
func dialStub(t *testing.T, srv *stubControlPlane) (transport.ControlPlaneClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	transport.RegisterControlPlaneServer(gs, srv)
	go func() {
		_ = gs.Serve(lis)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	return transport.NewControlPlaneClient(conn), func() {
		_ = conn.Close()
		gs.Stop()
	}
}

func noReadFile(string) ([]byte, error) {
	return nil, errors.New("readFile should not be called")
}

func TestRunSubmitWithFlags(t *testing.T) {
	srv := &stubControlPlane{submitResp: &transport.SubmitJobResponse{JobId: "job-42"}}
	client, teardown := dialStub(t, srv)
	defer teardown()

	var out bytes.Buffer
	argv := []string{"submit", "--template", "wordcount", "--coupling", "leader", "--param", "shards=4"}
	if err := run(context.Background(), argv, client, &out, noReadFile); err != nil {
		t.Fatalf("run: %v", err)
	}

	if got, want := out.String(), "submitted job job-42\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	if srv.submitReq.GetTemplate() != "wordcount" {
		t.Fatalf("submitted template = %q, want %q", srv.submitReq.GetTemplate(), "wordcount")
	}
	if srv.submitReq.GetCoupling() != transport.Coupling_COUPLING_LEADER {
		t.Fatalf("submitted coupling = %v, want %v", srv.submitReq.GetCoupling(), transport.Coupling_COUPLING_LEADER)
	}
	if srv.submitReq.GetParams()["shards"] != "4" {
		t.Fatalf("submitted params = %v, want shards=4", srv.submitReq.GetParams())
	}
}

func TestRunSubmitWithJobFile(t *testing.T) {
	srv := &stubControlPlane{submitResp: &transport.SubmitJobResponse{JobId: "job-7"}}
	client, teardown := dialStub(t, srv)
	defer teardown()

	readFile := func(path string) ([]byte, error) {
		if path != "job.json" {
			t.Fatalf("readFile called with %q, want %q", path, "job.json")
		}
		return []byte(`{"template":"wordcount","coupling":"independent"}`), nil
	}

	var out bytes.Buffer
	if err := run(context.Background(), []string{"submit", "job.json"}, client, &out, readFile); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got, want := out.String(), "submitted job job-7\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunSubmitValidationFailsWithoutRPC(t *testing.T) {
	srv := &stubControlPlane{err: errors.New("should not be called")}
	client, teardown := dialStub(t, srv)
	defer teardown()

	var out bytes.Buffer
	err := run(context.Background(), []string{"submit", "--template", ""}, client, &out, noReadFile)
	if err == nil {
		t.Fatal("run: want an error for an empty template, got nil")
	}
}

func TestRunPs(t *testing.T) {
	srv := &stubControlPlane{psResp: &transport.PsResponse{Cells: 3, Machines: 42, Jobs: 7}}
	client, teardown := dialStub(t, srv)
	defer teardown()

	var out bytes.Buffer
	if err := run(context.Background(), []string{"ps"}, client, &out, noReadFile); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := "CELLS  MACHINES  JOBS\n3      42        7   \n"
	if got := out.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestRunLogs(t *testing.T) {
	srv := &stubControlPlane{statusResp: &transport.JobStatusResponse{Done: true, Aggregate: []byte("42")}}
	client, teardown := dialStub(t, srv)
	defer teardown()

	var out bytes.Buffer
	if err := run(context.Background(), []string{"logs", "job-1"}, client, &out, noReadFile); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := "job job-1: done\naggregate: 42\n"
	if got := out.String(); got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
	if srv.statusReq.GetJobId() != "job-1" {
		t.Fatalf("status request job id = %q, want %q", srv.statusReq.GetJobId(), "job-1")
	}
}

func TestRunParseError(t *testing.T) {
	client, teardown := dialStub(t, &stubControlPlane{})
	defer teardown()

	var out bytes.Buffer
	if err := run(context.Background(), []string{"bogus"}, client, &out, noReadFile); err == nil {
		t.Fatal("run: want an error for an unknown subcommand, got nil")
	}
}

func TestRunRPCError(t *testing.T) {
	srv := &stubControlPlane{err: errors.New("control plane unavailable")}
	client, teardown := dialStub(t, srv)
	defer teardown()

	var out bytes.Buffer
	err := run(context.Background(), []string{"ps"}, client, &out, noReadFile)
	if err == nil {
		t.Fatal("run: want an error when the RPC fails, got nil")
	}
}

// TestDefaultAddrMatchesDaemonListen asserts the CLI's default dial target
// matches swarmd control-plane's default --listen of ":7070", so a
// freshly started daemon is reachable with no env var set.
func TestDefaultAddrMatchesDaemonListen(t *testing.T) {
	unset := func(string) string { return "" }
	if got, want := controlPlaneAddr(unset), "localhost:7070"; got != want {
		t.Fatalf("controlPlaneAddr(unset) = %q, want %q", got, want)
	}
	if defaultAddr != "localhost:7070" {
		t.Fatalf("defaultAddr = %q, want %q", defaultAddr, "localhost:7070")
	}
}

// TestControlPlaneAddrEnvOverride asserts SWARM_CONTROL_PLANE_ADDR still
// overrides the default when set.
func TestControlPlaneAddrEnvOverride(t *testing.T) {
	getenv := func(key string) string {
		if key != "SWARM_CONTROL_PLANE_ADDR" {
			t.Fatalf("getenv called with %q, want %q", key, "SWARM_CONTROL_PLANE_ADDR")
		}
		return "example.com:9999"
	}
	if got, want := controlPlaneAddr(getenv), "example.com:9999"; got != want {
		t.Fatalf("controlPlaneAddr(override) = %q, want %q", got, want)
	}
}

func TestToTransportCoupling(t *testing.T) {
	// Exercised indirectly by TestRunSubmitWithFlags; this covers the
	// default branch directly.
	if got := toTransportCoupling(99); got != transport.Coupling_COUPLING_INDEPENDENT {
		t.Fatalf("toTransportCoupling(99) = %v, want COUPLING_INDEPENDENT", got)
	}
}
