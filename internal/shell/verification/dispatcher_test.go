package verification

import (
	"context"
	"errors"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestFakeDispatcher_UnconfiguredMachineErrors(t *testing.T) {
	d := NewFakeDispatcher()
	_, err := d.Dispatch(context.Background(), "ghost", model.Task{ID: "t1"})
	if err == nil {
		t.Fatal("Dispatch on an unconfigured machine returned nil error, want an error")
	}
}

func TestFakeDispatcher_ExplicitErr(t *testing.T) {
	d := NewFakeDispatcher()
	want := errors.New("boom")
	d.Set("m1", FakeBehavior{Err: want})

	_, err := d.Dispatch(context.Background(), "m1", model.Task{ID: "t1"})
	if !errors.Is(err, want) {
		t.Fatalf("Dispatch err = %v, want %v", err, want)
	}
}

func TestFakeDispatcher_HangingReturnsErrTimeoutOnCancel(t *testing.T) {
	d := NewFakeDispatcher()
	d.Hanging("m1")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := d.Dispatch(ctx, "m1", model.Task{ID: "t1"})
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("Dispatch on a hanging machine returned early (%v) before ctx was canceled", err)
	default:
	}

	cancel()
	if err := <-done; err != ErrTimeout {
		t.Fatalf("Dispatch err after cancel = %v, want ErrTimeout", err)
	}
}

func TestFakeDispatcher_Calls(t *testing.T) {
	d := NewFakeDispatcher()
	d.Honest("m1", []byte("v"))
	d.Honest("m2", []byte("v"))

	if _, err := d.Dispatch(context.Background(), "m1", model.Task{ID: "t1"}); err != nil {
		t.Fatalf("Dispatch(m1) error: %v", err)
	}
	if _, err := d.Dispatch(context.Background(), "m2", model.Task{ID: "t1"}); err != nil {
		t.Fatalf("Dispatch(m2) error: %v", err)
	}

	got := d.Calls()
	want := []model.MachineID{"m1", "m2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Calls() = %v, want %v", got, want)
	}
}
