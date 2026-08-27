package agentreg

import (
	"reflect"
	"testing"
	"time"

	"github.com/msivraj/swarm/internal/model"
)

// TestStepTransitions covers every (phase, event) pair the doc names, plus
// the pairs it leaves as no-ops.
func TestStepTransitions(t *testing.T) {
	tests := []struct {
		name    string
		state   RegState
		event   RegEvent
		jitter  float64
		want    RegState
		wantCmd []Command
	}{
		// --- Dialing ---
		{
			name:    "Dialing+DialOK unenrolled enrolls",
			state:   RegState{Phase: Dialing},
			event:   DialOK,
			want:    RegState{Phase: Enrolling},
			wantCmd: []Command{{Kind: Enroll}},
		},
		{
			name:    "Dialing+DialOK already enrolled skips straight to joining",
			state:   RegState{Phase: Dialing, Enrolled: true, Attempt: 3},
			event:   DialOK,
			want:    RegState{Phase: Joining, Enrolled: true},
			wantCmd: []Command{{Kind: JoinCell}},
		},
		{
			name:    "Dialing+DialFail waits with backoff",
			state:   RegState{Phase: Dialing},
			event:   DialFail,
			jitter:  0.5,
			want:    RegState{Phase: Dialing, Attempt: 1},
			wantCmd: []Command{{Kind: Wait, Wait: Backoff(1, DefaultBackoffCfg, 0.5)}},
		},
		{
			name:    "Dialing+DialFail preserves Enrolled while incrementing Attempt",
			state:   RegState{Phase: Dialing, Enrolled: true, Attempt: 1},
			event:   DialFail,
			jitter:  0.1,
			want:    RegState{Phase: Dialing, Enrolled: true, Attempt: 2},
			wantCmd: []Command{{Kind: Wait, Wait: Backoff(2, DefaultBackoffCfg, 0.1)}},
		},
		{
			name:   "repeated DialFail crosses FailoverThreshold",
			state:  RegState{Phase: Dialing, Attempt: FailoverThreshold - 1},
			event:  DialFail,
			jitter: 0.2,
			want:   RegState{Phase: Dialing, Attempt: FailoverThreshold},
			wantCmd: []Command{
				{Kind: Failover},
				{Kind: Wait, Wait: Backoff(FailoverThreshold, DefaultBackoffCfg, 0.2)},
			},
		},
		{
			name:  "DialFail beyond threshold keeps failing over",
			state: RegState{Phase: Dialing, Attempt: FailoverThreshold + 4},
			event: DialFail,
			want:  RegState{Phase: Dialing, Attempt: FailoverThreshold + 5},
			wantCmd: []Command{
				{Kind: Failover},
				{Kind: Wait, Wait: Backoff(FailoverThreshold+5, DefaultBackoffCfg, 0)},
			},
		},
		{
			name:  "Dialing+Enrolled is a no-op",
			state: RegState{Phase: Dialing},
			event: Enrolled,
			want:  RegState{Phase: Dialing},
		},
		{
			name:  "Dialing+Joined is a no-op",
			state: RegState{Phase: Dialing},
			event: Joined,
			want:  RegState{Phase: Dialing},
		},
		{
			name:    "Dialing+ConnLost re-dials",
			state:   RegState{Phase: Dialing, Attempt: 2},
			event:   ConnLost,
			want:    RegState{Phase: Dialing},
			wantCmd: []Command{{Kind: Dial}},
		},
		{
			name:  "Dialing+Tick is a no-op",
			state: RegState{Phase: Dialing},
			event: Tick,
			want:  RegState{Phase: Dialing},
		},

		// --- Enrolling ---
		{
			name:    "Enrolling+Enrolled joins a cell",
			state:   RegState{Phase: Enrolling},
			event:   Enrolled,
			want:    RegState{Phase: Joining, Enrolled: true},
			wantCmd: []Command{{Kind: JoinCell}},
		},
		{
			name:  "Enrolling+DialOK is a no-op",
			state: RegState{Phase: Enrolling},
			event: DialOK,
			want:  RegState{Phase: Enrolling},
		},
		{
			name:  "Enrolling+DialFail is a no-op",
			state: RegState{Phase: Enrolling},
			event: DialFail,
			want:  RegState{Phase: Enrolling},
		},
		{
			name:  "Enrolling+Joined is a no-op",
			state: RegState{Phase: Enrolling},
			event: Joined,
			want:  RegState{Phase: Enrolling},
		},
		{
			name:    "Enrolling+ConnLost re-dials without identity",
			state:   RegState{Phase: Enrolling},
			event:   ConnLost,
			want:    RegState{Phase: Dialing},
			wantCmd: []Command{{Kind: Dial}},
		},
		{
			name:  "Enrolling+Tick is a no-op",
			state: RegState{Phase: Enrolling},
			event: Tick,
			want:  RegState{Phase: Enrolling},
		},

		// --- Joining ---
		{
			name:    "Joining+Joined becomes a member",
			state:   RegState{Phase: Joining, Enrolled: true},
			event:   Joined,
			want:    RegState{Phase: Member, Enrolled: true},
			wantCmd: []Command{{Kind: Heartbeat}},
		},
		{
			name:  "Joining+DialOK is a no-op",
			state: RegState{Phase: Joining, Enrolled: true},
			event: DialOK,
			want:  RegState{Phase: Joining, Enrolled: true},
		},
		{
			name:  "Joining+DialFail is a no-op",
			state: RegState{Phase: Joining, Enrolled: true},
			event: DialFail,
			want:  RegState{Phase: Joining, Enrolled: true},
		},
		{
			name:  "Joining+Enrolled is a no-op",
			state: RegState{Phase: Joining, Enrolled: true},
			event: Enrolled,
			want:  RegState{Phase: Joining, Enrolled: true},
		},
		{
			name:    "Joining+ConnLost re-dials, keeps identity",
			state:   RegState{Phase: Joining, Enrolled: true},
			event:   ConnLost,
			want:    RegState{Phase: Dialing, Enrolled: true},
			wantCmd: []Command{{Kind: Dial}},
		},
		{
			name:  "Joining+Tick is a no-op",
			state: RegState{Phase: Joining, Enrolled: true},
			event: Tick,
			want:  RegState{Phase: Joining, Enrolled: true},
		},

		// --- Member ---
		{
			name:    "Member+Tick heartbeats",
			state:   RegState{Phase: Member, Enrolled: true},
			event:   Tick,
			want:    RegState{Phase: Member, Enrolled: true},
			wantCmd: []Command{{Kind: Heartbeat}},
		},
		{
			name:    "Member+ConnLost re-dials, keeps identity",
			state:   RegState{Phase: Member, Enrolled: true, Attempt: 0},
			event:   ConnLost,
			want:    RegState{Phase: Dialing, Enrolled: true},
			wantCmd: []Command{{Kind: Dial}},
		},
		{
			name:  "Member+DialOK is a no-op",
			state: RegState{Phase: Member, Enrolled: true},
			event: DialOK,
			want:  RegState{Phase: Member, Enrolled: true},
		},
		{
			name:  "Member+DialFail is a no-op",
			state: RegState{Phase: Member, Enrolled: true},
			event: DialFail,
			want:  RegState{Phase: Member, Enrolled: true},
		},
		{
			name:  "Member+Enrolled is a no-op",
			state: RegState{Phase: Member, Enrolled: true},
			event: Enrolled,
			want:  RegState{Phase: Member, Enrolled: true},
		},
		{
			name:  "Member+Joined is a no-op",
			state: RegState{Phase: Member, Enrolled: true},
			event: Joined,
			want:  RegState{Phase: Member, Enrolled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, cmds := Step(tt.state, tt.event, model.Instant(0), tt.jitter)
			if got != tt.want {
				t.Fatalf("Step() state = %+v, want %+v", got, tt.want)
			}
			if !reflect.DeepEqual(cmds, tt.wantCmd) {
				t.Fatalf("Step() commands = %+v, want %+v", cmds, tt.wantCmd)
			}
		})
	}
}

// TestConnLostFromMemberKeepsIdentity pins the scenario named explicitly in
// the ticket: a Member that loses its connection goes back to Dialing but
// does not lose its enrolled identity, so the next successful dial skips
// re-enrollment.
func TestConnLostFromMemberKeepsIdentity(t *testing.T) {
	member := RegState{Phase: Member, Enrolled: true}

	dialing, cmds := Step(member, ConnLost, model.Instant(42), 0)
	if dialing.Phase != Dialing {
		t.Fatalf("phase = %v, want Dialing", dialing.Phase)
	}
	if !dialing.Enrolled {
		t.Fatal("Enrolled = false, want true — identity must survive ConnLost")
	}
	if !reflect.DeepEqual(cmds, []Command{{Kind: Dial}}) {
		t.Fatalf("commands = %+v, want [Dial]", cmds)
	}

	// The subsequent DialOK must skip Enrolling entirely.
	joining, cmds := Step(dialing, DialOK, model.Instant(43), 0)
	if joining.Phase != Joining {
		t.Fatalf("phase = %v, want Joining (enroll-once skips re-enrollment)", joining.Phase)
	}
	if !reflect.DeepEqual(cmds, []Command{{Kind: JoinCell}}) {
		t.Fatalf("commands = %+v, want [JoinCell]", cmds)
	}
}

// TestRepeatedDialFailTriggersFailover pins the other scenario the ticket
// names: enough consecutive DialFail events without an intervening success
// must eventually produce a Failover command, and the retry loop never
// terminates on its own (retry-forever).
func TestRepeatedDialFailTriggersFailover(t *testing.T) {
	s := RegState{Phase: Dialing}
	sawFailover := false

	for i := 0; i < FailoverThreshold+2; i++ {
		var cmds []Command
		s, cmds = Step(s, DialFail, model.Instant(0), 0.3)
		if s.Phase != Dialing {
			t.Fatalf("attempt %d: phase = %v, want Dialing (retry-forever)", i, s.Phase)
		}
		for _, c := range cmds {
			if c.Kind == Failover {
				sawFailover = true
			}
		}
		if i+1 < FailoverThreshold {
			for _, c := range cmds {
				if c.Kind == Failover {
					t.Fatalf("attempt %d: unexpected Failover before threshold %d", i, FailoverThreshold)
				}
			}
		}
	}

	if !sawFailover {
		t.Fatalf("expected Failover after %d consecutive DialFail events, got none (final state %+v)", FailoverThreshold+2, s)
	}
}

// TestStepIsDeterministic guards the core's defining property: identical
// inputs always produce identical output, run after run.
func TestStepIsDeterministic(t *testing.T) {
	cases := []struct {
		state RegState
		event RegEvent
	}{
		{RegState{Phase: Dialing}, DialOK},
		{RegState{Phase: Dialing, Attempt: 4}, DialFail},
		{RegState{Phase: Enrolling}, Enrolled},
		{RegState{Phase: Joining, Enrolled: true}, Joined},
		{RegState{Phase: Member, Enrolled: true}, Tick},
		{RegState{Phase: Member, Enrolled: true}, ConnLost},
	}

	for _, c := range cases {
		wantState, wantCmds := Step(c.state, c.event, model.Instant(1000), 0.42)
		for i := 0; i < 50; i++ {
			gotState, gotCmds := Step(c.state, c.event, model.Instant(1000), 0.42)
			if gotState != wantState {
				t.Fatalf("non-deterministic state on run %d for %+v/%v: %+v vs %+v", i, c.state, c.event, gotState, wantState)
			}
			if !reflect.DeepEqual(gotCmds, wantCmds) {
				t.Fatalf("non-deterministic commands on run %d for %+v/%v: %+v vs %+v", i, c.state, c.event, gotCmds, wantCmds)
			}
		}
	}
}

func TestBackoffMonotonicAndCapped(t *testing.T) {
	cfg := BackoffCfg{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2}

	// At jitter 0 the delay is half of the capped exponential value, so it
	// plateaus at cfg.Max/2 once the exponential term saturates.
	var prev time.Duration
	for attempt := 0; attempt <= 20; attempt++ {
		d := Backoff(attempt, cfg, 0)
		if d < prev {
			t.Fatalf("attempt %d: Backoff = %v, went down from %v", attempt, d, prev)
		}
		if d > cfg.Max {
			t.Fatalf("attempt %d: Backoff = %v, exceeds cfg.Max %v", attempt, d, cfg.Max)
		}
		prev = d
	}
	if want := cfg.Max / 2; prev != want {
		t.Fatalf("Backoff at attempt 20, jitter 0 = %v, want it plateaued at cfg.Max/2 %v", prev, want)
	}

	// At jitter 1 the delay reaches the full cap once the exponential term
	// saturates, and never exceeds it.
	prev = 0
	for attempt := 0; attempt <= 20; attempt++ {
		d := Backoff(attempt, cfg, 1)
		if d < prev {
			t.Fatalf("attempt %d: Backoff = %v, went down from %v", attempt, d, prev)
		}
		if d > cfg.Max {
			t.Fatalf("attempt %d: Backoff = %v, exceeds cfg.Max %v", attempt, d, cfg.Max)
		}
		prev = d
	}
	if prev != cfg.Max {
		t.Fatalf("Backoff at attempt 20, jitter 1 = %v, want it saturated at cfg.Max %v", prev, cfg.Max)
	}
}

func TestBackoffJitterBounds(t *testing.T) {
	cfg := BackoffCfg{Base: 200 * time.Millisecond, Max: 5 * time.Second, Factor: 3}

	tests := []struct {
		name    string
		attempt int
		jitter  float64
	}{
		{"attempt 0, jitter 0", 0, 0},
		{"attempt 0, jitter 1", 0, 1},
		{"attempt 3, jitter 0", 3, 0},
		{"attempt 3, jitter 1", 3, 1},
		{"attempt way past cap, jitter 0.5", 30, 0.5},
		{"negative attempt clamps to 0", -1, 0.5},
		{"jitter below 0 clamps to 0", 3, -5},
		{"jitter above 1 clamps to 1", 3, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Backoff(tt.attempt, cfg, tt.jitter)
			if d < 0 {
				t.Fatalf("Backoff() = %v, want non-negative", d)
			}
			if d > cfg.Max {
				t.Fatalf("Backoff() = %v, exceeds cfg.Max %v", d, cfg.Max)
			}
		})
	}
}

// TestBackoffDeterministicGivenJitter shows Backoff draws no randomness of
// its own: the same (attempt, cfg, jitter) always yields the same duration.
func TestBackoffDeterministicGivenJitter(t *testing.T) {
	cfg := DefaultBackoffCfg
	want := Backoff(3, cfg, 0.618)
	for i := 0; i < 100; i++ {
		if got := Backoff(3, cfg, 0.618); got != want {
			t.Fatalf("non-deterministic Backoff on run %d: %v vs %v", i, got, want)
		}
	}
}

// TestBackoffExponentialGrowth pins the expected shape at jitter 0 (the
// lower bound: half of the capped exponential delay) before the cap kicks
// in, and that increasing jitter only ever increases the delay for a fixed
// attempt.
func TestBackoffExponentialGrowth(t *testing.T) {
	cfg := BackoffCfg{Base: 10 * time.Millisecond, Max: time.Hour, Factor: 2}

	want := []time.Duration{
		5 * time.Millisecond,  // half of 10ms
		10 * time.Millisecond, // half of 20ms
		20 * time.Millisecond, // half of 40ms
		40 * time.Millisecond, // half of 80ms
	}
	for attempt, w := range want {
		if got := Backoff(attempt, cfg, 0); got != w {
			t.Fatalf("Backoff(%d, cfg, 0) = %v, want %v", attempt, got, w)
		}
	}

	for attempt := 0; attempt < 4; attempt++ {
		low := Backoff(attempt, cfg, 0)
		high := Backoff(attempt, cfg, 1)
		if high < low {
			t.Fatalf("attempt %d: jitter=1 delay %v < jitter=0 delay %v", attempt, high, low)
		}
	}
}
