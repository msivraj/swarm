package honeypot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	coreenrollment "github.com/msivraj/swarm/internal/core/enrollment"
	corehoneypot "github.com/msivraj/swarm/internal/core/honeypot"
	"github.com/msivraj/swarm/internal/model"
	"github.com/msivraj/swarm/internal/shell/enrollment"
	"github.com/msivraj/swarm/internal/shell/verification"
)

// -----------------------------------------------------------------------
// Test doubles
// -----------------------------------------------------------------------

// fakeCall records one Dispatch invocation: which machine, and which task
// (by ID) it was for — enough to tell a probe dispatch from a real one.
type fakeCall struct {
	machine model.MachineID
	taskID  model.TaskID
}

// fakeDispatcher is a minimal recording verification.Dispatcher for
// ProbingDispatcher's own unit tests. Unlike verification.FakeDispatcher
// (fixed per machine), responses are configured per (machine, task ID)
// pair, so a test can give one machine a different claim for the probe task
// than for the real task — proving a probe never corrupts the real
// dispatch's result. Safe for concurrent use.
type fakeDispatcher struct {
	mu        sync.Mutex
	calls     []fakeCall
	responses map[model.MachineID]map[model.TaskID]model.Result
	errs      map[model.MachineID]map[model.TaskID]error
}

func newFakeDispatcher() *fakeDispatcher {
	return &fakeDispatcher{
		responses: make(map[model.MachineID]map[model.TaskID]model.Result),
		errs:      make(map[model.MachineID]map[model.TaskID]error),
	}
}

func (f *fakeDispatcher) set(machine model.MachineID, taskID model.TaskID, res model.Result) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.responses[machine] == nil {
		f.responses[machine] = make(map[model.TaskID]model.Result)
	}
	f.responses[machine][taskID] = res
}

func (f *fakeDispatcher) setErr(machine model.MachineID, taskID model.TaskID, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.errs[machine] == nil {
		f.errs[machine] = make(map[model.TaskID]error)
	}
	f.errs[machine][taskID] = err
}

func (f *fakeDispatcher) Dispatch(_ context.Context, machine model.MachineID, task model.Task) (model.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{machine: machine, taskID: task.ID})
	if errs, ok := f.errs[machine]; ok {
		if err, ok := errs[task.ID]; ok {
			return model.Result{}, err
		}
	}
	return f.responses[machine][task.ID], nil
}

func (f *fakeDispatcher) Calls() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeCall, len(f.calls))
	copy(out, f.calls)
	return out
}

var _ verification.Dispatcher = (*fakeDispatcher)(nil)

// fakeReputation is a minimal ReputationReader for tests: a plain map, so a
// test can give specific machines specific reputations without a full
// reputation.Store.
type fakeReputation map[model.SpiffeID]model.Reputation

func (f fakeReputation) Get(id model.SpiffeID) model.Reputation { return f[id] }

// constRNG returns a func() float64 seam that always returns v — a
// deterministic, controllable rng for tests. No math/rand: v is plain data
// chosen by the test.
func constRNG(v float64) func() float64 {
	return func() float64 { return v }
}

// sequenceRNG returns a func() float64 seam that yields each of vs in
// order, then repeats the last value for any further calls — lets a test
// force a probe on some Dispatch calls and skip it on others
// deterministically (e.g. probe on the first call only).
func sequenceRNG(vs ...float64) func() float64 {
	i := 0
	return func() float64 {
		v := vs[i]
		if i < len(vs)-1 {
			i++
		}
		return v
	}
}

// -----------------------------------------------------------------------
// ProbingDispatcher — table-driven unit tests
// -----------------------------------------------------------------------

var (
	probeTask   = model.Task{ID: "probe-task"}
	realTask    = model.Task{ID: "real-task"}
	knownResult = model.Result{Value: []byte("known-good"), OK: true}
)

func TestProbingDispatcher_ForceProbe_SecondLieCaughtAndBlacklisted(t *testing.T) {
	fd := newFakeDispatcher()
	fd.set("m1", probeTask.ID, model.Result{Value: []byte("wrong-answer"), OK: true})
	fd.set("m1", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   bl,
		RNG:         constRNG(0), // force-probe: rng=0 always below any positive probe rate
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	// First lie: below the default two-strike limit, so m1 must not be
	// blacklisted yet — a single caught lie is tolerated.
	res, err := pd.Dispatch(context.Background(), "m1", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil", err)
	}
	if string(res.Value) != "real-answer" {
		t.Errorf("Dispatch result Value = %q, want %q (probe must not corrupt the real result)", res.Value, "real-answer")
	}
	if bl.IsBlacklisted("m1") {
		t.Error("m1's FIRST caught lie already blacklisted it; want tolerated below DefaultStrikeLimit")
	}

	// Second lie: reaches the default limit (2), so m1 must now be
	// blacklisted.
	res, err = pd.Dispatch(context.Background(), "m1", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil", err)
	}
	// The real dispatch's result is returned untouched by the probe's lie —
	// a probe is a side channel, never a substitute for the real answer.
	if string(res.Value) != "real-answer" {
		t.Errorf("Dispatch result Value = %q, want %q (probe must not corrupt the real result)", res.Value, "real-answer")
	}
	if !bl.IsBlacklisted("m1") {
		t.Error("m1's SECOND caught lie did not blacklist it")
	}

	calls := fd.Calls()
	if len(calls) != 4 {
		t.Fatalf("Dispatch call count = %d, want 4 (probe+real, twice)", len(calls))
	}
	if calls[0].taskID != probeTask.ID {
		t.Errorf("first dispatched task = %q, want the probe task %q (probe happens before the real task)", calls[0].taskID, probeTask.ID)
	}
	if calls[1].taskID != realTask.ID {
		t.Errorf("second dispatched task = %q, want the real task %q", calls[1].taskID, realTask.ID)
	}
}

func TestProbingDispatcher_ForceProbe_CorrectAnswerNotBlacklisted(t *testing.T) {
	fd := newFakeDispatcher()
	fd.set("m2", probeTask.ID, knownResult) // answers the probe correctly
	fd.set("m2", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   bl,
		RNG:         constRNG(0),
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	res, err := pd.Dispatch(context.Background(), "m2", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil", err)
	}
	if string(res.Value) != "real-answer" {
		t.Errorf("Dispatch result Value = %q, want %q", res.Value, "real-answer")
	}
	if bl.IsBlacklisted("m2") {
		t.Error("m2 answered the probe correctly but was blacklisted anyway")
	}
	if len(fd.Calls()) != 2 {
		t.Fatalf("Dispatch call count = %d, want 2 (probe still happens on a correct answer)", len(fd.Calls()))
	}
}

func TestProbingDispatcher_NeverProbe_SkipsProbeDispatch(t *testing.T) {
	fd := newFakeDispatcher()
	fd.set("m3", realTask.ID, model.Result{Value: []byte("y"), OK: true})
	// No probe response is configured for m3 at all — if a probe were
	// mistakenly dispatched anyway, fd would silently answer the zero
	// Result rather than fail loudly, so the call-count/task-ID assertions
	// below are what actually catches a wrongly-triggered probe.

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   bl,
		RNG:         constRNG(0.999999), // rng≈1: never below any probe rate
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	res, err := pd.Dispatch(context.Background(), "m3", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil", err)
	}
	if string(res.Value) != "y" {
		t.Errorf("Dispatch result Value = %q, want %q", res.Value, "y")
	}
	if bl.IsBlacklisted("m3") {
		t.Error("m3 was blacklisted despite never being probed")
	}

	calls := fd.Calls()
	if len(calls) != 1 {
		t.Fatalf("Dispatch call count = %d, want 1 (no probe dispatched)", len(calls))
	}
	if calls[0].taskID != realTask.ID {
		t.Errorf("only dispatched task = %q, want the real task %q", calls[0].taskID, realTask.ID)
	}
}

func TestProbingDispatcher_ReputationSeamDrivesProbeRate(t *testing.T) {
	tests := []struct {
		name       string
		reputation ReputationReader // nil => Config.Reputation left unset
		rng        float64
		wantProbed bool
	}{
		{
			name:       "nil Reputation seam defaults to zero-value (max) probe rate",
			reputation: nil,
			rng:        0.4, // below maxProbeRate (0.5), above minProbeRate (0.05)
			wantProbed: true,
		},
		{
			name:       "trusted reputation (Score=100) uses the min probe rate floor",
			reputation: fakeReputation{"m4": {Score: 100}},
			rng:        0.4, // above minProbeRate (0.05): a trusted identity is NOT probed here
			wantProbed: false,
		},
		{
			name:       "trusted reputation still probed below the min-rate floor",
			reputation: fakeReputation{"m4": {Score: 100}},
			rng:        0.01, // below minProbeRate (0.05): the floor never disables probing
			wantProbed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fd := newFakeDispatcher()
			fd.set("m4", probeTask.ID, knownResult)
			fd.set("m4", realTask.ID, model.Result{Value: []byte("z"), OK: true})

			pd := NewProbingDispatcher(Config{
				Dispatcher:  fd,
				Reputation:  tt.reputation,
				Blacklist:   NewBlacklist(),
				RNG:         constRNG(tt.rng),
				ProbeTask:   probeTask,
				ProbeResult: knownResult,
			})

			if _, err := pd.Dispatch(context.Background(), "m4", realTask); err != nil {
				t.Fatalf("Dispatch returned error %v, want nil", err)
			}

			gotProbed := len(fd.Calls()) == 2
			if gotProbed != tt.wantProbed {
				t.Errorf("probed = %v, want %v (calls = %d)", gotProbed, tt.wantProbed, len(fd.Calls()))
			}
		})
	}
}

func TestProbingDispatcher_ProbeDispatchError_InconclusiveNoBlacklist(t *testing.T) {
	fd := newFakeDispatcher()
	fd.setErr("m5", probeTask.ID, errors.New("machine unreachable"))
	fd.set("m5", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   bl,
		RNG:         constRNG(0),
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	res, err := pd.Dispatch(context.Background(), "m5", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil (the real dispatch itself succeeded)", err)
	}
	if string(res.Value) != "real-answer" {
		t.Errorf("Dispatch result Value = %q, want %q", res.Value, "real-answer")
	}
	if bl.IsBlacklisted("m5") {
		t.Error("an unreachable probe is inconclusive and must not blacklist the machine")
	}
}

func TestProbingDispatcher_NilBlacklist_NeverPanics(t *testing.T) {
	fd := newFakeDispatcher()
	fd.set("m6", probeTask.ID, model.Result{Value: []byte("wrong"), OK: true})
	fd.set("m6", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   nil, // deliberately unset
		RNG:         constRNG(0),
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	res, err := pd.Dispatch(context.Background(), "m6", realTask)
	if err != nil {
		t.Fatalf("Dispatch returned error %v, want nil", err)
	}
	if string(res.Value) != "real-answer" {
		t.Errorf("Dispatch result Value = %q, want %q", res.Value, "real-answer")
	}
}

// -----------------------------------------------------------------------
// Per-identity honeypot-lie strike tracking: the shell increments a strike
// on every caught lie and only applies the pure core's blacklist action at
// the configured limit — see #210.
// -----------------------------------------------------------------------

func TestProbingDispatcher_Strikes(t *testing.T) {
	t.Run("oneStrikeNotBlacklisted", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.set("m20", probeTask.ID, model.Result{Value: []byte("wrong-answer"), OK: true})
		fd.set("m20", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher: fd,
			Blacklist:  bl,
			// Force a probe on the first Dispatch only (rng=0), then never
			// again (rng≈1) — the second Dispatch below must not add a
			// second strike, so it isolates "one strike" from "one probe".
			RNG:         sequenceRNG(0, 0.999999),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
		})

		if _, err := pd.Dispatch(context.Background(), "m20", realTask); err != nil {
			t.Fatalf("Dispatch returned error %v, want nil", err)
		}
		if bl.IsBlacklisted("m20") {
			t.Fatal("a single caught lie already blacklisted the identity, want it tolerated below DefaultStrikeLimit")
		}

		// Still eligible: a second Dispatch (no probe forced this time, so
		// no second strike is added) reaches the inner dispatcher exactly
		// like any other machine's would.
		res, err := pd.Dispatch(context.Background(), "m20", realTask)
		if err != nil {
			t.Fatalf("subsequent Dispatch returned error %v, want nil", err)
		}
		if string(res.Value) != "real-answer" {
			t.Errorf("subsequent Dispatch result Value = %q, want %q", res.Value, "real-answer")
		}
		if bl.IsBlacklisted("m20") {
			t.Error("identity was blacklisted after one strike; want still eligible")
		}
	})

	t.Run("twoStrikesBlacklisted", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.set("m21", probeTask.ID, model.Result{Value: []byte("wrong-answer"), OK: true})
		fd.set("m21", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher:  fd,
			Blacklist:   bl,
			RNG:         constRNG(0),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
		})

		if _, err := pd.Dispatch(context.Background(), "m21", realTask); err != nil {
			t.Fatalf("first Dispatch returned error %v, want nil", err)
		}
		if bl.IsBlacklisted("m21") {
			t.Fatal("blacklisted after the FIRST lie, want only after the second")
		}

		if _, err := pd.Dispatch(context.Background(), "m21", realTask); err != nil {
			t.Fatalf("second Dispatch returned error %v, want nil", err)
		}
		if !bl.IsBlacklisted("m21") {
			t.Fatal("not blacklisted after a SECOND caught lie")
		}
	})

	t.Run("strikesPerIdentity", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.set("m22", probeTask.ID, model.Result{Value: []byte("wrong-a"), OK: true})
		fd.set("m22", realTask.ID, model.Result{Value: []byte("real-a"), OK: true})
		fd.set("m23", probeTask.ID, model.Result{Value: []byte("wrong-b"), OK: true})
		fd.set("m23", realTask.ID, model.Result{Value: []byte("real-b"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher:  fd,
			Blacklist:   bl,
			RNG:         constRNG(0),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
		})

		if _, err := pd.Dispatch(context.Background(), "m22", realTask); err != nil {
			t.Fatalf("Dispatch(m22) returned error %v, want nil", err)
		}
		if _, err := pd.Dispatch(context.Background(), "m23", realTask); err != nil {
			t.Fatalf("Dispatch(m23) returned error %v, want nil", err)
		}

		if bl.IsBlacklisted("m22") {
			t.Error("m22 blacklisted after only one lie of its own — strikes must not cross identities")
		}
		if bl.IsBlacklisted("m23") {
			t.Error("m23 blacklisted after only one lie of its own — strikes must not cross identities")
		}
	})

	t.Run("matchNoStrike", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.set("m24", probeTask.ID, knownResult) // always answers the probe correctly
		fd.set("m24", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher:  fd,
			Blacklist:   bl,
			RNG:         constRNG(0),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
		})

		// Many correct probe answers in a row: if a Match were mistakenly
		// counted as a strike, this alone would eventually reach the
		// blacklist limit — it must not.
		for i := 0; i < 5; i++ {
			if _, err := pd.Dispatch(context.Background(), "m24", realTask); err != nil {
				t.Fatalf("Dispatch #%d returned error %v, want nil", i, err)
			}
		}
		if bl.IsBlacklisted("m24") {
			t.Fatal("an honest identity that always answers the probe correctly was blacklisted")
		}

		// Now switch to a lie: this is the identity's FIRST real strike, so
		// it alone must not blacklist. If the prior matches had wrongly
		// incremented the counter, this single lie would push it over the
		// limit.
		fd.set("m24", probeTask.ID, model.Result{Value: []byte("now-lying"), OK: true})
		if _, err := pd.Dispatch(context.Background(), "m24", realTask); err != nil {
			t.Fatalf("Dispatch after switching to a lie returned error %v, want nil", err)
		}
		if bl.IsBlacklisted("m24") {
			t.Error("blacklisted on its first lie — prior matches must not have counted as strikes")
		}
	})

	t.Run("probeErrorNoStrike", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.setErr("m25", probeTask.ID, errors.New("machine unreachable"))
		fd.set("m25", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher:  fd,
			Blacklist:   bl,
			RNG:         constRNG(0),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
		})

		// Repeated inconclusive (errored) probes: if an error were wrongly
		// counted as a strike, enough of them would eventually blacklist
		// the identity — it must not, no matter how many.
		for i := 0; i < 5; i++ {
			res, err := pd.Dispatch(context.Background(), "m25", realTask)
			if err != nil {
				t.Fatalf("Dispatch #%d returned error %v, want nil (the real dispatch itself succeeded)", i, err)
			}
			if string(res.Value) != "real-answer" {
				t.Errorf("Dispatch #%d result Value = %q, want %q", i, res.Value, "real-answer")
			}
		}
		if bl.IsBlacklisted("m25") {
			t.Error("blacklisted after repeated probe-dispatch errors, want errors to never count as strikes")
		}
	})

	t.Run("customStrikeLimitShiftsThreshold", func(t *testing.T) {
		fd := newFakeDispatcher()
		fd.set("m26", probeTask.ID, model.Result{Value: []byte("wrong-answer"), OK: true})
		fd.set("m26", realTask.ID, model.Result{Value: []byte("real-answer"), OK: true})

		bl := NewBlacklist()
		pd := NewProbingDispatcher(Config{
			Dispatcher:  fd,
			Blacklist:   bl,
			RNG:         constRNG(0),
			ProbeTask:   probeTask,
			ProbeResult: knownResult,
			StrikeLimit: 3,
		})

		for i, wantBlacklisted := range []bool{false, false, true} {
			if _, err := pd.Dispatch(context.Background(), "m26", realTask); err != nil {
				t.Fatalf("Dispatch #%d returned error %v, want nil", i+1, err)
			}
			if got := bl.IsBlacklisted("m26"); got != wantBlacklisted {
				t.Errorf("after strike %d: IsBlacklisted = %v, want %v (StrikeLimit=3)", i+1, got, wantBlacklisted)
			}
		}
	})
}

// TestConfig_strikeLimit tests Config.strikeLimit's default behavior
// directly: zero/unset falls back to corehoneypot.DefaultStrikeLimit, and a
// configured positive value is used unchanged.
func TestConfig_strikeLimit(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want int
	}{
		{"zero value defaults", Config{}, corehoneypot.DefaultStrikeLimit},
		{"negative value defaults", Config{StrikeLimit: -1}, corehoneypot.DefaultStrikeLimit},
		{"configured value used", Config{StrikeLimit: 5}, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.strikeLimit(); got != tt.want {
				t.Errorf("strikeLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// Concurrency safety (-race): many concurrent Dispatch calls across many
// machines, mixing honest and lying behavior, must land in the correct
// blacklist state with no data race.
// -----------------------------------------------------------------------

func TestProbingDispatcher_ConcurrentDispatch_RaceSafe(t *testing.T) {
	const nMachines = 40
	const dispatchesPerMachine = 2 // == DefaultStrikeLimit: exactly enough for a liar to cross it

	fd := newFakeDispatcher()
	liars := make(map[model.MachineID]bool, nMachines)
	for i := 0; i < nMachines; i++ {
		m := model.MachineID(fmt.Sprintf("m%d", i))
		liars[m] = i%2 == 0
		if liars[m] {
			fd.set(m, probeTask.ID, model.Result{Value: []byte("lie"), OK: true})
		} else {
			fd.set(m, probeTask.ID, knownResult)
		}
		fd.set(m, realTask.ID, model.Result{Value: []byte("real"), OK: true})
	}

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher:  fd,
		Blacklist:   bl,
		RNG:         constRNG(0), // force-probe every dispatch
		ProbeTask:   probeTask,
		ProbeResult: knownResult,
	})

	// Two concurrent Dispatch calls per machine — enough for a liar's strike
	// count to reach DefaultStrikeLimit, and enough to race the SAME
	// identity's strike counter against itself (not just across machines).
	var wg sync.WaitGroup
	for m := range liars {
		for i := 0; i < dispatchesPerMachine; i++ {
			wg.Add(1)
			go func(machine model.MachineID) {
				defer wg.Done()
				if _, err := pd.Dispatch(context.Background(), machine, realTask); err != nil {
					t.Errorf("Dispatch(%s) returned error %v", machine, err)
				}
			}(m)
		}
	}
	wg.Wait()

	for m, lied := range liars {
		if got := bl.IsBlacklisted(identityOf(m)); got != lied {
			t.Errorf("IsBlacklisted(%s) = %v, want %v", m, got, lied)
		}
	}
}

// -----------------------------------------------------------------------
// End-to-end: ProbingDispatcher wired in front of the real
// verification.Coordinator, sharing one Blacklist that both the
// coordinator's K-set filter and the enrollment shell consult (fork b,
// #132). Proves a caught lie is not just recorded — it is excluded from
// every future K-set.
// -----------------------------------------------------------------------

func TestHoneypot_EndToEnd_BlacklistedMachineExcludedFromFutureKSets(t *testing.T) {
	pool := []model.MachineID{"m1", "m2", "m3", "m4", "m5"}
	honestValue := []byte("the-honest-answer")
	lieValue := []byte("a-lie")

	disp := verification.NewFakeDispatcher()
	disp.Honest("m1", honestValue)
	disp.Honest("m2", honestValue)
	disp.Honest("m3", honestValue)
	disp.Honest("m4", honestValue)
	disp.Lying("m5", lieValue) // m5 lies about everything it's asked, probe included

	bl := NewBlacklist()
	pd := NewProbingDispatcher(Config{
		Dispatcher: disp,
		Blacklist:  bl,
		RNG:        constRNG(0), // force-probe every dispatch this round
		ProbeTask:  model.Task{ID: "honeypot-probe"},
		// The known-good answer to the injected probe happens to be the
		// same value real honest work returns in this test — a valid
		// setup, since a honeypot task's known answer is whatever the
		// operator planted, independent of what any given real task asks.
		ProbeResult: model.Result{Value: honestValue, OK: true},
		// StrikeLimit: 1 here so a single probe within round 1 blacklists
		// m5 — this test is about propagation (a blacklisted identity is
		// excluded from every future K-set), not about the two-strike
		// threshold itself, which dispatcher_test.go's strike tests cover
		// directly.
		StrikeLimit: 1,
	})

	coord := verification.New(verification.Config{
		Dispatcher:  pd,
		Blacklist:   bl, // the SAME shared blacklist ProbingDispatcher writes into
		Clock:       verification.NewFakeClock(0),
		Timeout:     1000,
		MaxAttempts: 1,
	})

	const requester = model.SpiffeID("req-1")
	ctx := context.Background()

	// Round 1: m5 is still in the pool. Honest machines hold the majority,
	// so the round still reaches Agreed even though m5 is dispatched (and,
	// via the honeypot probe, caught and blacklisted during this very
	// round).
	v1, err := coord.Verify(ctx, model.Task{ID: "job-1"}, model.Open, requester, pool, 1)
	if err != nil {
		t.Fatalf("round 1 Verify returned error %v, want nil", err)
	}
	if v1.Kind != model.Agreed || string(v1.Value) != string(honestValue) {
		t.Fatalf("round 1 Verdict = %+v, want Agreed(%q)", v1, honestValue)
	}

	if !bl.IsBlacklisted("m5") {
		t.Fatal("m5 lied on the honeypot probe but was not blacklisted after round 1")
	}
	for _, m := range []model.MachineID{"m1", "m2", "m3", "m4"} {
		if bl.IsBlacklisted(identityOf(m)) {
			t.Errorf("honest machine %s was blacklisted", m)
		}
	}

	callsAfterRound1 := len(disp.Calls())

	// Round 2: a fresh task, same pool (m5 still listed). The coordinator's
	// blacklist filter must exclude m5 from eligibility entirely — it is
	// never assigned, never dispatched, and so never has a chance to
	// re-lie.
	v2, err := coord.Verify(ctx, model.Task{ID: "job-2"}, model.Open, requester, pool, 100)
	if err != nil {
		t.Fatalf("round 2 Verify returned error %v, want nil", err)
	}
	if v2.Kind != model.Agreed || string(v2.Value) != string(honestValue) {
		t.Fatalf("round 2 Verdict = %+v, want Agreed(%q)", v2, honestValue)
	}

	round2Calls := disp.Calls()[callsAfterRound1:]
	if len(round2Calls) == 0 {
		t.Fatal("round 2 dispatched to no machines at all")
	}
	for _, m := range round2Calls {
		if m == "m5" {
			t.Fatal("round 2 dispatched to m5, a blacklisted identity — it must be dropped from every future K-set")
		}
	}
}

// TestHoneypot_EndToEnd_BlacklistAlsoRefusesEnrollmentAdmission additionally
// demonstrates the shared Blacklist's other consumer (fork b, #132): the
// enrollment shell refuses a join whose derived identity is on the very
// blacklist ProbingDispatcher writes to.
func TestHoneypot_EndToEnd_BlacklistAlsoRefusesEnrollmentAdmission(t *testing.T) {
	bl := NewBlacklist()

	req := model.JoinReq{PubKey: []byte("attacker-pubkey")}
	pow := model.PowProof{}
	cfg := model.PowCfg{DifficultyBits: 0} // PoW disabled: any well-formed request is admitted by the core

	admit := coreenrollment.AdmitOpen(req, pow, cfg)
	if admit.Kind != model.Accept {
		t.Fatalf("AdmitOpen = %+v, want Accept (test setup)", admit)
	}

	// Simulate a prior honeypot lie having blacklisted this exact identity
	// — the same write path ProbingDispatcher.probe uses.
	bl.Apply(model.Action{Kind: model.Blacklist, ID: admit.ID})

	issuer := enrollment.NewFakeIssuer()
	keys := enrollment.NewKeyring()
	enroller := enrollment.NewEnroller(cfg, issuer, bl, keys)

	result, err := enroller.Enroll(req, pow)
	if err != nil {
		t.Fatalf("Enroll returned error %v, want nil", err)
	}
	if result.Status != enrollment.StatusBlacklisted {
		t.Fatalf("Enroll Status = %v, want StatusBlacklisted", result.Status)
	}
	if issuer.Issued(admit.ID) {
		t.Error("a blacklisted identity was issued a certificate")
	}
}
