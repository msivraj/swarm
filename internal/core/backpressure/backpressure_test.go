package backpressure

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

// stdLimits mirrors the doc's headline example: capacity 100, shed at 95%.
var stdLimits = model.Limits{Capacity: 100, ShedThreshold: 0.95}

func load(inFlight, queueDepth int) model.LoadState {
	return model.LoadState{InFlight: inFlight, QueueDepth: queueDepth}
}

func req(priority int) model.Req {
	return model.Req{Priority: priority}
}

// admitUnderLoad pins the doc example ("at 95% capacity, low-priority
// request -> Shed") plus the Admit/Throttle/Shed boundary transitions.
func TestAdmitUnderLoad(t *testing.T) {
	tests := []struct {
		name string
		req  model.Req
		load model.LoadState
		lim  model.Limits
		want model.LoadDecision
	}{
		{
			name: "doc headline: 95% capacity, low-priority -> Shed",
			req:  req(0),
			load: load(95, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Shed},
		},
		{
			name: "comfortably below capacity admits any priority",
			req:  req(0),
			load: load(10, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.AdmitLoad},
		},
		{
			name: "just below low-water mark still admits",
			req:  req(0),
			load: load(49, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.AdmitLoad},
		},
		{
			name: "at the low-water mark, throttle begins",
			req:  req(0),
			load: load(50, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Throttle, Delay: 0},
		},
		{
			name: "just below shed threshold: throttle with a large delay",
			req:  req(0),
			load: load(94, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Throttle, Delay: 977777777},
		},
		{
			name: "at shed threshold exactly: shed",
			req:  req(0),
			load: load(95, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Shed},
		},
		{
			name: "above shed threshold: still shed",
			req:  req(0),
			load: load(96, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Shed},
		},
		{
			name: "queue depth contributes to the ratio like in-flight does",
			req:  req(0),
			load: load(0, 95),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Shed},
		},
		{
			name: "priority 5 at 95%: throttled, not shed",
			req:  req(5),
			load: load(95, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Throttle, Delay: 899999999},
		},
		{
			name: "priority 50 at 95%: throttled with a smaller delay than priority 0's shed edge",
			req:  req(50),
			load: load(95, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.Throttle, Delay: 473684210},
		},
		{
			name: "zero capacity sheds rather than divide by zero",
			req:  req(0),
			load: load(0, 0),
			lim:  model.Limits{Capacity: 0, ShedThreshold: 0.95},
			want: model.LoadDecision{Kind: model.Shed},
		},
		{
			name: "idle load with configured capacity admits",
			req:  req(0),
			load: load(0, 0),
			lim:  stdLimits,
			want: model.LoadDecision{Kind: model.AdmitLoad},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AdmitUnderLoad(tt.req, tt.load, tt.lim)
			if got != tt.want {
				t.Fatalf("AdmitUnderLoad() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// restrictiveness orders decisions from least to most restrictive, so the
// monotonicity property can be checked with a plain <=.
func restrictiveness(k model.LoadDecisionKind) int {
	switch k {
	case model.AdmitLoad:
		return 0
	case model.Throttle:
		return 1
	case model.Shed:
		return 2
	default:
		return 2
	}
}

// monotonicity: as load climbs, a fixed request's decision only ever gets
// MORE restrictive (Admit -> Throttle -> Shed), never less, and a Throttle
// delay never decreases as load keeps climbing. Swept over a fixed load
// range via repeated UpdateLoad arrivals — deterministic, no randomness.
func TestAdmitUnderLoadMonotonicity(t *testing.T) {
	lim := stdLimits
	priorities := []int{0, 1, 5, 20}

	for _, p := range priorities {
		r := req(p)
		l := model.LoadState{}
		prevRestrictiveness := 0
		prevDelay := model.Duration(0)

		for step := 0; step < 200; step++ {
			got := AdmitUnderLoad(r, l, lim)
			rst := restrictiveness(got.Kind)

			if rst < prevRestrictiveness {
				t.Fatalf("priority %d, step %d: decision relaxed from restrictiveness %d to %d (load=%+v)",
					p, step, prevRestrictiveness, rst, l)
			}
			if got.Kind == model.Throttle && rst == prevRestrictiveness && got.Delay < prevDelay {
				t.Fatalf("priority %d, step %d: throttle delay decreased from %d to %d (load=%+v)",
					p, step, prevDelay, got.Delay, l)
			}
			prevRestrictiveness = rst
			if got.Kind == model.Throttle {
				prevDelay = got.Delay
			}

			l = UpdateLoad(l, model.LoadEvent{InFlightDelta: 1})
		}
	}
}

// priority never treated worse than a lower priority at the same load: for
// every load level in a fixed sweep, a higher-priority request's decision is
// never more restrictive than a lower-priority request's.
func TestAdmitUnderLoadPriorityNeverWorse(t *testing.T) {
	lim := stdLimits
	priorityPairs := [][2]int{{0, 1}, {0, 5}, {1, 5}, {5, 50}, {0, 100}}

	for _, pair := range priorityPairs {
		lo, hi := pair[0], pair[1]
		l := model.LoadState{}
		for step := 0; step < 200; step++ {
			loDecision := AdmitUnderLoad(req(lo), l, lim)
			hiDecision := AdmitUnderLoad(req(hi), l, lim)

			if restrictiveness(hiDecision.Kind) > restrictiveness(loDecision.Kind) {
				t.Fatalf("load=%+v: priority %d got %v but lower priority %d got %v",
					l, hi, hiDecision.Kind, lo, loDecision.Kind)
			}
			l = UpdateLoad(l, model.LoadEvent{InFlightDelta: 1})
		}
	}
}

// effectiveShedThreshold clamps to lowWaterMark rather than inverting the
// band when a pathologically low (negative) priority would otherwise push
// the effective shed threshold below the point throttling even begins.
func TestEffectiveShedThresholdClampsAtLowWaterMark(t *testing.T) {
	lim := model.Limits{Capacity: 100, ShedThreshold: 0.1}
	got := effectiveShedThreshold(req(-100), lim)
	if got != lowWaterMark {
		t.Fatalf("effectiveShedThreshold() = %v, want clamped %v", got, lowWaterMark)
	}
}

// throttleDelay clamps its fraction to [0,1] even when called with a ratio
// outside the [lowWaterMark, shedAt) band AdmitUnderLoad itself always
// bounds it to — white-box coverage of the clamp the caller relies on to
// keep the linear scale well-defined at its edges.
func TestThrottleDelayClamps(t *testing.T) {
	tests := []struct {
		name   string
		ratio  float64
		shedAt float64
		want   model.Duration
	}{
		{name: "ratio below the band clamps to zero delay", ratio: 0.1, shedAt: 0.95, want: 0},
		{name: "ratio at or above shedAt clamps to max delay", ratio: 0.99, shedAt: 0.95, want: maxThrottleDelay},
		{name: "zero-width band returns zero delay", ratio: 0.6, shedAt: lowWaterMark, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := throttleDelay(tt.ratio, tt.shedAt)
			if got != tt.want {
				t.Fatalf("throttleDelay(%v, %v) = %v, want %v", tt.ratio, tt.shedAt, got, tt.want)
			}
		})
	}
}

// updateLoad: an arrival raises measured load, a completion lowers it, load
// never goes negative, and folding a sequence is order-consistent for the
// net effect of commuting arrivals/completions.
func TestUpdateLoad(t *testing.T) {
	tests := []struct {
		name  string
		start model.LoadState
		ev    model.LoadEvent
		want  model.LoadState
	}{
		{
			name:  "arrival raises in-flight and queue depth",
			start: load(1, 2),
			ev:    model.LoadEvent{InFlightDelta: 1, QueueDelta: 1},
			want:  load(2, 3),
		},
		{
			name:  "completion lowers in-flight",
			start: load(5, 0),
			ev:    model.LoadEvent{InFlightDelta: -1},
			want:  load(4, 0),
		},
		{
			name:  "dequeue lowers queue depth",
			start: load(0, 5),
			ev:    model.LoadEvent{QueueDelta: -1},
			want:  load(0, 4),
		},
		{
			name:  "in-flight clamps at zero, never negative",
			start: load(0, 0),
			ev:    model.LoadEvent{InFlightDelta: -5},
			want:  load(0, 0),
		},
		{
			name:  "queue depth clamps at zero, never negative",
			start: load(0, 0),
			ev:    model.LoadEvent{QueueDelta: -5},
			want:  load(0, 0),
		},
		{
			name:  "large completion past current in-flight clamps, doesn't go negative",
			start: load(3, 0),
			ev:    model.LoadEvent{InFlightDelta: -10},
			want:  load(0, 0),
		},
		{
			name:  "zero event is a no-op",
			start: load(7, 3),
			ev:    model.LoadEvent{},
			want:  load(7, 3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UpdateLoad(tt.start, tt.ev)
			if got != tt.want {
				t.Fatalf("UpdateLoad() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// Folding an arrival then its matching completion (or vice versa) commutes
// in net effect: the order does not change the resulting LoadState.
func TestUpdateLoadCommutesArrivalCompletion(t *testing.T) {
	start := load(10, 4)
	arrival := model.LoadEvent{InFlightDelta: 1, QueueDelta: 1}
	completion := model.LoadEvent{InFlightDelta: -1, QueueDelta: -1}

	arriveFirst := UpdateLoad(UpdateLoad(start, arrival), completion)
	completeFirst := UpdateLoad(UpdateLoad(start, completion), arrival)

	if arriveFirst != completeFirst {
		t.Fatalf("UpdateLoad does not commute: arrival-then-completion=%+v, completion-then-arrival=%+v",
			arriveFirst, completeFirst)
	}
	if arriveFirst != start {
		t.Fatalf("net effect of arrival+completion should be a no-op: got %+v, want %+v", arriveFirst, start)
	}
}

// determinism: identical (req, load, lim) always yields an identical
// decision, and identical (load, ev) always yields an identical fold.
func TestDeterministic(t *testing.T) {
	loads := []model.LoadState{load(0, 0), load(50, 0), load(94, 0), load(95, 0), load(200, 50)}
	reqs := []model.Req{req(0), req(1), req(5), req(50)}
	lims := []model.Limits{stdLimits, {Capacity: 1000, ShedThreshold: 0.8}}

	for _, lim := range lims {
		for _, l := range loads {
			for _, r := range reqs {
				first := AdmitUnderLoad(r, l, lim)
				for i := 0; i < 10; i++ {
					got := AdmitUnderLoad(r, l, lim)
					if got != first {
						t.Fatalf("AdmitUnderLoad not deterministic for req=%+v load=%+v lim=%+v: %+v vs %+v",
							r, l, lim, first, got)
					}
				}
			}
		}
	}

	events := []model.LoadEvent{{InFlightDelta: 3, QueueDelta: -1}, {InFlightDelta: -5, QueueDelta: 2}}
	for _, l := range loads {
		for _, ev := range events {
			first := UpdateLoad(l, ev)
			for i := 0; i < 10; i++ {
				got := UpdateLoad(l, ev)
				if got != first {
					t.Fatalf("UpdateLoad not deterministic for load=%+v ev=%+v: %+v vs %+v", l, ev, first, got)
				}
			}
		}
	}
}
