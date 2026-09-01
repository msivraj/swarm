package tenancy

import "github.com/msivraj/swarm/internal/model"

// normalizeToCapacity converts raw, absolute-unit per-resource amounts into
// shares of capacity — raw[k] / capacity[k] for every key raw declares. See
// the package doc for why no special case is needed for a resource absent
// from (or zero in) capacity: IEEE754 division already gives the "zero
// ceiling" WithinQuota wants (+Inf for a positive demand, NaN — which
// compares false against everything — for a zero one).
func normalizeToCapacity(raw, capacity model.ResourceVec) model.ResourceVec {
	if len(raw) == 0 {
		return nil
	}
	out := make(model.ResourceVec, len(raw))
	for k, v := range raw {
		out[k] = v / capacity[k]
	}
	return out
}

// addVec returns the componentwise sum of a and b, over the union of their
// keys.
func addVec(a, b model.ResourceVec) model.ResourceVec {
	out := make(model.ResourceVec, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] += v
	}
	return out
}

// subVecFloor returns a's components minus b's, floored at 0 per component,
// over the union of their keys.
func subVecFloor(a, b model.ResourceVec) model.ResourceVec {
	out := make(model.ResourceVec, len(a))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		next := out[k] - v
		if next < 0 {
			next = 0
		}
		out[k] = next
	}
	return out
}

// divVec returns v with every component divided by w.
func divVec(v model.ResourceVec, w float64) model.ResourceVec {
	if len(v) == 0 {
		return nil
	}
	out := make(model.ResourceVec, len(v))
	for k, x := range v {
		out[k] = x / w
	}
	return out
}

// copyVec returns a shallow copy of v (nil stays nil).
func copyVec(v model.ResourceVec) model.ResourceVec {
	if v == nil {
		return nil
	}
	out := make(model.ResourceVec, len(v))
	for k, x := range v {
		out[k] = x
	}
	return out
}
