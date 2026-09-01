package model

import "testing"

// TestTrustTierOrder pins the iota order of the TrustTier constants and the
// zero-value contract: BaselineTrust must be zero so a machine with no
// attestation provider (and thus no result) still runs, at baseline trust.
func TestTrustTierOrder(t *testing.T) {
	tests := []struct {
		name string
		got  TrustTier
		want TrustTier
	}{
		{"BaselineTrust", BaselineTrust, 0},
		{"AttestedTrust", AttestedTrust, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d, want %d", tt.name, tt.got, tt.want)
			}
		})
	}

	var zero TrustTier
	if zero != BaselineTrust {
		t.Fatalf("zero TrustTier = %d, want BaselineTrust (%d)", zero, BaselineTrust)
	}
}

// TestAttResultZeroIsInvalid asserts the security-load-bearing contract: an
// un-set AttResult reads as Invalid (Valid == false), never as trusted. This
// is fail-closed by construction — Go's bool zero value is false.
func TestAttResultZeroIsInvalid(t *testing.T) {
	var zero AttResult
	if zero.Valid {
		t.Fatalf("zero AttResult.Valid = true, want false (Invalid, fail-closed)")
	}
	if zero.Measurements != nil {
		t.Fatalf("zero AttResult.Measurements = %v, want nil", zero.Measurements)
	}

	valid := AttResult{Valid: true, Measurements: [][]byte{{0x01, 0x02}}}
	if !valid.Valid || len(valid.Measurements) != 1 || valid.Measurements[0][0] != 0x01 {
		t.Fatalf("AttResult did not round-trip: %+v", valid)
	}
}

// TestAttQuoteAndPolicyZeroAndRoundTrip asserts the quote/policy types'
// zero values are usable and that fields round-trip once populated. Both
// types are format-agnostic — nothing TPM-specific.
func TestAttQuoteAndPolicyZeroAndRoundTrip(t *testing.T) {
	t.Run("AttQuote", func(t *testing.T) {
		var zero AttQuote
		if zero.Measurements != nil || zero.Nonce != nil || zero.Signature != nil || zero.Signer != nil {
			t.Fatalf("zero AttQuote = %+v, want all nil", zero)
		}
		q := AttQuote{
			Measurements: [][]byte{{0xAA}, {0xBB}},
			Nonce:        []byte("nonce"),
			Signature:    Sig("sig"),
			Signer:       PubKey("key"),
		}
		if len(q.Measurements) != 2 || string(q.Nonce) != "nonce" || string(q.Signature) != "sig" || string(q.Signer) != "key" {
			t.Fatalf("AttQuote did not round-trip: %+v", q)
		}
	})

	t.Run("AttPolicy", func(t *testing.T) {
		var zero AttPolicy
		if zero.Expected != nil || zero.TrustedKey != nil || zero.ExpectedNonce != nil {
			t.Fatalf("zero AttPolicy = %+v, want all nil", zero)
		}
		p := AttPolicy{
			Expected:      [][]byte{{0xAA}},
			TrustedKey:    PubKey("key"),
			ExpectedNonce: []byte("nonce"),
		}
		if len(p.Expected) != 1 || string(p.TrustedKey) != "key" || string(p.ExpectedNonce) != "nonce" {
			t.Fatalf("AttPolicy did not round-trip: %+v", p)
		}
	})
}
