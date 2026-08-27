package cli

import (
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		spec    model.JobSpec
		wantErr bool
	}{
		{
			name: "a good spec is accepted",
			spec: model.JobSpec{Template: "wordcount", Coupling: model.Independent, Params: map[string]string{"k": "v"}},
		},
		{
			name: "no params is accepted",
			spec: model.JobSpec{Template: "wordcount", Coupling: model.Leader},
		},
		{
			name:    "empty template is rejected",
			spec:    model.JobSpec{Template: "", Coupling: model.Independent},
			wantErr: true,
		},
		{
			name:    "negative coupling is rejected",
			spec:    model.JobSpec{Template: "t", Coupling: -1},
			wantErr: true,
		},
		{
			name:    "coupling past the known range is rejected",
			spec:    model.JobSpec{Template: "t", Coupling: model.MessagePassing + 1},
			wantErr: true,
		},
		{
			name:    "an empty param key is rejected",
			spec:    model.JobSpec{Template: "t", Coupling: model.Independent, Params: map[string]string{"": "v"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate(%+v) error = %v, wantErr %v", tt.spec, err, tt.wantErr)
			}
		})
	}
}

// TestValidateIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestValidateIsDeterministic(t *testing.T) {
	spec := model.JobSpec{Template: "wordcount", Coupling: model.Independent, Params: map[string]string{"k": "v"}}
	first := Validate(spec)
	for i := 0; i < 100; i++ {
		got := Validate(spec)
		if (got == nil) != (first == nil) {
			t.Fatalf("non-deterministic output on run %d: %v vs %v", i, got, first)
		}
	}
}
