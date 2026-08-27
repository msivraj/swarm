package admission

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/core/templates"
	"github.com/msivraj/swarm/internal/model"
)

func TestAdmit(t *testing.T) {
	keyspaceJob := model.JobSpec{
		ID:       "j-ks",
		Template: TemplateKeyspaceSearch,
		Coupling: model.Independent,
		Params:   map[string]string{"start": "0", "end": "10", "shards": "3"},
	}
	wantKeyspaceTasks := templates.KeyspaceDecompose(templates.KeyspaceJob{
		JobID: "j-ks", Start: 0, End: 10, Shards: 3,
	})

	mcJob := model.JobSpec{
		ID:       "j-mc",
		Template: TemplateMonteCarlo,
		Coupling: model.Independent,
		Params:   map[string]string{"trials": "100", "blockSize": "30", "seed": "7"},
	}
	wantMCTasks := templates.MonteCarloDecompose(templates.MCJob{
		JobID: "j-mc", Trials: 100, BlockSize: 30, BaseSeed: 7,
	})

	tests := []struct {
		name       string
		spec       model.JobSpec
		wantTasks  []model.Task
		wantReject bool
	}{
		{
			name:      "valid keyspace-search job decomposes",
			spec:      keyspaceJob,
			wantTasks: wantKeyspaceTasks,
		},
		{
			name:      "valid monte-carlo job decomposes",
			spec:      mcJob,
			wantTasks: wantMCTasks,
		},
		{
			name: "unknown template is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: "no-such-template",
				Coupling: model.Independent,
				Params:   map[string]string{},
			},
			wantReject: true,
		},
		{
			name: "empty template is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Coupling: model.Independent,
			},
			wantReject: true,
		},
		{
			name: "non-Independent coupling is rejected for keyspace-search",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateKeyspaceSearch,
				Coupling: model.Barrier,
				Params:   map[string]string{"start": "0", "end": "10", "shards": "3"},
			},
			wantReject: true,
		},
		{
			name: "non-Independent coupling is rejected for monte-carlo",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateMonteCarlo,
				Coupling: model.Leader,
				Params:   map[string]string{"trials": "100", "blockSize": "30", "seed": "7"},
			},
			wantReject: true,
		},
		{
			name: "keyspace-search missing start is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateKeyspaceSearch,
				Coupling: model.Independent,
				Params:   map[string]string{"end": "10", "shards": "3"},
			},
			wantReject: true,
		},
		{
			name: "keyspace-search invalid end is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateKeyspaceSearch,
				Coupling: model.Independent,
				Params:   map[string]string{"start": "0", "end": "not-a-number", "shards": "3"},
			},
			wantReject: true,
		},
		{
			name: "keyspace-search missing shards is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateKeyspaceSearch,
				Coupling: model.Independent,
				Params:   map[string]string{"start": "0", "end": "10"},
			},
			wantReject: true,
		},
		{
			name: "keyspace-search empty range is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateKeyspaceSearch,
				Coupling: model.Independent,
				Params:   map[string]string{"start": "10", "end": "10", "shards": "3"},
			},
			wantReject: true,
		},
		{
			name: "monte-carlo missing trials is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateMonteCarlo,
				Coupling: model.Independent,
				Params:   map[string]string{"blockSize": "30", "seed": "7"},
			},
			wantReject: true,
		},
		{
			name: "monte-carlo invalid blockSize is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateMonteCarlo,
				Coupling: model.Independent,
				Params:   map[string]string{"trials": "100", "blockSize": "not-a-number", "seed": "7"},
			},
			wantReject: true,
		},
		{
			name: "monte-carlo missing seed is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateMonteCarlo,
				Coupling: model.Independent,
				Params:   map[string]string{"trials": "100", "blockSize": "30"},
			},
			wantReject: true,
		},
		{
			name: "monte-carlo zero trials is rejected",
			spec: model.JobSpec{
				ID:       "j1",
				Template: TemplateMonteCarlo,
				Coupling: model.Independent,
				Params:   map[string]string{"trials": "0", "blockSize": "30", "seed": "7"},
			},
			wantReject: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTasks, gotReject := Admit(tt.spec)

			if gotReject.Rejected != tt.wantReject {
				t.Fatalf("Admit() Reject.Rejected = %v, want %v (Reason: %q)", gotReject.Rejected, tt.wantReject, gotReject.Reason)
			}
			if tt.wantReject {
				if gotReject.Reason == "" {
					t.Fatal("Admit() rejected with an empty Reason")
				}
				if gotTasks != nil {
					t.Fatalf("Admit() returned tasks alongside a Reject: %+v", gotTasks)
				}
				return
			}
			if gotReject != (Reject{}) {
				t.Fatalf("Admit() Reject = %+v, want zero value", gotReject)
			}
			if !reflect.DeepEqual(gotTasks, tt.wantTasks) {
				t.Fatalf("Admit() tasks = %+v, want %+v", gotTasks, tt.wantTasks)
			}
		})
	}
}

// TestAdmitIsDeterministic guards the core's defining property: identical
// inputs always produce identical output.
func TestAdmitIsDeterministic(t *testing.T) {
	specs := []model.JobSpec{
		{
			ID:       "j-ks",
			Template: TemplateKeyspaceSearch,
			Coupling: model.Independent,
			Params:   map[string]string{"start": "3", "end": "97", "shards": "5"},
		},
		{
			ID:       "j-mc",
			Template: TemplateMonteCarlo,
			Coupling: model.Independent,
			Params:   map[string]string{"trials": "250", "blockSize": "40", "seed": "-11"},
		},
		{
			ID:       "j-bad",
			Template: "unknown",
			Coupling: model.Independent,
		},
	}

	for _, spec := range specs {
		wantTasks, wantReject := Admit(spec)
		for i := 0; i < 100; i++ {
			gotTasks, gotReject := Admit(spec)
			if !reflect.DeepEqual(gotTasks, wantTasks) || gotReject != wantReject {
				t.Fatalf("non-deterministic output on run %d for %q: (%+v, %+v) vs (%+v, %+v)",
					i, spec.ID, gotTasks, gotReject, wantTasks, wantReject)
			}
		}
	}
}
