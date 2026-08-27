package cli

import (
	"reflect"
	"testing"

	"github.com/msivraj/swarm/internal/model"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		argv    []string
		want    Command
		wantErr bool
	}{
		{
			name:    "empty argv is an error",
			argv:    nil,
			wantErr: true,
		},
		{
			name:    "unknown subcommand is an error",
			argv:    []string{"frobnicate"},
			wantErr: true,
		},
		{
			name: "submit with a job file",
			argv: []string{"submit", "job.json"},
			want: Command{Kind: Submit, JobFile: "job.json"},
		},
		{
			name:    "submit with a job file and flags is ambiguous",
			argv:    []string{"submit", "job.json", "--template", "t"},
			wantErr: true,
		},
		{
			name:    "submit with two positional args is an error",
			argv:    []string{"submit", "job.json", "extra.json"},
			wantErr: true,
		},
		{
			name:    "submit with no arguments at all is an error",
			argv:    []string{"submit"},
			wantErr: true,
		},
		{
			name: "submit with template flag only",
			argv: []string{"submit", "--template", "wordcount"},
			want: Command{Kind: Submit, Spec: model.JobSpec{
				Template: "wordcount",
				Coupling: model.Independent,
				Params:   map[string]string{},
			}},
		},
		{
			name: "submit with template, coupling, and params",
			argv: []string{
				"submit",
				"--template", "wordcount",
				"--coupling", "barrier",
				"--param", "input=s3://bucket/key",
				"--param", "shards=4",
			},
			want: Command{Kind: Submit, Spec: model.JobSpec{
				Template: "wordcount",
				Coupling: model.Barrier,
				Params:   map[string]string{"input": "s3://bucket/key", "shards": "4"},
			}},
		},
		{
			name:    "submit --template missing value is an error",
			argv:    []string{"submit", "--template"},
			wantErr: true,
		},
		{
			name:    "submit --coupling missing value is an error",
			argv:    []string{"submit", "--template", "t", "--coupling"},
			wantErr: true,
		},
		{
			name:    "submit --coupling unknown value is an error",
			argv:    []string{"submit", "--template", "t", "--coupling", "quorum"},
			wantErr: true,
		},
		{
			name:    "submit --param missing value is an error",
			argv:    []string{"submit", "--template", "t", "--param"},
			wantErr: true,
		},
		{
			name:    "submit --param malformed is an error",
			argv:    []string{"submit", "--template", "t", "--param", "noequals"},
			wantErr: true,
		},
		{
			name:    "submit unknown flag is an error",
			argv:    []string{"submit", "--bogus", "x"},
			wantErr: true,
		},
		{
			name: "ps takes no arguments",
			argv: []string{"ps"},
			want: Command{Kind: Ps},
		},
		{
			name:    "ps with arguments is an error",
			argv:    []string{"ps", "extra"},
			wantErr: true,
		},
		{
			name: "logs requires a job id",
			argv: []string{"logs", "job-1"},
			want: Command{Kind: Logs, JobID: "job-1"},
		},
		{
			name: "status is an alias for logs",
			argv: []string{"status", "job-1"},
			want: Command{Kind: Logs, JobID: "job-1"},
		},
		{
			name:    "logs with no job id is an error",
			argv:    []string{"logs"},
			wantErr: true,
		},
		{
			name:    "logs with too many args is an error",
			argv:    []string{"logs", "job-1", "job-2"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.argv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Parse(%v) error = %v, wantErr %v", tt.argv, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Parse(%v) = %+v, want %+v", tt.argv, got, tt.want)
			}
		})
	}
}

// TestParseIsDeterministic guards the core's defining property: identical
// argv always produces identical output.
func TestParseIsDeterministic(t *testing.T) {
	argv := []string{"submit", "--template", "t", "--coupling", "leader", "--param", "k=v"}
	first, err := Parse(argv)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	for i := 0; i < 100; i++ {
		got, err := Parse(argv)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic output on run %d: %+v vs %+v", i, got, first)
		}
	}
}

func TestDecodeJobSpec(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		want    model.JobSpec
		wantErr bool
	}{
		{
			name: "template only defaults to independent",
			data: `{"template":"wordcount"}`,
			want: model.JobSpec{Template: "wordcount", Coupling: model.Independent},
		},
		{
			name: "full doc",
			data: `{"template":"wordcount","coupling":"leader","params":{"k":"v"}}`,
			want: model.JobSpec{Template: "wordcount", Coupling: model.Leader, Params: map[string]string{"k": "v"}},
		},
		{
			name:    "invalid json is an error",
			data:    `not json`,
			wantErr: true,
		},
		{
			name:    "unknown coupling is an error",
			data:    `{"template":"t","coupling":"quorum"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeJobSpec([]byte(tt.data))
			if (err != nil) != tt.wantErr {
				t.Fatalf("DecodeJobSpec(%q) error = %v, wantErr %v", tt.data, err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("DecodeJobSpec(%q) = %+v, want %+v", tt.data, got, tt.want)
			}
		})
	}
}

func TestKindString(t *testing.T) {
	tests := []struct {
		k    Kind
		want string
	}{
		{Submit, "submit"},
		{Ps, "ps"},
		{Logs, "logs"},
		{Unknown, "unknown"},
		{Kind(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.k.String(); got != tt.want {
			t.Errorf("Kind(%d).String() = %q, want %q", tt.k, got, tt.want)
		}
	}
}
