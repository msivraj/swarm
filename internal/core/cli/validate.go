package cli

import (
	"fmt"

	"github.com/msivraj/swarm/internal/model"
)

// Validate checks a JobSpec before it crosses the wire, so the CLI fails
// fast on a bad job instead of paying a round trip to the control plane.
func Validate(spec model.JobSpec) error {
	if spec.Template == "" {
		return fmt.Errorf("cli: template is required")
	}
	if spec.Coupling < model.Independent || spec.Coupling > model.MessagePassing {
		return fmt.Errorf("cli: unknown coupling %d", spec.Coupling)
	}
	for k := range spec.Params {
		if k == "" {
			return fmt.Errorf("cli: param keys must not be empty")
		}
	}
	return nil
}
