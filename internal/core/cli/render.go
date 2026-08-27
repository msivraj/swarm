package cli

import (
	"fmt"
	"strings"

	"github.com/msivraj/swarm/internal/model"
)

// ReplyKind is the tag of a Reply's tagged union — one per RPC the CLI
// renders.
type ReplyKind int

const (
	// ReplyUnknown is the zero value; a real reply is one of the kinds below.
	ReplyUnknown ReplyKind = iota
	ReplySubmit
	ReplyPs
	ReplyJobStatus
)

// Reply is the control-plane response the shell hands to Render. It is a
// tagged union over the RPC replies the CLI supports; the shell is
// responsible for filling in the fields that belong to reply.Kind.
type Reply struct {
	Kind ReplyKind

	// ReplySubmit, ReplyJobStatus
	JobID model.JobID

	// ReplyPs
	Cells    int
	Machines int
	Jobs     int

	// ReplyJobStatus
	Done      bool
	Aggregate []byte
}

// Render turns a Reply into the text the shell prints to stdout.
func Render(reply Reply) string {
	switch reply.Kind {
	case ReplySubmit:
		return fmt.Sprintf("submitted job %s\n", reply.JobID)
	case ReplyPs:
		return renderPs(reply)
	case ReplyJobStatus:
		return renderJobStatus(reply)
	default:
		return "unknown reply\n"
	}
}

func renderPs(reply Reply) string {
	var b strings.Builder
	b.WriteString("CELLS  MACHINES  JOBS\n")
	fmt.Fprintf(&b, "%-5d  %-8d  %-4d\n", reply.Cells, reply.Machines, reply.Jobs)
	return b.String()
}

func renderJobStatus(reply Reply) string {
	status := "running"
	if reply.Done {
		status = "done"
	}
	if len(reply.Aggregate) == 0 {
		return fmt.Sprintf("job %s: %s\n", reply.JobID, status)
	}
	return fmt.Sprintf("job %s: %s\naggregate: %s\n", reply.JobID, status, string(reply.Aggregate))
}
