package middleware

import "github.com/tass-io/scheduler/pkg/span"

type Source string

type Decision string

const (
	Abort Decision = "Abort"
	Next  Decision = "Next"
)

// Handler for synchronous handle like update span info
// todo define pointcut
type Handler interface {
	Handle(map[string]interface{}, *span.Span) (map[string]interface{}, Decision)
	GetSource() Source
}

func Register() map[Source]Handler {
	return map[Source]Handler{
		GetLSDSMiddleware().GetSource(): GetLSDSMiddleware(),
	}
}

var Order = func() []Source {
	return []Source{
		GetLSDSMiddleware().GetSource(),
	}
}
