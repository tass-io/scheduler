package middleware

import (
	"github.com/tass-io/scheduler/pkg/span"
	"sort"
)

type Source string

type Decision string
var (
	handlers    = make(map[Source]Handler)
	orderOrigin = make(map[int][]Handler)
)


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

func Register(source Source, handler Handler, order int) {
	handlers[source] = handler
	level, existed := orderOrigin[order]
	if !existed {
		level = []Handler{}
	}
	level = append(level, handler)
}

var Middlewares = func() map[Source]Handler {
	return handlers
}

var Orders = func() []Source {
	keys := make([]int, 0, len(orderOrigin))
	handlers := make([]Source, 0, len(orderOrigin))
	for key := range orderOrigin {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, key := range keys {
		levelHandlers := orderOrigin[key]
		for _, handler := range levelHandlers {
			handlers = append(handlers, handler.GetSource())
		}
	}
	return handlers
}
