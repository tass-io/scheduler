package middleware

import (
	"fmt"
	"sort"

	"github.com/tass-io/scheduler/pkg/span"
)

// Source indicates the source of an event
type Source string

// Decision is the action a function takes
type Decision string

const (
	Abort Decision = "Abort"
	Next  Decision = "Next"
)

var (
	// handlers records all Middleware Handler has registered
	handlers = make(map[Source]MiddlewareHandler)
	// orderOrigin is an inverted map that record all middleware Handlers in different level
	orderOrigin = make(map[int][]MiddlewareHandler)
)

// MiddlewareHandler for synchronous handle like update span info
// todo define pointcut
type MiddlewareHandler interface {
	Handle(*span.Span, map[string]interface{}) (map[string]interface{}, Decision, error)
	GetSource() Source
}

// Register registers a handler by the given source
// It registers:
// 1. Source and middleware hander function in handlers map
// 2. Put handler into the orderOrigin map according to the input "order"
func Register(source Source, handler MiddlewareHandler, order int) {
	fmt.Printf("register %s with order %d\n", source, order)
	handlers[source] = handler
	level, existed := orderOrigin[order]
	if !existed {
		level = []MiddlewareHandler{}
	}
	level = append(level, handler)
	orderOrigin[order] = level
}

// Middleware return all middleware handlers 
var Middlewares = func() map[Source]MiddlewareHandler {
	return handlers
}

// Orders returns a slice of Source in increasing order
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

// FindMiddlewareBySource returns a middleware handler by source
func FindMiddlewareBySource(src Source) MiddlewareHandler {
	return handlers[src]
}
