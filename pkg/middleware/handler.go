package middleware

import (
	"sort"

	"github.com/tass-io/scheduler/pkg/span"
)

var (
	// handlers records all Middleware Handler has registered
	handlers = make(map[Source]Handler)
	// invertedHandlers is an inverted map that record all middleware Handlers in different level
	// the smaller the key is, the higher the order is
	invertedHandlers = make(map[int][]Handler)
)

// Handler for synchronous handle like update span info
// todo define pointcut
type Handler interface {
	Handle(*span.Span, map[string]interface{}) (map[string]interface{}, Decision, error)
	GetSource() Source
}

// Register registers a handler by the given source
// It registers:
// 1. Source and middleware hander function in handlers map
// 2. Put handler into the invertedHandlers map according to the input "order"
func Register(source Source, handler Handler, order int) {
	registerHandler(source, handler)
	registerOrderedHandler(source, handler, order)
}

func registerHandler(source Source, handler Handler) {
	handlers[source] = handler
}

func registerOrderedHandler(source Source, handler Handler, order int) {
	level, existed := invertedHandlers[order]
	if !existed {
		level = []Handler{}
	}
	level = append(level, handler)
	invertedHandlers[order] = level
}

// GetHandlers returns all registered middleware handlers
func GetHandlers() map[Source]Handler {
	return handlers
}

// GetOrderedSources returns a Source slice in increasing order
func GetOrderedSources() []Source {
	keys := make([]int, 0, len(invertedHandlers))
	for key := range invertedHandlers {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	sources := make([]Source, 0, len(invertedHandlers))
	for _, key := range keys {
		levelHandlers := invertedHandlers[key]
		for _, handler := range levelHandlers {
			sources = append(sources, handler.GetSource())
		}
	}
	return sources
}

// GetHandlerBySource returns a middleware handler by source
func GetHandlerBySource(src Source) Handler {
	return handlers[src]
}
