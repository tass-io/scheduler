package event

import (
	"sort"

	"github.com/tass-io/scheduler/pkg/event/source"
)

var (
	// handlers records all event Handlers has registered
	handlers = make(map[source.Source]EventHandler)
	// orderOrigin is an inverted map which records all event Handlers in different level
	orderOrigin = make(map[int][]EventHandler)
	// deleted stores the policy about Decide
	// it indicates that if a event has been used, whether it should be delete by the board
	deleted = make(map[source.Source]bool)
)

// EventHandler for async handle like cold start, scale up and down...
type EventHandler interface {
	AddEvent(interface{})
	GetSource() source.Source
	Start() error
}

// Register is the point for HandlerRegister
// it registers:
// 1. Source and hander function in handlers map
// 2. Put handler into the orderOrigin map according to the input "order"
func Register(source source.Source, handler EventHandler, order int, delete bool) {
	handlers[source] = handler
	level, existed := orderOrigin[order]
	if !existed {
		level = []EventHandler{}
	}
	level = append(level, handler)
	orderOrigin[order] = level
	deleted[source] = delete
}

// Events returns the handlers map that stores the Source and its handler
var Events = func() map[source.Source]EventHandler {
	return handlers
}

// Orders returns a slice of Source in increasing order.
var Orders = func() []source.Source {
	keys := make([]int, 0, len(orderOrigin))
	handlers := make([]source.Source, 0, len(orderOrigin))
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

// NeedDelete returns whether the event(the kind is Source) should be deleted after using
func NeedDelete(source source.Source) bool {
	return deleted[source]
}

// FindEventHandlerBySource returns the function handler for the given source
func FindEventHandlerBySource(src source.Source) EventHandler {
	return handlers[src]
}
