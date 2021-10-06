package event

import (
	"sort"
)

var (
	// handlers records all event Handlers has registered
	handlers = make(map[Source]Handler)
	// invertedHandlers is an inverted map which records all event Handlers in different level
	invertedHandlers = make(map[int][]Handler)
	// deleted stores the policy about Source
	// it indicates that if a event has been used, whether it should be delete by the board
	deleted = make(map[Source]bool)
)

// Handler for async handle like cold start, scale up and down...
type Handler interface {
	AddEvent(e ScheduleEvent)
	GetSource() Source
	Start() error
}

// Register is the point for HandlerRegister
// it registers:
// 1. Source and hander function in handlers map
// 2. Put handler into the invertedHandlers map according to the input "order"
func Register(source Source, handler Handler, order int, delete bool) {
	registerHandler(source, handler)
	registerOrderedHandler(source, handler, order)
	registerDeletedSource(source, delete)
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

func registerDeletedSource(source Source, delete bool) {
	deleted[source] = delete
}

// GetHandlers returns the handlers map that stores the Source and its handler
func GetHandlers() map[Source]Handler {
	return handlers
}

// GetOrderedSources returns a Source slice in increasing order.
var GetOrderedSources = func() []Source {
	keys := make([]int, 0, len(invertedHandlers))
	for key := range invertedHandlers {
		keys = append(keys, key)
	}

	sources := make([]Source, 0, len(invertedHandlers))
	sort.Ints(keys)
	for _, key := range keys {
		levelHandlers := invertedHandlers[key]
		for _, handler := range levelHandlers {
			sources = append(sources, handler.GetSource())
		}
	}
	return sources
}

// NeedDelete returns whether the event(the kind is Source) should be deleted after using
func NeedDelete(source Source) bool {
	return deleted[source]
}

// GetHandlerBySource returns the function handler for the given source
func GetHandlerBySource(src Source) Handler {
	return handlers[src]
}
