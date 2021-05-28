package event

import (
	"sort"

	"github.com/tass-io/scheduler/pkg/event/source"
)

var (
	// handlers will record all Handlers has registered
	handlers = make(map[source.Source]Handler)
	// orderOrigin will record all Handlers in different level
	orderOrigin = make(map[int][]Handler)
	// deleted store the policy about Decide
	// if a event has used, whether the board should delete it
	deleted = make(map[source.Source]bool)
)

// event Handler for async handle like cold start
type Handler interface {
	AddEvent(interface{})
	GetSource() source.Source
	Start() error
}

// HandlerRegister point
func Register(source source.Source, handler Handler, order int, delete bool) {
	handlers[source] = handler
	level, existed := orderOrigin[order]
	if !existed {
		level = []Handler{}
	}
	level = append(level, handler)
	orderOrigin[order] = level
	deleted[source] = delete
}

var Events = func() map[source.Source]Handler {
	return handlers
}

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

func NeedDelete(source source.Source) bool {
	return deleted[source]
}

func FindEventHandlerBySource(src source.Source) Handler {
	return handlers[src]
}
