package event

import (
	"sort"
)

var (
	// handlers will record all Handlers has registered
	handlers = make(map[Source]Handler)
	// orderOrigin will record all Handlers in different level
	orderOrigin = make(map[int][]Handler)
	// deleted store the policy about Decide
	// if a event has used, whether the board should delete it
	deleted = make(map[Source]bool)
)

// Source show the ScheduleEvent's source, ScheduleHandler has a priority table to
type Source string

// event Handler for async handle like cold start
type Handler interface {
	AddEvent(interface{})
	GetSource() Source
	Start() error
}

// HandlerRegister point
func Register(source Source, handler Handler, order int, delete bool) {
	handlers[source] = handler
	level, existed := orderOrigin[order]
	if !existed {
		level = []Handler{}
	}
	level = append(level, handler)
	orderOrigin[order] = level
	deleted[source] = delete
}

var Events = func() map[Source]Handler {
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

func NeedDelete(source Source) bool {
	return deleted[source]
}
