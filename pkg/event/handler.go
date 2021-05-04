package event

// Source show the ScheduleEvent's source, ScheduleHandler has a priority table to
type Source string

// event Handler for async handle like cold start
type Handler interface {
	AddEvent(interface{})
	GetSource() Source
	Start() error
}

// HandlerRegister point
// todo add private function for internal handler inject
var Register = func() map[Source]Handler {
	return map[Source]Handler{
		GetScheduleHandlerIns().GetSource(): GetScheduleHandlerIns(),
	}
}
