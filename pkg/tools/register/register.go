package register

var AddScheduleEvent func(string, int, string, string)

// Register registers an scheduleEvent
// NOTE: this is a helper register function,
// in order to avoid future modules causing "import cycles not allowed" error,
// they can still call the registered function.
func Register(f func(string, int, string, string)) {
	AddScheduleEvent = f
}
