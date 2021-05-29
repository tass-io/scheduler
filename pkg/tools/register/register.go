package register

var AddScheduleEvent func(string, int, string, string)

func Register(f func(string, int, string, string)) {
	AddScheduleEvent = f
}
