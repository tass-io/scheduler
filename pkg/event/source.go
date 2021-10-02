package event

// Trend is a type that claims what the operation expects to be done.
// It's wrapped in a ScheduleEvent to show the meaning of this event.
// For example, a ScheduleEvent that Trend is "Increase" and Target is 2
// means that you wanna increase the function instance to 2
type Trend string

const (
	None     Trend = "None" // None for init
	Increase Trend = "Increase"
	Decrease Trend = "Decrease"
)

// Source show the ScheduleEvent's source, ScheduleHandler has a priority table to
type Source string

const (
	ScheduleSource Source = "Source"
	MetricsSource  Source = "Metrics"
	QPSSource      Source = "QPS"
	TTLSource      Source = "TTL"
)
