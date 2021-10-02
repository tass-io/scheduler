package middleware

// Decision is the action a function takes
type Decision string

const (
	Abort Decision = "Abort"
	Next  Decision = "Next"
)

// Source indicates the source of a middleware
type Source string

const (
	StaticMiddlewareSource Source = "Static"
	QPSMiddlewareSource    Source = "QPS"
	LSDSMiddlewareSource   Source = "LSDS"
	ColdstartSource        Source = "Coldstart"
)
