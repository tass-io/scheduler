package middleware

// Decision is the action a function takes
type Decision string

const (
	Abort Decision = "Abort"
	Next  Decision = "Next"
	Error Decision = "Error"
)

// Source indicates the source of a middleware
type Source string

const (
	StaticMiddlewareSource Source = "Static"    // priority: 1
	ColdstartSource        Source = "Coldstart" // priority: 2
	LSDSMiddlewareSource   Source = "LSDS"      // priority: 3
	QPSMiddlewareSource    Source = "QPS"       // priority: 10
)
