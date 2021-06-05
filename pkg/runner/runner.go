package runner

import (
	"github.com/tass-io/scheduler/pkg/span"
)

// InstanceStatus returns the status of the Runner.
// The key is the function name and the value is the number of running instances
type InstanceStatus map[string]int

type RunnerType string

const SCORE_MAX = 9999

var (
	Mock    RunnerType = "Mock"
	Process RunnerType = "Process"
)

// Runner is responsible for running a function
type Runner interface {
	// Run runs a function
	Run(span *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error)
	// Stats returns the runner current status, which is a map that the key is the function name
	// and the value is the number of running instances
	Stats() InstanceStatus
}
