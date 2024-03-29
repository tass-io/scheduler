package runner

import (
	"github.com/tass-io/scheduler/pkg/span"
)

// InstanceStatus returns the status of the Runner.
// The key is the function name and the value is the number of running instances
type InstanceStatus map[string]int

type InstanceType string

const (
	MaxScore int = 9999
)

var (
	Mock    InstanceType = "Mock"
	Process InstanceType = "Process"
)

// Runner is responsible for running a function
type Runner interface {
	// Run runs a function
	Run(span *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error)
	// Stats returns the runner current status, which is a map that the key is the function name
	// and the value is the number of running instances
	Stats() InstanceStatus
	// FunctionStats returns the current running instances numbers for the given function (param1)
	// if the instanceSet for the function doesn't exist, it returns 0
	FunctionStats(functionName string) int
}
