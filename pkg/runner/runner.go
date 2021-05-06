package runner

import (
	"github.com/tass-io/scheduler/pkg/span"
)

type InstanceStatus map[string]int

type RunnerType string

const SCORE_MAX = 9999

var (
	Mock    RunnerType = "Mock"
	Process RunnerType = "Process"
)

type Runner interface {
	Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error)
	Stats() InstanceStatus
}

