package runner

import (
	"github.com/tass-io/scheduler/pkg/span"
)

type Runner interface {
	Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error)
}
