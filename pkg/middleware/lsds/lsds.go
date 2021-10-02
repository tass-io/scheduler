package lsds

import (
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/runner/helper"
	runnerlsds "github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

var (
	// lsds is the local scheduler discovery service middleware for local scheduler
	// lsds is the short name for "local scheduler discovery service"
	lsds *lsdsMiddleware
)

// init initializes the lsds middleware
func init() {
	lsds = newLSDSMiddleware()
}

// Register registers the lsds middleware as a priority of 3
func Register() {
	middleware.Register(middleware.LSDSMiddlewareSource, lsds, 3)
}

// lsdsMiddleware checks status for function,
// which make the requests have a chance to redirect to other Local Scheduler
type lsdsMiddleware struct{}

var _ middleware.Handler = &lsdsMiddleware{}

// newLSDSMiddleware returns a lsds middleware
func newLSDSMiddleware() *lsdsMiddleware {
	return &lsdsMiddleware{}
}

// Handle receives a request and does lsds middleware logic.
// lsds middleware checks the instance existance again (the first time is cold start middleware),
// if still not exists, it forwards the function request to LSDS Runner
func (lsds *lsdsMiddleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	lsdsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	lsdsSpan.Start("lsds")
	defer lsdsSpan.Finish()

	functionName := sp.GetFunctionName()
	instanceNum := helper.GetMasterRunner().FunctionStats(functionName)

	zap.S().Infow("status at lsds middleware", "function", functionName, "number", instanceNum)

	// no running instances
	if instanceNum == 0 {
		zap.S().Warnw("no running instances available in local scheduler", "function", functionName)
		result, err := runnerlsds.GetLSDSIns().Run(sp, body)
		if err != nil {
			zap.S().Errorw("lsds middleware run error", "err", err)
			return nil, middleware.Abort, err
		}
		return result, middleware.Abort, nil
	}
	return nil, middleware.Next, nil
}

// GetSource returns the middleware source
func (lsds *lsdsMiddleware) GetSource() middleware.Source {
	return middleware.LSDSMiddlewareSource
}
