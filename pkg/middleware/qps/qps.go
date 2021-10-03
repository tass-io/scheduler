package qps

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

var (
	// qps is the middleware for statisticsis
	qps *qpsMiddleware
)

// init initializes the qps middleware
func init() {
	qps = newQPSMiddleware()
}

// Register registers the qps middleware as a priority of 10
func Register() {
	middleware.Register(middleware.QPSMiddlewareSource, qps, 10)
}

// qpsMiddleware is the qps middleware for statisticsis,
// it only does some statisticsis for different functions,
// it does nothing about Event adding
type qpsMiddleware struct {
	qpsRecorders sync.Map
}

var _ middleware.Handler = &qpsMiddleware{}

// newQPSMiddleware returns a new QPS middleware
func newQPSMiddleware() *qpsMiddleware {
	return &qpsMiddleware{
		qpsRecorders: sync.Map{},
	}
}

// GetQPSMiddleware returns the QPS middleware instance
func GetQPSMiddleware() middleware.Handler {
	return qps
}

// Handle records metrics when a request comes
func (qps *qpsMiddleware) Handle(
	sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {

	qpsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	qpsSpan.Start("qps")
	defer qpsSpan.Finish()

	functionName := qpsSpan.GetFunctionName()
	mgrRaw, _ := qps.qpsRecorders.LoadOrStore(functionName, newQPSRecorder(1000))
	mgr := mgrRaw.(*recorder)
	mgr.Inc()
	zap.S().Debugw("qps handler inc", "functionName", functionName)
	return nil, middleware.Next, nil
}

// GetSource returns the QPS middleware source
func (qps *qpsMiddleware) GetSource() middleware.Source {
	return middleware.QPSMiddlewareSource
}
