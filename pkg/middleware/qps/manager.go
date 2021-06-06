package qps

import (
	"sync"

	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
	"go.uber.org/zap"
)

const (
	QPSMiddlewareSource middleware.Source = "QPS"
)

var (
	// qpsmiddle is the middleware for statisticsis
	qpsmiddle *QPSMiddleware
)

// init initializes the qps middleware
func init() {
	qpsmiddle = newQPSMiddleware()
}

// Register registers the qps middleware as a priority of 2
func Register() {
	middleware.Register(QPSMiddlewareSource, qpsmiddle, 3)
}

// qpsmiddle is the middleware for statisticsis,
// it only does some statisticsis for different functions,
// it does nothing about Event adding
type QPSMiddleware struct {
	qpsManagers sync.Map
}

// newQPSMiddleware returns a new QPS middleware
func newQPSMiddleware() *QPSMiddleware {
	return &QPSMiddleware{
		qpsManagers: sync.Map{},
	}
}

// GetQPSMiddleware returns the QPS middleware instance
func GetQPSMiddleware() *QPSMiddleware {
	return qpsmiddle
}

// Handle records metrics when a request comes
func (qps *QPSMiddleware) Handle(sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {
	qpsSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	qpsSpan.Start("qps")
	functionName := qpsSpan.GetFunctionName()
	mgrRaw, _ := qps.qpsManagers.LoadOrStore(functionName, newQPSManager(1000))
	mgr := mgrRaw.(*QPSManager)
	mgr.Inc()
	zap.S().Debugw("qps handler inc", "functionName", functionName)
	qpsSpan.Finish()
	return nil, middleware.Next, nil
}

// GetSource returns the QPS middleware source
func (qps *QPSMiddleware) GetSource() middleware.Source {
	return QPSMiddlewareSource
}

// GetStat returns the qps number of each function which is concurrency safe.
func (qps *QPSMiddleware) GetStat() map[string]int64 {
	stats := map[string]int64{}
	qps.qpsManagers.Range(func(key, value interface{}) bool {
		mgr := value.(*QPSManager)
		stats[key.(string)] = mgr.Get()
		return true
	})
	zap.S().Debugw("get stats at QPSMiddleware", "stats", stats)
	return stats
}
