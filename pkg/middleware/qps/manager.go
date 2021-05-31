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
	qpsmiddle *QPSMiddleware
)

func init() {
	qpsmiddle = newQPSMiddleware()
}

func Register() {
	middleware.Register(QPSMiddlewareSource, qpsmiddle, 3)
}

// StaticMiddleware will check fs status and use some policy to handle request, which make the request have chance to redirect to other Local Scheduler
type QPSMiddleware struct {
	qpsManagers sync.Map
}

func newQPSMiddleware() *QPSMiddleware {
	return &QPSMiddleware{
		qpsManagers: sync.Map{},
	}
}

func GetQPSMiddleware() *QPSMiddleware {
	return qpsmiddle
}

// Handle records metrics when a request comes
func (qps *QPSMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, middleware.Decision, error) {
	functionName := sp.FunctionName
	mgrRaw, _ := qps.qpsManagers.LoadOrStore(functionName, newQPSManager(1000))
	mgr := mgrRaw.(*QPSManager)
	mgr.Inc()
	zap.S().Debugw("qps handler inc", "functionName", functionName)
	return nil, middleware.Next, nil
}

func (qps *QPSMiddleware) GetSource() middleware.Source {
	return QPSMiddlewareSource
}

// GetStat returns the qps number of each function
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
