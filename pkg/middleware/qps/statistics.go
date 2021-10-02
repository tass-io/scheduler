package qps

import "go.uber.org/zap"

type Statistics interface {
	// GetStat returns the qps number of each function.
	GetStat() map[string]int64
}

// GetStat returns the qps number of each function which is concurrency safe.
func (qps *qpsMiddleware) GetStat() map[string]int64 {
	stats := map[string]int64{}
	qps.qpsManagers.Range(func(key, value interface{}) bool {
		mgr := value.(*manager)
		stats[key.(string)] = mgr.Get()
		return true
	})
	zap.S().Debugw("get stats at QPSMiddleware", "stats", stats)
	return stats
}

var _ Statistics = &qpsMiddleware{}
