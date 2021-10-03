package init

import (
	"github.com/tass-io/scheduler/pkg/event/metrics"
	"github.com/tass-io/scheduler/pkg/event/schedule"
)

func Init() {
	schedule.Init()
	metrics.Init()
}
