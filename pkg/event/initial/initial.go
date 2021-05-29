package initial

import (
	"github.com/tass-io/scheduler/pkg/event/metrics"
	"github.com/tass-io/scheduler/pkg/event/schedule"
)

func Initial() {
	schedule.Initial()
	metrics.Initial()
}
