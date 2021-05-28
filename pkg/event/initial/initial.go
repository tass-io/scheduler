package initial

import (
	"github.com/tass-io/scheduler/pkg/event/qps"
	"github.com/tass-io/scheduler/pkg/event/schedule"
)

func Initial() {
	qps.Initial()
	schedule.Initial()
}
