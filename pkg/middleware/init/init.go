package init

import (
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/middleware/coldstart"
	"github.com/tass-io/scheduler/pkg/middleware/lsds"
	"github.com/tass-io/scheduler/pkg/middleware/qps"
	"github.com/tass-io/scheduler/pkg/middleware/static"
)

// Init registers middleware of qps, static and lsds
func Init() {
	if viper.GetBool(env.QPSMiddleware) {
		qps.Register()
	}
	if viper.GetBool(env.StaticMiddleware) {
		static.Register()
	}
	coldstart.Register()
	lsds.Register()
}
