package initial

import (
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/middleware/lsds"
	"github.com/tass-io/scheduler/pkg/middleware/qps"
	"github.com/tass-io/scheduler/pkg/middleware/static"
)

// Initial registers middleware of qps, static and lsds
func Initial() {
	if viper.GetBool(env.QPSMiddleware) {
		qps.Register()
	}
	if viper.GetBool(env.StaticMiddleware) {
		static.Register()
	}
	if true {
		lsds.Register()
	}
}
