package env

import (
	"os"

	"github.com/spf13/viper"
)

const (
	WORKFLOW_PATH = "WORKFLOW_PATH"
)

func init() {
	initEnv(WORKFLOW_PATH, "/etc/tass/scheduler")
}

func initEnv(key string, defaultVal string) {
	val, exist := os.LookupEnv(key)
	if !exist {
		val = defaultVal
	}
	viper.Set(key, val)
}
