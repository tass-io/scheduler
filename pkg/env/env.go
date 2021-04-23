package env

import (
	"os"

	"github.com/spf13/viper"
)

const ()

func init() {}

func initEnv(key string, defaultVal string) {
	val, exist := os.LookupEnv(key)
	if !exist {
		val = defaultVal
	}
	viper.Set(key, val)
}
