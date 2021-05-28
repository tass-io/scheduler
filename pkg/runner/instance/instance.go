package instance

import (
	"errors"
)

var InstanceNotServiceErr = errors.New("instance not service")

type Instance interface {
	Invoke(parameters map[string]interface{}) (map[string]interface{}, error)
	Score() int
	Release()
	IsRunning() bool
	Start() error
	HasRequests() bool
}
