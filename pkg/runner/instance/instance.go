package instance

import (
	"errors"
)

var ErrInstanceNotService = errors.New("instance not service")

// Instance is a function process instance
type Instance interface {
	// Invoke invokes an process instance
	Invoke(parameters map[string]interface{}) (map[string]interface{}, error)
	// Score returns the score of the instance, the lower the score, the higher the priority
	Score() int
	// Release terminates the instance
	Release()
	// IsRuning returns whether the instance is released or not
	IsRunning() bool
	// Start starts the instance
	Start() error
	// HasRequests returns whether the instance is dealing with requests
	HasRequests() bool
	// InitDone returns when the instance initialization done
	InitDone()
}
