package runner

import (
	"testing"
)

func TestRunner(t *testing.T) {
	NewInstance = func(functionName string) instance {
		return NewMockInstance(functionName)
	}
}
