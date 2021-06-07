package instance

import (
	"github.com/tass-io/scheduler/pkg/tools/common"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"go.uber.org/zap"
)

// mockInstance mocks a process implementing the Instance interface
type mockInstance struct {
	functionName  string
	released      bool
	handleRequest bool
}

func (m *mockInstance) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	output, err := common.CopyMap(parameters)
	output[m.functionName] = m.functionName
	m.handleRequest = true
	return output, err
}

func (m *mockInstance) Score() int {
	return 1
}

func (m *mockInstance) Release() {
	zap.S().Debugw("instance mock release")
	m.released = true
}

func (m *mockInstance) IsRunning() bool {
	return !m.released
}

func (m *mockInstance) Start() error {
	return nil
}

func (m *mockInstance) HasRequests() bool {
	return !m.handleRequest
}

func NewMockInstance(functionName string) *mockInstance {
	return &mockInstance{
		functionName:  functionName,
		released:      false,
		handleRequest: false,
	}
}
