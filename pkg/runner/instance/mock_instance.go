package instance

import "github.com/tass-io/scheduler/pkg/tools/common"

type mockInstance struct {
	functionName string
}

func (m *mockInstance) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	output, err := common.CopyMap(parameters)
	output[m.functionName] = m.functionName
	return output, err
}

func (m *mockInstance) Score() int {
	return 1
}

func (m *mockInstance) Release() {
}

func (m *mockInstance) Start() error {
	return nil
}

func NewMockInstance(functionName string) *mockInstance {
	return &mockInstance{
		functionName: functionName,
	}
}
