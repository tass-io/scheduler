package local

type mockInstance struct {

}

func (m *mockInstance) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	return parameters, nil
}

func (m *mockInstance) Score() int {
	return 1
}

func (m *mockInstance) Release() {
	return
}

func (m *mockInstance) Start() error {
	return nil
}


func NewMockInstance(functionName string) *mockInstance{
	return &mockInstance{

	}
}