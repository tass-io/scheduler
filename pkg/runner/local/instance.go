package local

const (
	Init        Status = 0
	Running     Status = 1
	Terminating Status = 2
	Terminated  Status = 3
)

type InstanceType string

const (
	ProcessInstance InstanceType = "ProcessInstance"
	MockInstance InstanceType = "MockInstance"
)


type instance interface {
	Invoke(parameters map[string]interface{}) (map[string]interface{}, error)
	Score() int
	Release()
	Start() error
}