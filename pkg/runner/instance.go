package runner

// processInstance Status will be updated by FunctionScheduler
// first it will patch to apiserver, and then change local status, finally it will send SIGTERM to process
type Status int32

const (
	Init        Status = 0
	Running     Status = 1
	Terminating Status = 2
	Terminated  Status = 3
)

type InstanceType string

const (
	ProcessInstance InstanceType = "ProcessInstance"
	MockInstance    InstanceType = "MockInstance"
)

type instance interface {
	Invoke(parameters map[string]interface{}) (map[string]interface{}, error)
	Score() int
	Release()
	Start() error
}
