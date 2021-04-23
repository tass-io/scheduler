package local

import (
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/rs/xid"
)

// Instance Status will be updated by FunctionScheduler, first it will patch to apiserver, and then change local status, finally it will send SIGTERM to process
type Status int32

const (
	Init        Status = 0
	Running     Status = 1
	Terminating Status = 2
)

type instance struct {
	sync.Locker
	functionName    string
	Status          Status
	producer        *producer
	consumer        *consumer
	responseMapping map[string]chan map[string]interface{}
}

func NewInstance(functionName string) *instance {
	return &instance{
		Locker:          &sync.Mutex{},
		functionName:    functionName,
		responseMapping: make(map[string]chan map[string]interface{}, 10),
	}
}

func NewPipe() (*os.File, *os.File, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	return read, write, nil

}

// instance start will init and start producer/consumer, and start the real work process
func (i *instance) Start() (err error) {
	producerRead, producerWrite, err := NewPipe()
	if err != nil {
		return
	}
	consumerRead, consumerWrite, err := NewPipe()
	if err != nil {
		return
	}
	i.producer = NewProducer(producerWrite)
	i.producer.Start()
	i.consumer = NewConsumer(consumerRead)
	i.consumer.Start()
	i.startProcess(producerRead, consumerWrite)
	return
}

// start function process and use pipe create connection
// todo customize the command
func (i *instance) startProcess(request *os.File, response *os.File) (err error) {
	cmd := exec.Command("/proc/self/exe", "init")
	// It is different from docker, we do not create mount namespace and network namespace
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID | syscall.CLONE_NEWIPC,
	}
	cmd.ExtraFiles = []*os.File{request, response}
	cmd.Start()
	return
}

// Invoke will generate a functionRequest and will block until the function return the result
func (i *instance) Invoke(parameters map[string]interface{}) (result map[string]interface{}, err error) {
	id := xid.New().String()
	req := NewFunctionRequest(id, parameters)
	i.producer.GetChannel() <- *req
	return
}

func (i *instance) getWaitNum() int {
	i.Lock()
	defer i.Unlock()
	return len(i.responseMapping)
}

// choose a target instance by specific policy
func ChooseTargetInstance(instances []*instance) (target *instance) {
	max := 999999
	for _, instance := range instances {
		if instance.Status != Running {
			continue
		}
		waitNum := instance.getWaitNum()
		if waitNum < max {
			max = waitNum
			target = instance
		}
	}
	return
}
