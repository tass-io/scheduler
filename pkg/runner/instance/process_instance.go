package instance

import (
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/rs/xid"
	"github.com/tass-io/scheduler/pkg/runner"
	"go.uber.org/zap"
)

// processInstance Status will be updated by FunctionScheduler
// first it will patch to apiserver, and then change local status, finally it will send SIGTERM to process
type Status int32

const (
	Init        Status = 0
	Running     Status = 1
	Terminating Status = 2
	Terminated  Status = 3
)

type processInstance struct {
	lock            sync.Locker
	functionName    string
	Status          Status
	producer        *producer
	consumer        *consumer
	responseMapping map[string]chan map[string]interface{}
	cmd             *exec.Cmd
}

func (i *processInstance) Score() int {
	if i.Status != Running {
		return runner.SCORE_MAX
	}
	return len(i.responseMapping)
}

func NewProcessInstance(functionName string) *processInstance {
	return &processInstance{
		lock:            &sync.Mutex{},
		functionName:    functionName,
		responseMapping: make(map[string]chan map[string]interface{}, 10),
	}
}

func newPipe() (*os.File, *os.File, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	return read, write, nil
}

// processInstance start will init and start producer/consumer, and start the real work process
func (i *processInstance) Start() (err error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.Status = Init
	producerRead, producerWrite, err := newPipe()
	if err != nil {
		return
	}
	consumerRead, consumerWrite, err := newPipe()
	if err != nil {
		return
	}
	i.producer = NewProducer(producerWrite)
	i.producer.Start()
	i.consumer = NewConsumer(consumerRead)
	i.consumer.Start()
	err = i.startProcess(producerRead, consumerWrite, i.functionName)
	if err != nil {
		return
	}
	i.startListen()
	return
}

func (i *processInstance) startListen() {
	go func() {
		for resp := range i.consumer.GetChannel() {
			i.responseMapping[resp.Id] <- resp.Result
		}
	}()
}

// start function process and use pipe create connection
// todo customize the command
func (i *processInstance) startProcess(request *os.File, response *os.File, functionName string) (err error) {
	cmd := exec.Command("/proc/self/exe", "init", "-n", functionName)
	// It is different from docker, we do not create mount namespace and network namespace
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUTS | syscall.CLONE_NEWPID | syscall.CLONE_NEWIPC,
	}
	cmd.ExtraFiles = []*os.File{request, response}
	err = cmd.Start()
	// todo how to check whether a process is init over
	i.cmd = cmd
	go i.handleCmdExit()
	return
}

func (i *processInstance) handleCmdExit() {
	err := i.cmd.Wait()
	if err != nil {
		zap.S().Errorw("processInstance cmd exit error", "processInstance", i, "err", err)
	}
	i.cleanUp()
}

func (i *processInstance) Release() {
	// todo send SIGTERM
	i.cmd.Process.Kill()
	i.cleanUp()
}

// cleanUp will be used at processInstance exception or graceful shut down
func (i *processInstance) cleanUp() {
	i.Status = Terminating
	i.consumer.Terminate()
	i.producer.Terminate()
}

// Invoke will generate a functionRequest and will block until the function return the result
func (i *processInstance) Invoke(parameters map[string]interface{}) (result map[string]interface{}, err error) {
	i.lock.Lock()
	id := xid.New().String()
	req := NewFunctionRequest(id, parameters)
	i.responseMapping[id] = make(chan map[string]interface{})
	i.producer.GetChannel() <- *req
	i.lock.Unlock()
	result = <-i.responseMapping[id]
	return
}

func (i *processInstance) getWaitNum() int {
	i.lock.Lock()
	defer i.lock.Unlock()
	return len(i.responseMapping)
}
