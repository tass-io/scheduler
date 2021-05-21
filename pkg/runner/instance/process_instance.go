package instance

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
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

var (
	binary   = "/proc/self/exe"
	policies = map[string]func(*processInstance) int{
		"default": defaultPolicyfunc,
	}
)

func defaultPolicyfunc(i *processInstance) int {
	now := time.Now().Unix()
	birthday := i.startTime.Unix() // happy birthday for LittleDrizzle. He write this line at the birthday (smddx)
	return int(now - birthday)
}

type processInstance struct {
	startTime       time.Time
	uuid            string
	lock            sync.Locker
	functionName    string
	Status          Status
	cpu             string // todo we thought about implementing resource limitation either at instance create
	memory          string
	environment     string
	producer        *Producer
	consumer        *Consumer
	responseMapping map[string]chan map[string]interface{}
	cmd             *exec.Cmd
}

// todo
func (i *processInstance) Score() int {
	if i.Status != Running {
		return runner.SCORE_MAX
	}
	policyName := viper.GetString(env.InstanceScorePolicy)
	return policies[policyName](i)
}

func NewProcessInstance(functionName string) *processInstance {
	function, existed, err := k8sutils.GetFunctionByName(functionName)
	if err != nil {
		zap.S().Warnw("new process instances get function by name error", "functionName", functionName)
		return nil
	}
	if !existed {
		zap.S().Warnw("function infomartion not found", "functionName", functionName)
		return nil
	}
	return &processInstance{
		startTime:       time.Now(),
		uuid:            xid.New().String(),
		lock:            &sync.Mutex{},
		functionName:    functionName,
		cpu:             function.Spec.Resource.Cpu,
		memory:          function.Spec.Resource.Memory,
		environment:     string(function.Spec.Environment),
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
	i.producer = NewProducer(producerWrite, &FunctionRequest{})
	i.producer.Start()
	i.consumer = NewConsumer(consumerRead, &FunctionResponse{})
	i.consumer.Start()
	err = i.startProcess(producerRead, consumerWrite, i.functionName)
	if err != nil {
		return
	}
	i.startListen()
	i.Status = Running
	return
}

func (i *processInstance) startListen() {
	go func() {
		for respRaw := range i.consumer.GetChannel() {
			resp := respRaw.(*FunctionResponse)
			i.responseMapping[resp.Id] <- resp.Result
		}
	}()
}

// start function process and use pipe create connection
// todo customize the command
func (i *processInstance) startProcess(request *os.File, response *os.File, functionName string) (err error) {
	initParam := fmt.Sprintf("init -n %s -I %s -P %s -S %s -E %s", functionName, viper.GetString(env.RedisIp), viper.GetString(env.RedisPort), viper.GetString(env.RedisPassword), i.environment)
	cmd := exec.Command(binary, strings.Split(initParam, " ")...)
	// It is different from docker, we do not create mount namespace and network namespace
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWUTS | syscall.CLONE_NEWIPC,
	}
	_ = os.MkdirAll(fmt.Sprintf("%slogs/", env.TassFileRoot), 0777)
	logFileName := fmt.Sprintf("%slogs/%s.log", env.TassFileRoot, i.uuid)
	logFile, err := os.Create(logFileName)
	zap.S().Debugw("init log file", "logFileName", logFileName)
	if err != nil {
		zap.S().Errorw("init log file error", "err", err)
	} else {
		cmd.Stdout = logFile
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
		zap.S().Errorw("processInstance cmd exit error", "processInstance", i.uuid, "err", err)
	}
}

// Send SIGTERM to process and trigger clean up
// Process Release step
/* 1. send SIGTERM to function process
 *  2. close main request channel
 *  3. when function process get eof about request, set noNewInfo true
 *  4. when function process has no requests handling, function process Wrapper.requestMap is empty.
 *  5. when function process sends all responses, close file
 *  6. when main consumer get eof about response, all things have been done.
 */
func (i *processInstance) Release() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.Status = Terminating
	err := i.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		zap.S().Errorw("process send SIGTERM error", "err", err)
	}
	i.cleanUp()
}

func (i *processInstance) IsRunning() bool {
	return i.Status == Running
}

// cleanUp will be used at processInstance exception or graceful shut down
func (i *processInstance) cleanUp() {
	i.producer.Terminate()
}

// Invoke will generate a functionRequest and will block until the function return the result
func (i *processInstance) Invoke(parameters map[string]interface{}) (result map[string]interface{}, err error) {
	if i.Status != Running {
		zap.S().Infow("process instance Invoke", "status", i.Status)
		return nil, InstanceNotServiceErr
	}
	i.lock.Lock()
	id := xid.New().String()
	req := NewFunctionRequest(id, parameters)
	i.responseMapping[id] = make(chan map[string]interface{})
	i.producer.GetChannel() <- *req
	i.lock.Unlock()
	result = <-i.responseMapping[id]
	delete(i.responseMapping, id)
	return
}

func (i *processInstance) getWaitNum() int {
	return len(i.responseMapping)
}
