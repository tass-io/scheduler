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
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"go.uber.org/zap"
)

// processInstance Status will be updated by FunctionScheduler
// first it patches to apiserver, and then changes local status, finally it sends SIGTERM to process
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

// defaultPolicyfunc is a naive policy to calculate the score,
// the longer the process instance lives, the higher the score is.
func defaultPolicyfunc(i *processInstance) int {
	now := time.Now().Unix()
	birthday := i.startTime.Unix() // happy birthday for LittleDrizzle. He wrote this line at the birthday (smddx)
	return int(now - birthday)
}

// processInstance records the status of the process instance.
// this struct implements all methods in Instance interface
type processInstance struct {
	startTime    time.Time
	uuid         string
	lock         sync.Locker
	functionName string
	status       Status
	cpu          string // todo we thought about implementing resource limitation either at instance create
	memory       string
	environment  string
	producer     *Producer
	consumer     *Consumer
	// each invocation generates a unique id when invoked,
	// this field is a temporary place to store the value of the function result.
	// The key of the map is the request id.
	responseMapping map[string]chan map[string]interface{}
	cmd             *exec.Cmd
	cleanOnce       *sync.Once
}

// Score returns the score of the Process.
// The scheduler chooses the process which has the minimum score as the target
func (i *processInstance) Score() int {
	if i.status != Running {
		return runner.SCORE_MAX
	}
	policyName := viper.GetString(env.InstanceScorePolicy)
	return policies[policyName](i)
}

// NewProcessInstance creates a new process status structure.
// It contains the start time of the process, the unique uuid, function name and other information.
// Note that the initialization of the process does not init a new process,
// it only prepares useful information for the process.
// Start function creates the real process.
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
		cpu:             function.Spec.Resource.ResourceCPU,
		memory:          function.Spec.Resource.ResourceMemory,
		environment:     string(function.Spec.Environment),
		responseMapping: make(map[string]chan map[string]interface{}, 10),
		cleanOnce:       &sync.Once{},
	}
}

// newPipe creates a unnamed pipe and returns a read fd and write fd if no error.
func newPipe() (*os.File, *os.File, error) {
	read, write, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	return read, write, nil
}

// Start inits and starts producer/consumer, and starts the real work process
func (i *processInstance) Start() (err error) {
	zap.S().Debugw("process start")
	i.lock.Lock()
	defer i.lock.Unlock()
	i.status = Init
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
	return
}

// startListen ranges the consumer channels and records the response data.
func (i *processInstance) startListen() {
	go func() {
		for respRaw := range i.consumer.GetChannel() {
			resp := respRaw.(*FunctionResponse)
			i.responseMapping[resp.Id] <- resp.Result
		}
	}()
}

// startProcess starts function process, prepares log output directory
// and uses the pipe to build two connections,
// the request (param1) is the producer read pipe,
// the response (param2) is the consumer write pipe.
// todo customize the command
func (i *processInstance) startProcess(
	request *os.File, response *os.File, functionName string) (err error) {

	initParam := fmt.Sprintf("init -n %s -I %s -P %s -S %s -E %s", functionName,
		viper.GetString(env.RedisIp), viper.GetString(env.RedisPort),
		viper.GetString(env.RedisPassword), i.environment)
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

// handleCmdExit cleans the process when recieves a exit code
func (i *processInstance) handleCmdExit() {
	err := i.cmd.Wait()
	if err != nil {
		zap.S().Errorw("processInstance cmd exit error", "processInstance", i.uuid, "err", err)
		i.cleanUp()
	}

}

// Sends a SIGTERM signal to process and triggers `clean up` action
// Process Release steps
/*  1. send SIGTERM to function process
 *  2. close main request channel
 *  3. when function process get eof about request, set noNewInfo true
 *  4. when function process has no requests handling, function process Wrapper.requestMap is empty.
 *  5. when function process sends all responses, close file
 *  6. when main consumer get eof about response, all things have been done.
 */
func (i *processInstance) Release() {
	zap.S().Debugw("instance release", "id", i.uuid)
	i.lock.Lock()
	defer i.lock.Unlock()
	i.status = Terminating
	err := i.cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		zap.S().Errorw("process send SIGTERM error", "err", err)
	}
	i.cleanUp()
}

// IsRunning returns wether a process is running
func (i *processInstance) IsRunning() bool {
	return i.status == Running
}

// cleanUp is used at processInstance exception or graceful shut down
func (i *processInstance) cleanUp() {
	i.cleanOnce.Do(func() {
		i.producer.Terminate()
	})
}

// Invoke generates a functionRequest and is blocked until the function return the result
// Invoke is a process-level invoke
func (i *processInstance) Invoke(parameters map[string]interface{}) (result map[string]interface{}, err error) {
	if i.status != Running {
		zap.S().Infow("process instance Invoke", "status", i.status)
		return nil, ErrInstanceNotService
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

// getWaitNum returns the number of response data waiting for dealing with in responseMapping
func (i *processInstance) getWaitNum() int {
	return len(i.responseMapping)
}

// HasRequests returns the number of requests working in process
func (i *processInstance) HasRequests() bool {
	return len(i.responseMapping) > 0
}

// InitDone returns when the process instance initialization done, or it hangs forever.
func (i *processInstance) InitDone() {
	zap.S().Infow("process instance init done", "process", i.uuid)
	initDoneCh := i.consumer.GetInitDoneChannel()
	<-initDoneCh

	// lazy, change the status only when this method is called
	i.status = Running
}

var _ Instance = &processInstance{}
