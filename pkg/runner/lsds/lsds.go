package lsds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

var ErrInvalidTarget = errors.New("no valid target")

// Policy returns the ip we choose to send request.
type Policy func(functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string

var (
	lsds *LSDS
	once = &sync.Once{}
)

// LSDS is the short of Local Scheduler Discovery Service,
// which maintains own information and syncs to apiserver
// and gets other Local Scheduler info for remote request
type LSDS struct {
	runner.Runner
	ctx          context.Context
	stopCh       chan struct{}
	lock         sync.Locker
	workflowName string
	selfName     string
	policies     map[string]Policy
}

// LSDSinit initializes a new lsds instance, which implements the runner.Runner interface
func LSDSinit() {
	// todo context thinking
	lsds = NewLSDS(context.Background())
}

// GetLSDSIns returns a lsds instance
func GetLSDSIns() *LSDS {
	once.Do(LSDSinit)
	return lsds
}

// SimplePolicy is the basic policy for lsds finds a new pod ip for the request.
// SimplePolicy iterates all pods info in WorkflowRuntime and
// chooses the pod which has the most processes of the input function
var SimplePolicy Policy = func(
	functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string {
	var target string
	max := 0
	for i, instance := range runtime.Spec.Status.Instances {
		if i == selfName {
			continue
		}
		zap.S().Debugw("get instance", "selfName", selfName, "instance", instance.ProcessRuntimes)
		if t, existed := instance.ProcessRuntimes[functionName]; existed {
			if max < t.Number {
				max = t.Number
				target = i
			}
		}
	}
	if target != "" {
		return *runtime.Spec.Status.Instances[target].Status.PodIP
	}
	return ""
}

// NewLSDS returns a new lsds instance
// client is a parameter because we will use mockclient to test
func NewLSDS(ctx context.Context) *LSDS {
	lsds := &LSDS{
		ctx:    ctx,
		stopCh: make(chan struct{}),
		lock:   &sync.Mutex{},
		policies: map[string]Policy{
			"simple": SimplePolicy,
		},
		workflowName: k8sutils.GetWorkflowName(),
		selfName:     k8sutils.GetSelfName(),
	}
	lsds.start()
	return lsds
}

// getWorkflowRuntimeByName returns the WorkflowRuntime by the input name via k8s client
func (l *LSDS) getWorkflowRuntimeByName(name string) (*serverlessv1alpha1.WorkflowRuntime, bool, error) {
	return k8sutils.GetWorkflowRuntimeByName(name)
}

// chooseTarget returns the chosen ip by policy that lsds will use to send a request
func (l *LSDS) chooseTarget(functionName string) (ip string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	wfrt, existed, err := l.getWorkflowRuntimeByName(l.workflowName)
	if err != nil {
		// todo add retry
		zap.S().Errorw("lsds get workflowruntime error", "err", err)
	}
	if !existed {
		zap.S().Warnw("workflowruntime not found", "functionName", functionName, "workflowName", l.workflowName)
		return ""
	}
	TargetPolicy := viper.GetString(env.RemoteCallPolicy)
	ip = l.policies[TargetPolicy](functionName, l.selfName, wfrt)
	zap.S().Debugw("choose target get wfrt", "wfrt", wfrt, "function", functionName, "ip", ip)
	return
}

// start starts lsds to watch other Local Scheduler Info
// FIXME: deprecated
func (l *LSDS) start() {
}

// WorkflowRequest sends a request to other LocalScheduler
func WorkflowRequest(sp *span.Span, parameters map[string]interface{}, target string) (dto.WorkflowResponse, error) {
	client := &http.Client{}
	invokeRequest := dto.WorkflowRequest{
		WorkflowName: sp.GetWorkflowName(),
		FlowName:     sp.GetFlowName(),
		Parameters:   parameters,
	}
	reqByte, err := json.Marshal(invokeRequest)
	if err != nil {
		zap.S().Errorw("workflow request body error", "err", err)
		return dto.WorkflowResponse{}, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/v1/workflow", target), strings.NewReader(string(reqByte)))
	if err != nil {
		zap.S().Errorw("workflow request request error", "err", err)
		return dto.WorkflowResponse{}, err
	}
	// so the span has same level span in two local scheduler
	sp.InjectRoot(req.Header)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		zap.S().Errorw("workflow request response error", "err", err)
		return dto.WorkflowResponse{}, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		zap.S().Errorw("workflow request response body error", "err", err)
		return dto.WorkflowResponse{}, err
	}
	invokeResp := dto.WorkflowResponse{}
	_ = json.Unmarshal(body, &invokeResp)
	return invokeResp, nil
}

// Run finds a suitable pod to send a http request
func (l *LSDS) Run(sp *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error) {
	target := l.chooseTarget(sp.GetFunctionName())
	if target == "" {
		return nil, ErrInvalidTarget
	}
	resp, err := WorkflowRequest(sp, parameters, target)
	if err != nil {
		zap.S().Errorw("lsds run request error", "error", err)
		return nil, err
	}
	return resp.Result, nil
}

// Stats returns lsds own stats in the serverlessv1alpha1.WorkflowRuntime
func (l *LSDS) Stats() runner.InstanceStatus {
	wfrt, existed, err := l.getWorkflowRuntimeByName(l.workflowName)
	if err != nil {
		zap.S().Errorw("lsds stats error", "err", err)
		return nil
	}
	if !existed {
		return nil
	}
	zap.S().Debugw("get workflow runtime", "wfrt", wfrt, "selfName", l.selfName)
	selfStatus, existed := wfrt.Spec.Status.Instances[l.selfName]
	if !existed {
		return nil
	}
	result := runner.InstanceStatus{}
	for functionName, stat := range selfStatus.ProcessRuntimes {
		result[functionName] = stat.Number
	}
	return result
}

// FunctionStats returns the current running instances numbers for the given function (param1),
// it iterates the self instances data in WorkflowRuntime, and returns the number
// if the function instance doesn't exist or something error from getting the WorkflowRuntime,
// it returns 0
func (l *LSDS) FunctionStats(functionName string) int {
	wfrt, existed, err := l.getWorkflowRuntimeByName(l.workflowName)
	if err != nil {
		zap.S().Errorw("get lsds stats error", "err", err)
		return 0
	}
	if !existed {
		return 0
	}

	zap.S().Debugw("get workflow runtime", "wfrt", wfrt, "selfName", l.selfName)
	selfStatus, existed := wfrt.Spec.Status.Instances[l.selfName]
	if !existed {
		return 0
	}

	return selfStatus.ProcessRuntimes[functionName].Number
}

var _ runner.Runner = &LSDS{}
