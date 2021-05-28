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
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

var InvalidTargetError error = errors.New("no valid target")

// Policy will return the ip we choose to send request.
type Policy func(functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string

var (
	lsds *LSDS
	once = &sync.Once{}
)

func LDSinit() {
	// todo context thinking
	lsds = NewLSDS(context.Background())
}

func GetLSDSIns() *LSDS {
	once.Do(LDSinit)
	return lsds
}

// LSDS is the short of Local Scheduler Discovery Service, which maintains own information and sync to apiserver
// and get other Local Scheduler info for remote request
type LSDS struct {
	runner.Runner
	ctx          context.Context
	stopCh       chan struct{}
	lock         sync.Locker
	workflowName string
	selfName     string
	policies     map[string]Policy
}

var SimplePolicy Policy = func(functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string {
	var target string
	max := 0
	for i, instance := range runtime.Spec.Status.Instances {
		if i == selfName {
			continue
		}
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

func (l *LSDS) getWorkflowRuntimeByName(name string) (*serverlessv1alpha1.WorkflowRuntime, bool, error) {
	return k8sutils.GetWorkflowRuntimeByName(name)
}

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
	TargetPolicy := viper.GetString(env.Policy)
	ip = l.policies[TargetPolicy](functionName, l.selfName, wfrt)
	return
}

// LDS Start to watch other Local Scheduler Info
// todo deprecated
func (l *LSDS) start() {
}

// send request to other LocalScheduler
func WorkflowRequest(parameters map[string]interface{}, target string, sp span.Span) (dto.InvokeResponse, error) {
	client := &http.Client{}
	invokeRequest := dto.InvokeRequest{
		WorkflowName: sp.WorkflowName,
		FlowName:     sp.FlowName,
		Parameters:   parameters,
	}
	reqByte, err := json.Marshal(invokeRequest)
	if err != nil {
		zap.S().Errorw("workflow request body error", "err", err)
		return dto.InvokeResponse{}, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/v1/workflow", target), strings.NewReader(string(reqByte)))
	if err != nil {
		zap.S().Errorw("workflow request request error", "err", err)
		return dto.InvokeResponse{}, err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		zap.S().Errorw("workflow request response error", "err", err)
		return dto.InvokeResponse{}, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		zap.S().Errorw("workflow request response body error", "err", err)
		return dto.InvokeResponse{}, err
	}
	invokeResp := dto.InvokeResponse{}
	_ = json.Unmarshal(body, &invokeResp)
	return invokeResp, nil
}

// find a suitable pod to send http request with
func (l *LSDS) Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error) {
	target := l.chooseTarget(span.FunctionName)
	if target == "" {
		return nil, InvalidTargetError
	}
	resp, err := WorkflowRequest(parameters, target, span)
	if err != nil {
		zap.S().Errorw("lsds run request error", "error", err)
		return nil, err
	}
	return resp.Result, nil
}

// LSDS Stats will return own stats in the serverlessv1alpha1.WorkflowRuntime
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
