package runner

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	api "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	"io/ioutil"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"net/http"
	"strings"
	"sync"
	"time"
)

var NOT_VALID_TARGET_ERR error = errors.New("no valid target")

type Policy func(functionName string, selfName string, runtime *api.WorkflowRuntime) string

var (
	WorkflowRuntimeResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "WorkflowRuntime",
	}
	TargetPolicy = "simple"
)

type LSDS struct {
	Runner
	informer        informers.GenericInformer
	ctx             context.Context
	stopCh          chan struct{}
	lock            sync.Locker
	workflowName    string
	selfName        string
	policies        map[string]Policy
	processRuntimes api.ProcessRuntimes
}

var SimplePolicy Policy = func(functionName string, selfName string, runtime *api.WorkflowRuntime) string {
	var target string
	max := 0
	for i, instance := range runtime.Status.Instances {
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
		return runtime.Status.Instances[target].Spec.PodIP
	}
	return ""
}

// client is a parameter because we will use mockclient to test
func NewLSDS(ctx context.Context) *LSDS {
	return &LSDS{
		ctx:    ctx,
		stopCh: make(chan struct{}),
		lock:   &sync.Mutex{},
		policies: map[string]Policy{
			"simple": SimplePolicy,
		},
		processRuntimes: api.ProcessRuntimes{},
		workflowName:    k8sutils.GetWorkflowName(),
		selfName:        k8sutils.GetSelfName(),
	}
}

func (l *LSDS) getWorkflowRuntimeByName(name string) (*api.WorkflowRuntime, error) {
	obj, err := l.informer.Lister().Get(name)
	if err != nil {
		return nil, err
	}
	wfrt, ok := obj.(*api.WorkflowRuntime)
	if !ok {
		panic(obj)
	}
	return wfrt, nil
}

func (l *LSDS) chooseTarget(functionName string) (ip string) {
	l.lock.Lock()
	defer l.lock.Unlock()
	wfrt, err := l.getWorkflowRuntimeByName(l.workflowName)
	if err != nil {
		// todo add retry
		zap.S().Errorw("lsds get workflowruntime error", "err", err)
	}
	ip = l.policies[TargetPolicy](functionName, l.selfName, wfrt)
	return
}

func (l *LSDS) Sync(info map[string]int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	processes := api.ProcessRuntimes{}
	for key, num := range info {
		processes[key] = api.ProcessRuntime{
			Number: num,
		}
	}
	l.processRuntimes = processes
	pathByte := l.GeneratePatchWorkflowRuntime(l.processRuntimes)
	// use patch to update
	k8sutils.GetPatchClientIns().Resource(WorkflowRuntimeResource).Patch(
		context.Background(),
		l.workflowName,
		types.StrategicMergePatchType,
		pathByte,
		v1.PatchOptions{})
}

func (l *LSDS) GeneratePatchWorkflowRuntime(processes api.ProcessRuntimes) []byte {
	pwfrt := api.WorkflowRuntime{
		Status: api.WorkflowRuntimeStatus{
			Instances: api.Instances{
				l.workflowName: api.Instance{
					ProcessRuntimes: processes,
				},
			},
		},
	}
	result, _ := json.Marshal(pwfrt)
	return result
}

// LDS Start to watch other Local Scheduler Info
func (l *LSDS) Start() {
	err := l.startListen()
	if err != nil {
		panic(err)
	}
}

func (l *LSDS) startListen() error {
	k8sclient := k8sutils.GetInformerClientIns()
	factory := informers.NewSharedInformerFactoryWithOptions(k8sclient, 1*time.Second, informers.WithTweakListOptions(func(options *v1.ListOptions) {
		options.LabelSelector = labels.Set(
			map[string]string{
				"type": "workflow",
				"name": k8sutils.GetWorkflowName(),
			}).String()
	}), informers.WithNamespace(k8sutils.GetWorkflowName()))
	informer, err := factory.ForResource(WorkflowRuntimeResource)
	if err != nil {
		panic(err)
	}
	l.informer = informer
	go informer.Informer().Run(l.stopCh)
	//watcher, err := cli.Resource(WorkflowRuntimeResource).Watch(l.ctx,
	//	v1.ListOptions{
	//		LabelSelector: labels.Set(
	//			map[string]string{
	//				"type": "workflowRuntime",
	//				"name": l.workflowName,
	//			}).String(),
	//		Watch: true,
	//	})
	//if err != nil {
	//	zap.S().Errorw("LSDS watch WorkflowRuntime error", "err", err)
	//	return err
	//}
	//for e := range watcher.ResultChan() {
	//	switch e.Type {
	//	case watch.Modified, watch.Added:
	//		{
	//			wfrt, ok := e.Object.(*api.WorkflowRuntime)
	//			if !ok {
	//				zap.S().Errorw("lsds watch struct convert err", "Object", e.Object)
	//			}
	//			l.lock.Lock()
	//			l.wfrt = wfrt
	//			l.lock.Unlock()
	//		}
	//	case watch.Deleted:
	//		{
	//			zap.S().Fatal("system error why a WorkflowRuntime deleted")
	//		}
	//	default:
	//		{
	//			zap.S().Infow("strange event at lsds", "event", e)
	//		}
	//	}
	//}
	return nil
}

func WorkflowRequest(parameters map[string]interface{}, target string, sp span.Span) (dto.InvokeResponse, error) {
	client := &http.Client{
	}
	invokeRequest := dto.InvokeRequest{
		WorkflowName: sp.WorkflowName,
		StepName:     sp.StepName,
		Parameters:   parameters,
	}
	reqByte, err := json.Marshal(invokeRequest)
	if err != nil {
		zap.S().Errorw("workflow request body error", "err", err)
		return dto.InvokeResponse{}, err
	}
	req, err := http.NewRequest("POST", target+":8080/workflow", strings.NewReader(string(reqByte)))
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
		return nil, NOT_VALID_TARGET_ERR
	}
	resp, err := WorkflowRequest(parameters, target, span)
	if err != nil {
		zap.S().Errorw("lsds run request error", "error", err)
		return nil, err
	}
	return resp.Result, nil
}
