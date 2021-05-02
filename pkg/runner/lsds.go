package runner

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

var InvalidTargetError error = errors.New("no valid target")

// Policy will return the ip we choose to send request.
type Policy func(functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string

var (
	lsds                    *LSDS
	WorkflowRuntimeResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "workflowruntimes",
	}
	TargetPolicy = viper.GetString("policy")
)

func LDSinit() {
	// todo context thinking
	lsds = NewLSDS(context.Background())
}

func GetLSDSIns() *LSDS {
	return lsds
}

// LSDS is the short of Local Scheduler Discovery Service, which maintains own information and sync to apiserver
// and get other Local Scheduler info for remote request
type LSDS struct {
	Runner
	informer        cache.SharedInformer
	ctx             context.Context
	stopCh          chan struct{}
	lock            sync.Locker
	workflowName    string
	selfName        string
	policies        map[string]Policy
	processRuntimes serverlessv1alpha1.ProcessRuntimes
}

var SimplePolicy Policy = func(functionName string, selfName string, runtime *serverlessv1alpha1.WorkflowRuntime) string {
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
		return runtime.Status.Instances[target].Status.PodIP
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
		processRuntimes: serverlessv1alpha1.ProcessRuntimes{},
		workflowName:    k8sutils.GetWorkflowName(),
		selfName:        k8sutils.GetSelfName(),
	}
	lsds.start()
	return lsds
}

func (l *LSDS) getWorkflowRuntimeByName(name string) (*serverlessv1alpha1.WorkflowRuntime, bool, error) {

	key := k8sutils.GetSelfName() + "/" + name
	obj, existed, err := l.informer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	ust := obj.(*unstructured.Unstructured)
	wfrt := &serverlessv1alpha1.WorkflowRuntime{}
	runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wfrt)
	return wfrt, true, nil
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
	ip = l.policies[TargetPolicy](functionName, l.selfName, wfrt)
	return
}

// Sync Will be call to update ProcessRuntime and patch to apiserver
func (l *LSDS) Sync(info map[string]int) {
	zap.S().Debug("in the sync")
	l.lock.Lock()
	defer l.lock.Unlock()
	processes := serverlessv1alpha1.ProcessRuntimes{}
	for key, num := range info {
		processes[key] = serverlessv1alpha1.ProcessRuntime{
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

// Help Function for Sync patch
func (l *LSDS) GeneratePatchWorkflowRuntime(processes serverlessv1alpha1.ProcessRuntimes) []byte {
	pwfrt := serverlessv1alpha1.WorkflowRuntime{
		Status: serverlessv1alpha1.WorkflowRuntimeStatus{
			Instances: serverlessv1alpha1.Instances{
				l.workflowName: serverlessv1alpha1.Instance{
					ProcessRuntimes: processes,
				},
			},
		},
	}
	result, _ := json.Marshal(pwfrt)
	return result
}

// LDS Start to watch other Local Scheduler Info
func (l *LSDS) start() {
	err := l.startListen()
	if err != nil {
		panic(err)
	}
}

func (l *LSDS) startListen() error {
	// factory := informers.NewSharedInformerFactoryWithOptions(k8sclient, 1*time.Second, informers.WithTweakListOptions(func(options *v1.ListOptions) {
	// 	options.LabelSelector = labels.Set(
	// 		map[string]string{
	// 			"type": "workflow",
	// 			"name": k8sutils.GetWorkflowName(),
	// 		}).String()
	// }), informers.WithNamespace(k8sutils.GetWorkflowName()))
	listAndWatch := k8sutils.CreateUnstructuredListWatch(l.ctx, k8sutils.GetSelfNamespace(), WorkflowRuntimeResource)
	informer := cache.NewSharedInformer(
		listAndWatch,
		&serverlessv1alpha1.WorkflowRuntime{},
		1*time.Second,
	)

	l.informer = informer
	zap.S().Debug("in the lsds start listen")
	go informer.Run(make(<-chan struct{}))
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

// send request to other LocalScheduler
func WorkflowRequest(parameters map[string]interface{}, target string, sp span.Span) (dto.InvokeResponse, error) {
	client := &http.Client{}
	invokeRequest := dto.InvokeRequest{
		WorkflowName: sp.WorkflowName,
		FunctionName: sp.FunctionName,
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
		return nil, InvalidTargetError
	}
	resp, err := WorkflowRequest(parameters, target, span)
	if err != nil {
		zap.S().Errorw("lsds run request error", "error", err)
		return nil, err
	}
	return resp.Result, nil
}
