package k8sutils

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"

	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/types"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	selfName         string
	workflowName     string
	dynamicClient    dynamic.Interface
	scheme           = runtime.NewScheme()
	WorkflowResource = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "workflows",
	}
	WorkflowRuntimeResources = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "workflowruntimes",
	}
	FunctionResources = schema.GroupVersionResource{
		Group:    "serverless.tass.io",
		Version:  "v1alpha1",
		Resource: "functions",
	}
	workflowInformer        cache.SharedInformer
	workflowRuntimeInformer cache.SharedInformer
	functionInformer        cache.SharedInformer
)
var WithInjectData = func(objects *[]runtime.Object) {

}

// NewStringPtr returns a string pointer for the given string
func NewStringPtr(val string) *string {
	ptr := new(string)
	*ptr = val
	return ptr
}

// nolint: unused
func wrapObjects(objs []runtime.Object) []runtime.Object {
	result := make([]runtime.Object, 0, len(objs))
	for _, obj := range objs {
		if ust, ok := obj.(*unstructured.Unstructured); ok {
			result = append(result, ust)
		} else {
			ustdata, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				zap.S().Panic(err)
			}
			ust := &unstructured.Unstructured{
				Object: ustdata,
			}
			result = append(result, ust)
		}
	}
	return result
}

// Prepare prepares environment of k8s client
// If it's local, it thes local Workflow and Workflowruntime files and use a fake Client
// If not local, use real k8s client.
func Prepare() {
	if local := viper.GetBool(env.Local); local {
		objects := []runtime.Object{}
		workflowRuntimeFilePath := viper.GetString(env.WorkflowRuntimeFilePath)
		selfName = viper.GetString(env.SelfName)
		err := generateWorkflowRuntimeObjectsByFile(workflowRuntimeFilePath, &objects)
		if err != nil {
			zap.S().Warnw("generate WorkflowRuntime error", "err", err)
		}
		workflowFilePath := viper.GetString(env.WorkflowPath)
		err = generateWorkflowObjectsByFile(workflowFilePath, &objects)
		if err != nil {
			zap.S().Warnw("generate Workflow error", "err", err)
		}
		functionPaths := viper.GetStringSlice(env.FuntionsPath)
		for _, functionPath := range functionPaths {
			zap.S().Infow("get functions path", "path", functionPath)
			err = generateFunctionObjectsByFile(functionPath, &objects)
			if err != nil {
				zap.S().Warnw("generate FunctionList error", "err", err)
			}
		}
		if workflowName == "" {
			workflowName = viper.GetString(env.WorkflowName)
		}
		zap.S().Infow("read object from file over")
		WithInjectData(&objects)
		zap.S().Infow("get objects", "objects", objects)
		if err := serverlessv1alpha1.AddToScheme(scheme); err != nil {
			zap.S().Panic(err)
		}
		// objects = wrapObjects(objects)
		for _, obj := range objects {
			zap.S().Debugw("obj type", "type", reflect.TypeOf(obj))
		}
		dynamicClient = dynamicfake.NewSimpleDynamicClient(scheme, objects...)
	} else {
		// use real k8s
		hostName, _ := os.Hostname()
		sli := strings.Split(hostName, "-")
		if len(sli) < 3 {
			zap.S().Panic(sli)
		}
		workflowName = strings.Join(sli[:len(sli)-2], "-")
		selfName = strings.Join(sli[len(sli)-2:], "-")

		config, err := rest.InClusterConfig()
		if err != nil {
			zap.S().Panic(err)
		}
		dynamicClient = dynamic.NewForConfigOrDie(config)

	}
	InitWorkflowRuntimeInformer()
	InitWorkflowInformer()
	InitFunctionInformer()
}

// GetSelfName returns the suffix of the Pod
// The Pod name exists in the hostname of the container
// If the pod name is workflow-sample-76f8774575-rhqw4, the selfName is 76f8774575-rhqw4.
var GetSelfName = func() string {
	return selfName
}

// GetWorkflowName returns the workflow name
// The name comes from the prefix of the Pod
// If the pod name is workflow-sample-76f8774575-rhqw4, the workflowName is workflow-sample.
var GetWorkflowName = func() string {
	return workflowName
}

// GetPatchClientIns returns the dynamicClient instance
var GetPatchClientIns = func() dynamic.Interface {
	return dynamicClient
}

// TODO: get namespace from file
// GetSelfNamespace returns the namespace of the Pod
var GetSelfNamespace = func() string {
	return "default"
}

// generateWorkflowObjectsByFile generates a Workflow object by file
// this method is used when using the local environment, a parser for local files are needed
func generateWorkflowObjectsByFile(fileName string, objects *[]runtime.Object) error {
	filebytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(filebytes), 100)
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			break
		}

		obj, _, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
		if err != nil {
			return err
		}
		ust := obj.(*unstructured.Unstructured)
		workflow := new(serverlessv1alpha1.Workflow)
		// transfer Unstructured to a typed object
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), workflow)
		if err != nil {
			return err
		}
		zap.S().Debugw("get Workflow", "workflow", workflow)
		workflowName = workflow.Name
		*objects = append(*objects, workflow)
	}
	return err
}

// generateWorkflowRuntimeObjectsByFile generates a WorkflowRuntime object by file
// this method is used when using the local environment, a parser for local files are needed
func generateWorkflowRuntimeObjectsByFile(fileName string, objects *[]runtime.Object) error {
	filebytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(filebytes), 100)
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			break
		}

		obj, _, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
		if err != nil {
			return err
		}
		ust := obj.(*unstructured.Unstructured)
		wfrt := new(serverlessv1alpha1.WorkflowRuntime)
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wfrt)
		if err != nil {
			return err
		}
		zap.S().Debugw("get WorkflowRuntime", "workflow", wfrt)
		*objects = append(*objects, wfrt)
	}
	return err
}

// generateFunctionListObjectsByFile generates a FunctionList object by file
// this method is used when using the local environment, a parser for local files are needed
func generateFunctionObjectsByFile(fileName string, objects *[]runtime.Object) error {
	filebytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(filebytes), len(filebytes))
	for {
		var rawObj runtime.RawExtension
		if err = decoder.Decode(&rawObj); err != nil {
			zap.S().Errorw("runtime RawExtension Error", "err", err)
			break
		}
		obj, _, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
		if err != nil {
			return err
		}
		ust := obj.(*unstructured.Unstructured)
		function := new(serverlessv1alpha1.Function)
		// transfer Unstructured to a typed object
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), function)
		if err != nil {
			return err
		}
		zap.S().Debugw("get FunctionList", "function", function)
		*objects = append(*objects, function)
	}
	return err
}

func CreateUnstructuredListWatch(ctx context.Context, ns string, resrc schema.GroupVersionResource) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			result, err := dynamicClient.Resource(resrc).Namespace(ns).List(ctx, opts)
			return result, err
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			// Watch needs to be set to true separately
			opts.Watch = true
			return dynamicClient.Resource(resrc).Namespace(ns).Watch(ctx, opts)
		},
	}
}

func InitWorkflowRuntimeInformer() {
	// i := factory.ForResource(WorkflowResource)
	listAndWatch := CreateUnstructuredListWatch(context.Background(), GetSelfNamespace(), WorkflowRuntimeResources)
	workflowRuntimeInformer = cache.NewSharedInformer(
		listAndWatch,
		// &serverlessv1alpha1.WorkflowRuntime{},
		&unstructured.Unstructured{},
		1*time.Second,
	)
	// zap.S().Debug("in the lsds start listen")
	go workflowRuntimeInformer.Run(make(<-chan struct{}))
}

func InitWorkflowInformer() {
	// i := factory.ForResource(WorkflowResource)
	listAndWatch := CreateUnstructuredListWatch(context.Background(), GetSelfNamespace(), WorkflowResource)
	informer := cache.NewSharedInformer(
		listAndWatch,
		// &serverlessv1alpha1.Workflow{},
		&unstructured.Unstructured{},
		1*time.Second,
	)
	workflowInformer = informer
	go workflowInformer.Run(make(<-chan struct{}))
}

func InitFunctionInformer() {
	// i := factory.ForResource(FunctionResources)
	listAndWatch := CreateUnstructuredListWatch(context.Background(), GetSelfNamespace(), FunctionResources)
	informer := cache.NewSharedInformer(
		listAndWatch,
		// &serverlessv1alpha1.Function{},
		&unstructured.Unstructured{},
		1*time.Second,
	)
	functionInformer = informer
	go functionInformer.Run(make(<-chan struct{}))
}

// GetWorkflowByName returns a workflow instance by the input name
func GetWorkflowByName(name string) (*serverlessv1alpha1.Workflow, bool, error) {
	zap.S().Debugw("get workflow name", "name", name, "keys", workflowInformer.GetStore().ListKeys())
	key := GetSelfNamespace() + "/" + name
	obj, existed, err := workflowInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	var wf *serverlessv1alpha1.Workflow
	switch obj := obj.(type) {
	case *serverlessv1alpha1.Workflow:
		wf = obj
	case *unstructured.Unstructured:
		ust := obj
		wf = &serverlessv1alpha1.Workflow{}
		_ = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wf)
	}
	return wf, true, nil
}

// GetWorkflowRuntimeByName returns a WorkflowRuntime instance by the input name
func GetWorkflowRuntimeByName(name string) (*serverlessv1alpha1.WorkflowRuntime, bool, error) {
	zap.S().Debugw("get workflowruntime name", "name", name, "keys", workflowRuntimeInformer.GetStore().ListKeys())
	key := GetSelfNamespace() + "/" + name
	obj, existed, err := workflowRuntimeInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	var wfrt *serverlessv1alpha1.WorkflowRuntime
	switch obj := obj.(type) {
	case *serverlessv1alpha1.WorkflowRuntime:
		wfrt = obj
	case *unstructured.Unstructured:
		ust := obj
		wfrt = &serverlessv1alpha1.WorkflowRuntime{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wfrt)
		if err != nil {
			return nil, false, err
		}
	}

	return wfrt, true, nil
}

// GetFunctionByName returns a Function instance by the input name
func GetFunctionByName(name string) (*serverlessv1alpha1.Function, bool, error) {
	zap.S().Debugw("get function name", "name", name, "keys", functionInformer.GetStore().ListKeys())
	key := GetSelfNamespace() + "/" + name
	obj, existed, err := functionInformer.GetStore().GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !existed {
		return nil, false, nil
	}
	var function *serverlessv1alpha1.Function
	switch obj := obj.(type) {
	case *serverlessv1alpha1.Function:
		function = obj
	case *unstructured.Unstructured:
		ust := obj
		function = &serverlessv1alpha1.Function{}
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), function)
		if err != nil {
			return nil, false, err
		}
	}

	return function, true, nil
}

// patchRuntime sends the workflow patch payloads to ApiServer
func patchRuntime(workflowName string, patchBytes []byte) error {
	zap.S().Debugw("patch Runtime", "patch", patchBytes)
	result, err := dynamicClient.Resource(WorkflowRuntimeResources).Namespace(GetSelfNamespace()).Patch(
		context.Background(),
		workflowName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{})
	if err != nil {
		zap.S().Errorw("k8s WorkflowRuntime patch error", "WorkflowRuntime", workflowName, "err", err)
	}
	zap.S().Debugw("patch get result", "result", result)
	return err
}

// generatePatchWorkflowRuntime is a helper function for Sync patch
// it generates a wfrt template bytes
func generatePatchWorkflowRuntime(runtimes serverlessv1alpha1.ProcessRuntimes) []byte {
	pwfrt := serverlessv1alpha1.WorkflowRuntime{
		Spec: &serverlessv1alpha1.WorkflowRuntimeSpec{
			Status: serverlessv1alpha1.WfrtStatus{
				Instances: serverlessv1alpha1.Instances{
					selfName: serverlessv1alpha1.Instance{
						ProcessRuntimes: runtimes,
					},
				},
			},
		},
	}
	result, _ := json.Marshal(pwfrt)
	return result
}

// Sync patch the WorkflowRuntime to apiserver
func Sync(info map[string]int) {
	processes := serverlessv1alpha1.ProcessRuntimes{}
	for key, num := range info {
		processes[key] = serverlessv1alpha1.ProcessRuntime{
			Number: num,
		}
	}
	patchBytes := generatePatchWorkflowRuntime(processes)
	_ = patchRuntime(workflowName, patchBytes)
}
