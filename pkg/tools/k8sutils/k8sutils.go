package k8sutils

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	"io/ioutil"
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
	"os"
	"strings"
	"time"
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
	workflowInformer        cache.SharedInformer
	workflowRuntimeInformer cache.SharedInformer
)
var WithInjectData = func(objects *[]runtime.Object) {

}

func Prepare() {
	if local := viper.GetBool(env.Local); local {
		objects := []runtime.Object{}
		workflowName = viper.GetString(env.WorkflowName)
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
		WithInjectData(&objects)
		zap.S().Infow("get objects", "objects", objects)
		if err := serverlessv1alpha1.AddToScheme(scheme); err != nil {
			panic(err)
		}
		dynamicClient = dynamicfake.NewSimpleDynamicClient(scheme, objects...)
		InitWorkflowRuntimeInformer()
		InitWorkflowInformer()
	} else {
		// use real k8s
		hostName, _ := os.Hostname()
		sli := strings.Split(hostName, "-")
		if len(sli) < 3 {
			panic(sli)
		}
		selfName = strings.Join(sli[:2], "-")
		workflowName = strings.Join(sli[2:], "-")

		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err)
		}
		dynamicClient = dynamic.NewForConfigOrDie(config)
		InitWorkflowRuntimeInformer()
		InitWorkflowInformer()
	}
}

var GetSelfName = func() string {
	return selfName
}

var GetWorkflowName = func() string {
	return workflowName
}

var GetPatchClientIns = func() dynamic.Interface {
	return dynamicClient
}

// todo get namespace from file
var GetSelfNamespace = func() string {
	return "default"
}

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
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), workflow)
		if err != nil {
			return err
		}
		zap.S().Debugw("get Workflow", "workflow", workflow)
		*objects = append(*objects, workflow)
	}
	return err
}

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

func CreateUnstructuredListWatch(ctx context.Context, namespace string, resource schema.GroupVersionResource) *cache.ListWatch {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {

			result, err := dynamicClient.Resource(resource).Namespace(namespace).List(ctx, opts)
			zap.S().Debugw("get result at list and watch", "result", result)
			return result, err
		},
		// Setup the watch function
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			// Watch needs to be set to true separately
			opts.Watch = true
			return dynamicClient.Resource(resource).Namespace(namespace).Watch(ctx, opts)
		},
	}
}

func InitWorkflowRuntimeInformer() {
	listAndWatch := CreateUnstructuredListWatch(context.Background(), GetSelfNamespace(), WorkflowRuntimeResources)
	workflowRuntimeInformer = cache.NewSharedInformer(
		listAndWatch,
		&serverlessv1alpha1.WorkflowRuntime{},
		1*time.Second,
	)

	zap.S().Debug("in the lsds start listen")
	go workflowRuntimeInformer.Run(make(<-chan struct{}))
}

func InitWorkflowInformer() {
	listAndWatch := CreateUnstructuredListWatch(context.Background(), GetSelfNamespace(), WorkflowResource)
	informer := cache.NewSharedInformer(
		listAndWatch,
		&serverlessv1alpha1.Workflow{},
		1*time.Second,
	)
	workflowInformer = informer
	go workflowInformer.Run(make(<-chan struct{}))
}

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
	ust := obj.(*unstructured.Unstructured)
	wf := &serverlessv1alpha1.Workflow{}
	_ = runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wf)
	return wf, true, nil
}

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
	switch obj.(type) {
	case *serverlessv1alpha1.WorkflowRuntime:
		{
			wfrt = obj.(*serverlessv1alpha1.WorkflowRuntime)
		}
	case *unstructured.Unstructured:
		{
			ust := obj.(*unstructured.Unstructured)
			wfrt = &serverlessv1alpha1.WorkflowRuntime{}
			runtime.DefaultUnstructuredConverter.FromUnstructured(ust.UnstructuredContent(), wfrt)
		}
	}

	return wfrt, true, nil
}

func patchRuntime(workflowName string, pathBytes []byte) error {
	_, err := dynamicClient.Resource(WorkflowRuntimeResources).Namespace(GetSelfNamespace()).Patch(
		context.Background(),
		workflowName,
		types.MergePatchType,
		pathBytes,
		metav1.PatchOptions{})
	if err != nil {
		zap.S().Errorw("k8s WorkflowRuntime patch error", "WorkflowRuntime", workflowName, "err", err)
	}
	return err
}

// Help Function for Sync patch
func generatePatchWorkflowRuntime(runtimes serverlessv1alpha1.ProcessRuntimes) []byte {
	pwfrt := serverlessv1alpha1.WorkflowRuntime{
		Status: serverlessv1alpha1.WorkflowRuntimeStatus{
			Instances: serverlessv1alpha1.Instances{
				selfName: serverlessv1alpha1.Instance{
					ProcessRuntimes: runtimes,
				},
			},
		},
	}
	result, _ := json.Marshal(pwfrt)
	return result
}

// Patch the WorkflowRuntime to apiserver
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
