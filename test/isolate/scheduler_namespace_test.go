package pipe

import (
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	"github.com/tass-io/scheduler/pkg/utils/base64"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/utils/log"
	"github.com/tass-io/scheduler/test"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
)

const (
	APIVersion              = "serverless.tass.io/v1alpha1"
	WorkflowRuntimeKindName = "WorkflowRuntime"
	FunctionListKindName    = "FunctionList"
	FunctionKindName        = "Function"
)

func DumpConfig(object runtime.Object, folderName, fileName string) error {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.AddToScheme(scheme)
	serializer := json.NewSerializerWithOptions(
		yaml.DefaultMetaFactory, scheme, scheme, json.SerializerOptions{Yaml: true, Pretty: false, Strict: false})
	err := os.MkdirAll(folderName, 0666)
	if err != nil {
		return err
	}
	dumpFile, err := os.OpenFile(folderName+fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0)
	if err != nil {
		return err
	}

	err = serializer.Encode(object, dumpFile)
	if err != nil {
		return err
	}
	return nil
}

func DumpFunctions(objects []*serverlessv1alpha1.Function, folderName string) error {
	for _, obj := range objects {
		err := DumpConfig(obj, folderName, obj.Name+".yaml")
		if err != nil {
			return err
		}
	}
	return nil
}

func stdOutDump(cmd *exec.Cmd, fileName string) {
	// open the out file for writing
	outfile, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	cmd.Stdout = outfile

	err = cmd.Start()
	if err != nil {
		panic(err)
	}
}

func TestSchedulerIsolate(t *testing.T) {
	Convey("test Scheduler Namespace", t, func() {
		testcases := []struct {
			caseName        string
			skipped         bool
			args            []string
			workflowName    string
			functionNames   []string
			fileName        string
			request         dto.WorkflowRequest
			expect          dto.WorkflowResponse
			workflowRuntime *serverlessv1alpha1.WorkflowRuntime
			functions       []*serverlessv1alpha1.Function
		}{
			{
				caseName: "direct test for function instance sacle down by ttl",
				skipped:  false,
				args: []string{
					"-l", "-T", "2s", "-w", "../sample/samples/pipeline/direct.yaml",
					"-r", "./config/workflowruntime.yaml",
					"-f", "./config/function1.yaml,./config/function2.yaml,./config/function3.yaml",
				},
				fileName:      "../../user-code/default-golang-wrapper.zip",
				functionNames: []string{"function1", "function2", "function3"},
				workflowRuntime: &serverlessv1alpha1.WorkflowRuntime{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowRuntimeKindName,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "direct",
						Namespace: "default",
					},
					Spec: &serverlessv1alpha1.WorkflowRuntimeSpec{
						Status: serverlessv1alpha1.WfrtStatus{
							Instances: map[string]serverlessv1alpha1.Instance{
								"ubuntu": {
									Status: &serverlessv1alpha1.InstanceStatus{
										HostIP: k8sutils.NewStringPtr("ubuntu"),
										PodIP:  k8sutils.NewStringPtr("ubuntu"),
									},
									ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{},
								},
							},
						},
					},
				},
				functions: []*serverlessv1alpha1.Function{
					{
						TypeMeta: metav1.TypeMeta{
							APIVersion: APIVersion,
							Kind:       FunctionKindName,
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "function1",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.FunctionSpec{
							Resource: serverlessv1alpha1.Resource{
								ResourceCPU:    "1",
								ResourceMemory: "100mi",
							},
							Environment: "Golang",
						},
					},
					{
						TypeMeta: metav1.TypeMeta{
							APIVersion: APIVersion,
							Kind:       FunctionKindName,
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "function2",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.FunctionSpec{
							Resource: serverlessv1alpha1.Resource{
								ResourceCPU:    "1",
								ResourceMemory: "100mi",
							},
							Environment: "Golang",
						},
					},
					{
						TypeMeta: metav1.TypeMeta{
							APIVersion: APIVersion,
							Kind:       FunctionKindName,
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "function3",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.FunctionSpec{
							Resource: serverlessv1alpha1.Resource{
								ResourceCPU:    "1",
								ResourceMemory: "100mi",
							},
							Environment: "Golang",
						},
					},
				},
				request: dto.WorkflowRequest{
					WorkflowName: "direct",
					FlowName:     "",
					Parameters: map[string]interface{}{
						"a": "b",
					},
				},
				expect: dto.WorkflowResponse{
					Success: true,
					Message: "ok",
					Result: map[string]interface{}{
						"next": map[string]interface{}{
							"end": map[string]interface{}{
								"a":     "b",
								"motto": "Veni Vidi Vici",
							},
						},
					},
				},
			},
		}

		complieCmd := exec.Command("go", "build", "../../main.go")
		err := complieCmd.Start()
		So(err, ShouldBeNil)
		err = complieCmd.Wait()
		So(err, ShouldBeNil)
		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}

			err = DumpConfig(testcase.workflowRuntime, "./config/", "workflowruntime.yaml")
			So(err, ShouldBeNil)

			err = DumpFunctions(testcase.functions, "./config/")
			So(err, ShouldBeNil)

			Convey("use http request test scheduler", func() {

				main := exec.Command("./main", testcase.args...)
				stdOutDump(main, "./caller.log")
				// nolint: errcheck
				defer main.Process.Kill()
				code, err := base64.EncodeUserCode(testcase.fileName)
				So(err, ShouldBeNil)
				viper.Set(env.RedisIP, "10.0.2.79")
				viper.Set(env.RedisPort, "6379")
				viper.Set(env.RedisPassword, "")
				viper.Set(env.DefaultDb, 0)
				for _, name := range testcase.functionNames {
					err = store.Set("default", name, code)
					So(err, ShouldBeNil)
				}
				resp := &dto.WorkflowResponse{}
				time.Sleep(6 * time.Second)
				status, err := test.RequestJson("http://localhost:8080/v1/workflow/", "POST",
					map[string]string{}, testcase.request, resp)
				So(err, ShouldBeNil)
				So(status, ShouldEqual, 200)
				So(*resp, ShouldResemble, testcase.expect)
				time.Sleep(5 * time.Second)
			})
		}
	})
}
