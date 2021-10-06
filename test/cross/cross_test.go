package cross

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/utils/k8sutils"
	"github.com/tass-io/scheduler/test"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
)

const (
	APIVersion          = "serverless.tass.io/v1alpha1"
	WorkflowKind        = "Workflow"
	WorkflowRuntimeKind = "WorkflowRuntime"
)

func GetSampleWorkflowRuntime() *serverlessv1alpha1.WorkflowRuntime {
	replicas := new(int32)
	*replicas = 2
	return &serverlessv1alpha1.WorkflowRuntime{
		TypeMeta: metav1.TypeMeta{
			Kind:       WorkflowRuntimeKind,
			APIVersion: APIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "simple",
			Namespace: "default",
		},
		Spec: &serverlessv1alpha1.WorkflowRuntimeSpec{
			Replicas: replicas,
			Status: serverlessv1alpha1.WfrtStatus{
				Instances: serverlessv1alpha1.Instances{
					"caller": {
						Status: &serverlessv1alpha1.InstanceStatus{
							HostIP: k8sutils.NewStringPtr("127.0.0.1"),
							PodIP:  k8sutils.NewStringPtr("127.0.0.1:8080"),
						},
						ProcessRuntimes: serverlessv1alpha1.ProcessRuntimes{
							"simple_start": {
								Number: 1,
							},
							"simple_mid": {
								Number: 1,
							},
						},
					},
					"callee": {
						Status: &serverlessv1alpha1.InstanceStatus{
							HostIP: k8sutils.NewStringPtr("127.0.0.1"),
							PodIP:  k8sutils.NewStringPtr("127.0.0.1:9090"),
						},
						ProcessRuntimes: serverlessv1alpha1.ProcessRuntimes{
							"simple_branch_1": {
								Number: 1,
							},
							"simple_branch_2": {
								Number: 1,
							},
						},
					},
				},
			},
		},
	}
}

func GetSampleWorkflow() *serverlessv1alpha1.Workflow {
	return &serverlessv1alpha1.Workflow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: APIVersion,
			Kind:       WorkflowKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "simple",
			Namespace: "default",
		},
		Spec: serverlessv1alpha1.WorkflowSpec{
			Spec: []serverlessv1alpha1.Flow{
				{
					Name:       "simple_start",
					Function:   "simple_start",
					Outputs:    []string{"simple_mid"},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       serverlessv1alpha1.Start,
				},
				{
					Name:       "simple_mid",
					Function:   "simple_mid",
					Outputs:    []string{"simple_branch_1", "simple_branch_2"},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       "",
				},
				{
					Name:       "simple_branch_1",
					Function:   "simple_branch_1",
					Outputs:    []string{},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       serverlessv1alpha1.End,
				},
				{
					Name:       "simple_branch_2",
					Function:   "simple_branch_2",
					Outputs:    []string{},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       serverlessv1alpha1.End,
				},
			},
		},
		Status: serverlessv1alpha1.WorkflowStatus{},
	}
}

func GetIfElseWorkflowRuntime() *serverlessv1alpha1.WorkflowRuntime {
	replicas := new(int32)
	*replicas = 2
	return &serverlessv1alpha1.WorkflowRuntime{
		TypeMeta: metav1.TypeMeta{
			Kind:       WorkflowRuntimeKind,
			APIVersion: APIVersion,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "if-else",
			Namespace: "default",
		},
		Spec: &serverlessv1alpha1.WorkflowRuntimeSpec{
			Replicas: replicas,
			Status: serverlessv1alpha1.WfrtStatus{
				Instances: serverlessv1alpha1.Instances{
					"caller": {
						Status: &serverlessv1alpha1.InstanceStatus{
							HostIP: k8sutils.NewStringPtr("127.0.0.1"),
							PodIP:  k8sutils.NewStringPtr("127.0.0.1:8080"),
						},
						ProcessRuntimes: serverlessv1alpha1.ProcessRuntimes{
							"start": {
								Number: 1,
							},
						},
					},
					"callee": {
						Status: &serverlessv1alpha1.InstanceStatus{
							HostIP: k8sutils.NewStringPtr("127.0.0.1"),
							PodIP:  k8sutils.NewStringPtr("127.0.0.1:9090"),
						},
						ProcessRuntimes: serverlessv1alpha1.ProcessRuntimes{
							"left": {
								Number: 1,
							},
							"right": {
								Number: 1,
							},
						},
					},
				},
			},
		},
	}
}

func GetIfElseWorkflow() *serverlessv1alpha1.Workflow {
	return &serverlessv1alpha1.Workflow{
		TypeMeta: metav1.TypeMeta{
			APIVersion: APIVersion,
			Kind:       WorkflowKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "if-else",
			Namespace: "default",
		},
		Spec: serverlessv1alpha1.WorkflowSpec{
			Spec: []serverlessv1alpha1.Flow{
				{
					Name:     "start",
					Function: "start",
					Outputs:  []string{},
					Conditions: []*serverlessv1alpha1.Condition{
						{
							Name:        "root",
							Type:        serverlessv1alpha1.Int,
							Operator:    serverlessv1alpha1.Eq,
							Target:      "$.a",
							Comparision: "5",
							Destination: serverlessv1alpha1.Destination{
								IsTrue: serverlessv1alpha1.Next{
									Flows: []string{"left"},
								},
								IsFalse: serverlessv1alpha1.Next{
									Flows: []string{"right"},
								},
							},
						},
					},
					Statement: serverlessv1alpha1.Switch,
					Role:      serverlessv1alpha1.Start,
				},
				{
					Name:       "left",
					Function:   "left",
					Outputs:    []string{},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       serverlessv1alpha1.End,
				},
				{
					Name:       "right",
					Function:   "right",
					Outputs:    []string{},
					Conditions: []*serverlessv1alpha1.Condition{},
					Statement:  serverlessv1alpha1.Direct,
					Role:       serverlessv1alpha1.End,
				},
			},
		},
		Status: serverlessv1alpha1.WorkflowStatus{},
	}
}

func DumpConfig(object runtime.Object, folderName, fileName string) error {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.AddToScheme(scheme)
	serializer := json.NewSerializerWithOptions(yaml.DefaultMetaFactory, scheme, scheme,
		json.SerializerOptions{Yaml: true, Pretty: false, Strict: false})
	err := os.MkdirAll(folderName, 0666)
	So(err, ShouldBeNil)
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

func TestCross(t *testing.T) {
	Convey("test across local scheduler test", t, func() {
		// build go binary
		complieCmd := exec.Command("go", "build", "../../main.go")
		err := complieCmd.Start()
		So(err, ShouldBeNil)
		err = complieCmd.Wait()
		So(err, ShouldBeNil)
		testcases := []struct {
			caseName        string
			skipped         bool
			workflow        *serverlessv1alpha1.Workflow
			workflowRuntime *serverlessv1alpha1.WorkflowRuntime
			request         dto.WorkflowRequest
			expect          map[string]interface{}
		}{
			{
				caseName:        "test simple parallel",
				skipped:         false,
				workflow:        GetSampleWorkflow(),
				workflowRuntime: GetSampleWorkflowRuntime(),
				request: dto.WorkflowRequest{
					WorkflowName: "simple",
					FlowName:     "",
					Parameters: map[string]interface{}{
						"simple": "simple",
					},
				},
				expect: map[string]interface{}{
					"simple_mid": map[string]interface{}{
						"simple_branch_1": map[string]interface{}{
							"simple":          "simple",
							"simple_branch_1": "simple_branch_1",
							"simple_mid":      "simple_mid",
							"simple_start":    "simple_start",
						},
						"simple_branch_2": map[string]interface{}{
							"simple":          "simple",
							"simple_branch_2": "simple_branch_2",
							"simple_mid":      "simple_mid",
							"simple_start":    "simple_start",
						},
					},
				},
			},
			{
				caseName:        "test simple branch for tracing",
				skipped:         false,
				workflow:        GetIfElseWorkflow(),
				workflowRuntime: GetIfElseWorkflowRuntime(),
				request: dto.WorkflowRequest{
					WorkflowName: "if-else",
					FlowName:     "",
					Parameters: map[string]interface{}{
						"a": 5,
					},
				},
				expect: map[string]interface{}{
					"flows": map[string]interface{}{
						"left": map[string]interface{}{
							"a":     5,
							"left":  "left",
							"start": "start",
						},
					},
				},
			},
		}

		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}
			t.Log(testcase.caseName)
			Convey(testcase.caseName, func() {
				// exec two commands to start two local schedulers
				err = DumpConfig(testcase.workflow, "./config/", "workflow.yaml")
				So(err, ShouldBeNil)
				err = DumpConfig(testcase.workflowRuntime, "./config/", "workflowruntime.yaml")
				So(err, ShouldBeNil)
				// exec two commands to start two local schedulers
				// -l means use local files to init k8s status (make -w ,-r, -s work)
				// -i means use static middleware to send request directly when no instances
				// -m means use mock instance
				// -a means set port
				// -s means set selfName
				callerParam := "-l -i -m -a 8080 -s caller -w ./config/workflow.yaml -r ./config/workflowruntime.yaml"
				calleeParam := "-l -i -m -a 9090 -s callee -w ./config/workflow.yaml -r ./config/workflowruntime.yaml"
				caller := exec.Command("./main", strings.Split(callerParam, " ")...)
				stdOutDump(caller, "./caller.log")
				// nolint
				defer caller.Process.Kill()
				callee := exec.Command("./main", strings.Split(calleeParam, " ")...)
				stdOutDump(callee, "./callee.log")
				// nolint
				defer callee.Process.Kill()
				time.Sleep(1 * time.Second)
				// request the first one
				request := testcase.request
				resp := &dto.WorkflowResponse{}
				status, err := test.RequestJson(
					"http://localhost:8080/v1/workflow/", "POST", map[string]string{}, request, resp)
				t.Log(err)
				So(status, ShouldEqual, 200)
				So(fmt.Sprintf("%v", resp.Result), ShouldResemble, fmt.Sprintf("%v", testcase.expect))
				t.Log(resp)
				// for jaeger flush
				time.Sleep(1 * time.Second)
			})
		}
	})
}
