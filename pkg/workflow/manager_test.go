package workflow

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	WorkflowAPIVersion = "serverless.tass.io/v1alpha1"
	WorkflowKind       = "Workflow"
)

type SimpleFakeRunner struct {
}

// SimpleFakeRunner will 'register' all function will be call, which indexes the function by span.FunctionName
func (r *SimpleFakeRunner) Run(parameters map[string]interface{}, sp span.Span) (result map[string]interface{}, err error) {
	switch sp.FunctionName {
	case "simple_start":
		{
			return map[string]interface{}{"b": "c"}, nil
		}
	case "simple_mid":
		{
			return map[string]interface{}{"c": "d"}, nil
		}
	case "simple_branch_1":
		{
			return map[string]interface{}{"branch_1": "branch_1"}, nil
		}
	case "simple_branch_2":
		{
			return map[string]interface{}{"branch_2": "branch_2"}, nil
		}
	case "simple_end":
		{
			return map[string]interface{}{"d": "e"}, nil
		}
	default:
		{
			return parameters, nil
		}
	}
}

func TestManager(t *testing.T) {
	Convey("test workflow work with mock client", t, func() {
		testcases := []struct {
			newRunner      func() runner.Runner
			withInjectData func(objects *[]runtime.Object)
			caseName       string
			sp             span.Span
			parameters     map[string]interface{}
			expect         map[string]interface{}
		}{
			{
				newRunner: func() runner.Runner {
					return &SimpleFakeRunner{}
				},
				withInjectData: func(objects *[]runtime.Object) {
					workflow := &serverlessv1alpha1.Workflow{
						TypeMeta: metav1.TypeMeta{
							APIVersion: WorkflowAPIVersion,
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
									Outputs:    []string{"simple_end"},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       "",
								},
								{
									Name:       "simple_end",
									Function:   "simple_end",
									Outputs:    []string{},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.End,
								},
							},
						},
						Status: serverlessv1alpha1.WorkflowStatus{},
					}
					*objects = append(*objects, workflow)
				},
				caseName: "simple_single_chain",
				sp: span.Span{
					FunctionName: "",
					WorkflowName: "simple",
				},
				parameters: map[string]interface{}{"a": "b"},
				expect:     map[string]interface{}{"d": "e"},
			},
			{
				newRunner: func() runner.Runner {
					return &SimpleFakeRunner{}
				},
				withInjectData: func(objects *[]runtime.Object) {
					workflow := &serverlessv1alpha1.Workflow{
						TypeMeta: metav1.TypeMeta{
							APIVersion: WorkflowAPIVersion,
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
					*objects = append(*objects, workflow)
				},
				caseName: "simple with parallel",
				sp: span.Span{
					FunctionName: "",
					WorkflowName: "simple",
				},
				parameters: map[string]interface{}{"a": "b"},
				expect:     map[string]interface{}{"simple_branch_1": map[string]interface{}{"branch_1": "branch_1"}, "simple_branch_2": map[string]interface{}{"branch_2": "branch_2"}},
			},
		}

		for _, testcase := range testcases {
			Convey(testcase.caseName, func() {
				// mock Runner
				runner.NewRunner = testcase.newRunner
				// mock data
				k8sutils.WithInjectData = testcase.withInjectData

				viper.Set("local", true)
				k8sutils.Prepare()
				mgr := NewManager()
				time.Sleep(2 * time.Second)
				result, err := mgr.Invoke(testcase.parameters, testcase.sp.WorkflowName, testcase.sp.FunctionName)
				So(err, ShouldBeNil)
				So(result, ShouldResemble, testcase.expect)
			})
		}
	})
}
