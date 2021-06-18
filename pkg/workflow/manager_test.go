package workflow

import (
	"testing"
	"time"

	"github.com/tass-io/scheduler/pkg/runner/helper"

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

type SimpleFakeRunner struct {}

// SimpleFakeRunner 'registers' all functions called later, which indexes the function by span.FlowName
func (r *SimpleFakeRunner) Run(
	sp *span.Span, parameters map[string]interface{}) (result map[string]interface{}, err error) {

	switch sp.GetFlowName() {
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
	case "tass":
		{
			parameters["tass"] = "tass"
			return parameters, nil
		}
	case "condition_mid":
		{
			parameters["condition_mid"] = "condition_mid"
			return parameters, nil
		}
	case "condition_flow":
		{
			parameters["condition_flow"] = "condition_flow"
			return parameters, nil
		}
	case "condition_end":
		{
			parameters["condition_end"] = "condition_end"
			return parameters, nil
		}
	case "condition_nested":
		{
			parameters["condition_nested"] = "condition_nested"
			return parameters, nil
		}
	default:
		{
			return parameters, nil
		}
	}
}

func (r *SimpleFakeRunner) Stats() runner.InstanceStatus {
	return nil
}

func (r *SimpleFakeRunner) FunctionStats(functionName string) int {
	return 0
}

func (r *SimpleFakeRunner) NewInstanceSetIfNotExist(fnName string) {}

func TestManager(t *testing.T) {
	Convey("test workflow work with mock client", t, func() {
		testcases := []struct {
			caseName       string
			skipped        bool
			newRunner      func() runner.Runner
			withInjectData func(objects *[]runtime.Object)
			sp             *span.Span
			parameters     map[string]interface{}
			expect         map[string]interface{}
		}{
			{
				caseName: "simple single chain",
				skipped:  false,
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
				sp: span.NewSpan("simple", "", ""),
				parameters: map[string]interface{}{
					"a": "b",
				},
				expect: map[string]interface{}{
					"simple_mid": map[string]interface{}{
						"simple_end": map[string]interface{}{
							"d": "e",
						},
					},
				},
			},
			{
				caseName: "simple with parallel",
				skipped:  false,
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
				sp: span.NewSpan("simple", "", ""),
				parameters: map[string]interface{}{
					"a": "b",
				},
				expect: map[string]interface{}{
					"simple_mid": map[string]interface{}{
						"simple_branch_1": map[string]interface{}{
							"branch_1": "branch_1",
						},
						"simple_branch_2": map[string]interface{}{
							"branch_2": "branch_2",
						},
					},
				},
			},
			{
				caseName: "simple with one condition",
				skipped:  false,
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
							Name:      "condition",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.WorkflowSpec{
							Spec: []serverlessv1alpha1.Flow{
								{
									Name:       "tass",
									Function:   "tass",
									Outputs:    []string{"condition_mid"},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.Start,
								},
								{
									Name:     "condition_mid",
									Function: "condition_mid",
									Outputs:  []string{"condition_end"},
									Conditions: []*serverlessv1alpha1.Condition{
										{
											Name:        "root",
											Type:        "string",
											Operator:    "eq",
											Target:      "tass",
											Comparision: "tass",
											Destination: serverlessv1alpha1.Destination{
												IsTrue: serverlessv1alpha1.Next{
													Flows: []string{"condition_flow"},
												},
												IsFalse: serverlessv1alpha1.Next{},
											},
										},
									},
									Statement: serverlessv1alpha1.Switch,
									Role:      "",
								},
								{
									Name:       "condition_flow",
									Function:   "condition_flow",
									Outputs:    []string{},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.End,
								},
								{
									Name:       "condition_end",
									Function:   "condition_end",
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
				sp:         span.NewSpan("condition", "", ""),
				parameters: map[string]interface{}{"a": "b"},
				expect: map[string]interface{}{
					"condition_mid": map[string]interface{}{
						"condition_end": map[string]interface{}{
							"condition_end": "condition_end",
							"flows": map[string]interface{}{
								"condition_flow": map[string]interface{}{
									"a":               "b",
									"condition_flow":  "condition_flow",
									"condition_mid":   "condition_mid",
									"tass": "tass",
								},
							},
						},
					},
				},
			},
			{
				caseName: "simple with one nested condition",
				skipped:  false,
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
							Name:      "condition",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.WorkflowSpec{
							Spec: []serverlessv1alpha1.Flow{
								{
									Name:       "tass",
									Function:   "tass",
									Outputs:    []string{"condition_mid"},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.Start,
								},
								{
									Name:     "condition_mid",
									Function: "condition_mid",
									Outputs:  []string{"condition_end"},
									Conditions: []*serverlessv1alpha1.Condition{
										{
											Name:        "root",
											Type:        "string",
											Operator:    "eq",
											Target:      "tass",
											Comparision: "tass",
											Destination: serverlessv1alpha1.Destination{
												IsTrue: serverlessv1alpha1.Next{
													Conditions: []string{"condition_nested"},
													Flows:      []string{"condition_flow"},
												},
												IsFalse: serverlessv1alpha1.Next{},
											},
										},
										{
											Name:        "condition_nested",
											Type:        "string",
											Operator:    "eq",
											Target:      "tass",
											Comparision: "tass",
											Destination: serverlessv1alpha1.Destination{
												IsTrue: serverlessv1alpha1.Next{
													Flows:      []string{"condition_nested"},
													Conditions: []string{},
												},
											},
										},
									},
									Statement: serverlessv1alpha1.Switch,
									Role:      "",
								},
								{
									Name:       "condition_flow",
									Function:   "condition_flow",
									Outputs:    []string{},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.End,
								},
								{
									Name:       "condition_nested",
									Function:   "condition_nested",
									Outputs:    []string{},
									Conditions: []*serverlessv1alpha1.Condition{},
									Statement:  serverlessv1alpha1.Direct,
									Role:       serverlessv1alpha1.End,
								},
								{
									Name:       "condition_end",
									Function:   "condition_end",
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
				sp:         span.NewSpan("condition", "", ""),
				parameters: map[string]interface{}{"a": "b"},
				expect: map[string]interface{}{
					"condition_mid": map[string]interface{}{
						"condition_end": map[string]interface{}{
							"condition_end": "condition_end",
							"conditions": map[string]interface{}{
								"condition_nested": map[string]interface{}{
									"flows": map[string]interface{}{
										"condition_nested": map[string]interface{}{
											"a":                "b",
											"condition_mid":    "condition_mid",
											"condition_nested": "condition_nested",
											"tass":  "tass",
										},
									},
								},
							},
							"flows": map[string]interface{}{
								"condition_flow": map[string]interface{}{
									"a":               "b",
									"condition_flow":  "condition_flow",
									"condition_mid":   "condition_mid",
									"tass": "tass",
								},
							},
						},
					},
				},
			},
		}

		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}
			Convey(testcase.caseName, func() {
				// mock Runner
				helper.GetMasterRunner = testcase.newRunner
				// mock data
				k8sutils.WithInjectData = testcase.withInjectData

				viper.Set("local", true)
				k8sutils.Prepare()
				mgr := NewManager()
				time.Sleep(500 * time.Millisecond)
				result, err := mgr.Invoke(testcase.sp, testcase.parameters)
				So(err, ShouldBeNil)
				So(result, ShouldResemble, testcase.expect)
			})
		}
	})
}
