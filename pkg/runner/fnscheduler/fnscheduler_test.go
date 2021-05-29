package fnscheduler

import (
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	eventinitial "github.com/tass-io/scheduler/pkg/event/initial"
	middlewareinitial "github.com/tass-io/scheduler/pkg/middleware/initial"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/pkg/runner/lsds"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/errorutils"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	APIVersion              = "serverless.tass.io/v1alpha1"
	WorkflowRuntimeKindName = "WorkflowRuntime"
)

func TestFunctionScheduler_Run(t *testing.T) {
	NewInstance = func(functionName string) instance.Instance {
		return instance.NewMockInstance(functionName)
	}
	testcases := []struct {
		caseName       string
		skipped        bool
		instanceInject map[string]*set
		span           span.Span
		expect         error
	}{
		{
			caseName: "test simple run",
			skipped:  false,
			instanceInject: map[string]*set{
				"a": {
					Locker: &sync.RWMutex{},
					instances: []instance.Instance{
						NewInstance("a"),
					},
				},
			},
			span: span.Span{
				WorkflowName: "test",
				FlowName:     "a",
				FunctionName: "a",
			},
			expect: nil,
		},
		{
			caseName: "test simple run",
			skipped:  false,
			instanceInject: map[string]*set{
				"a": {
					Locker: &sync.RWMutex{},
					instances: []instance.Instance{
						NewInstance("a"),
					},
				},
			},
			span: span.Span{
				WorkflowName: "test",
				FlowName:     "b",
				FunctionName: "b",
			},
			expect: errorutils.NewNoInstanceError("b"),
		},
	}
	for _, testcase := range testcases {
		if testcase.skipped {
			continue
		}
		Convey(testcase.caseName, t, func() {
			FunctionSchedulerInit()
			fs := GetFunctionScheduler()
			fs.instances = testcase.instanceInject
			_, err := fs.Run(nil, testcase.span)
			So(err, ShouldResemble, testcase.expect)
		})
	}
}

// pay attention! this test depends on lsds
// todo decouple
func TestFunctionScheduler_RefreshAndRun(t *testing.T) {
	NewInstance = func(functionName string) instance.Instance {
		return instance.NewMockInstance(functionName)
	}
	testcases := []struct {
		caseName       string
		skipped        bool
		targets        map[string]int
		excepts        runner.InstanceStatus
		runTargets     map[string]error
		workflowName   string
		selfName       string
		withInjectData func(objects *[]runtime.Object)
	}{
		{
			caseName: "simple target sets",
			skipped:  false,
			targets: map[string]int{
				"a": 2,
				"b": 1,
			},
			excepts: map[string]int{
				"a": 2,
				"b": 1,
			},
			runTargets: map[string]error{
				"a": nil,
				"b": nil,
				"c": errorutils.NewNoInstanceError("c"),
			},
			workflowName: "test",
			selfName:     "ty",
			withInjectData: func(objects *[]runtime.Object) {
				workflowRuntime := &serverlessv1alpha1.WorkflowRuntime{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowRuntimeKindName,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test",
						Namespace: "default",
					},
					Spec: &serverlessv1alpha1.WorkflowRuntimeSpec{
						Status: serverlessv1alpha1.WfrtStatus{
							Instances: map[string]serverlessv1alpha1.Instance{
								"littledrizzle": serverlessv1alpha1.Instance{
									Status: &serverlessv1alpha1.InstanceStatus{
										HostIP: k8sutils.NewStringPtr("littledrizzle"),
										PodIP:  k8sutils.NewStringPtr("littledrizzle"),
									},
									ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
										"test_mid": {Number: 1},
									},
								},
							},
						},
					},
				}
				*objects = append(*objects, workflowRuntime)
			},
		},
	}
	viper.Set(env.Local, true)
	viper.Set(env.CreatePolicy, "default")
	viper.Set(env.InstanceScorePolicy, "default")
	for _, testcase := range testcases {
		if testcase.skipped {
			continue
		}
		Convey(testcase.caseName, t, func() {
			k8sutils.WithInjectData = testcase.withInjectData
			viper.Set(env.WorkflowName, testcase.workflowName)
			viper.Set(env.SelfName, testcase.selfName)
			k8sutils.Prepare()
			eventinitial.Initial()
			middlewareinitial.Initial()
			ls := lsds.GetLSDSIns()
			FunctionSchedulerInit()
			fs := GetFunctionScheduler()
			time.Sleep(500 * time.Millisecond)
			for functionName, num := range testcase.targets {
				// work like a prepare middleware
				fs.instances[functionName] = newSet(functionName)
				fs.Refresh(functionName, num)
				time.Sleep(500 * time.Millisecond)
			}
			stats := fs.Stats()
			So(stats, ShouldResemble, testcase.excepts)
			time.Sleep(1 * time.Second)
			for functionName, e := range testcase.runTargets {
				_, err := fs.Run(nil, span.Span{WorkflowName: "", FlowName: functionName, FunctionName: functionName})
				So(err, ShouldResemble, e)
			}
			k8sstats := ls.Stats()
			So(k8sstats, ShouldResemble, testcase.excepts)
		})
	}
}
