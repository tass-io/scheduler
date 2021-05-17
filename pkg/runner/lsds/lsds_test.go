package lsds

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sync"
	"testing"
	"time"
)

const (
	APIVersion              = "serverless.tass.io/v1alpha1"
	WorkflowKind            = "Workflow"
	WorkflowRuntimeKind = "WorkflowRuntime"
)

func TestLSDS_Policy(t *testing.T) {
	testcases := []struct {
		caseName       string
		policyName     string
		workflowName   string
		selfName       string
		withInjectData func(objects *[]runtime.Object)
		expects        map[string]string // the key is functionName, the value is the target ip
	}{
		{
			caseName:     "test simple policy",
			policyName:   "simple",
			workflowName: "test",
			selfName:     "ty",
			withInjectData: func(objects *[]runtime.Object) {
				name := "test"
				workflow := &serverlessv1alpha1.Workflow{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowKind,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "default",
					},
					Spec: serverlessv1alpha1.WorkflowSpec{
						Spec: []serverlessv1alpha1.Flow{
							{
								Name:       "test_start",
								Function:   "test_start",
								Outputs:    []string{"test_mid"},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       serverlessv1alpha1.Start,
							},
							{
								Name:       "test_mid",
								Function:   "test_mid",
								Outputs:    []string{"test_end"},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       "",
							},
							{
								Name:       "test_end",
								Function:   "test_end",
								Outputs:    []string{},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       serverlessv1alpha1.End,
							},
						},
					},
					Status: serverlessv1alpha1.WorkflowStatus{},
				}
				workflowRuntime := &serverlessv1alpha1.WorkflowRuntime{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowRuntimeKind,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "default",
					},
					Status: serverlessv1alpha1.WorkflowRuntimeStatus{
						Instances: map[string]serverlessv1alpha1.Instance{
							"ty": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "ty",
									PodIP:  "ty",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_start": {Number: 1},
								},
							},
							"tx": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "tx",
									PodIP:  "tx",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_end": {Number: 1},
								},
							},
							"littledrizzle": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "littledrizzle",
									PodIP:  "littledrizzle",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_mid": {Number: 1},
								},
							},
						},
					},
				}
				*objects = append(*objects, workflow)
				*objects = append(*objects, workflowRuntime)
			},
			expects: map[string]string{
				"test_mid":   "littledrizzle",
				"test_end":   "tx",
				"test_start": "",
			},
		},
	}
	viper.Set(env.Local, true)
	for _, testcase := range testcases {
		Convey(testcase.caseName, t, func() {
			viper.Set(env.WorkflowName, testcase.workflowName)
			viper.Set(env.SelfName, testcase.selfName)
			viper.Set(env.Policy, testcase.policyName)
			// mock data
			k8sutils.WithInjectData = testcase.withInjectData
			k8sutils.Prepare()
			once = &sync.Once{}
			daemon := GetLSDSIns()
			time.Sleep(2 * time.Second)
			for functionName, ip := range testcase.expects {
				target := daemon.chooseTarget(functionName)
				So(target, ShouldEqual, ip)
			}
		})
	}
}

func ConvertMapToProcessRuntimes(info map[string]int) serverlessv1alpha1.ProcessRuntimes {
	result := serverlessv1alpha1.ProcessRuntimes{}
	for key, num := range info {
		result[key] = serverlessv1alpha1.ProcessRuntime{Number: num}
	}
	return result
}
func TestLSDS_Sync(t *testing.T) {
	testcases := []struct {
		caseName       string
		skipped        bool
		policyName     string
		workflowName   string
		selfName       string
		withInjectData func(objects *[]runtime.Object)
		syncInfo       map[string]int
	}{
		{
			caseName:     "test simple sync",
			skipped:      false,
			policyName:   "simple",
			workflowName: "test",
			selfName:     "ty",
			withInjectData: func(objects *[]runtime.Object) {
				name := "test"
				workflow := &serverlessv1alpha1.Workflow{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowKind,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "default",
					},
					Spec: serverlessv1alpha1.WorkflowSpec{
						Spec: []serverlessv1alpha1.Flow{
							{
								Name:       "test_start",
								Function:   "test_start",
								Outputs:    []string{"test_mid"},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       serverlessv1alpha1.Start,
							},
							{
								Name:       "test_mid",
								Function:   "test_mid",
								Outputs:    []string{"test_end"},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       "",
							},
							{
								Name:       "test_end",
								Function:   "test_end",
								Outputs:    []string{},
								Conditions: []*serverlessv1alpha1.Condition{},
								Statement:  serverlessv1alpha1.Direct,
								Role:       serverlessv1alpha1.End,
							},
						},
					},
					Status: serverlessv1alpha1.WorkflowStatus{},
				}
				workflowRuntime := &serverlessv1alpha1.WorkflowRuntime{
					TypeMeta: metav1.TypeMeta{
						APIVersion: APIVersion,
						Kind:       WorkflowRuntimeKind,
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      name,
						Namespace: "default",
					},
					Status: serverlessv1alpha1.WorkflowRuntimeStatus{
						Instances: map[string]serverlessv1alpha1.Instance{
							"ty": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "ty",
									PodIP:  "ty",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_start": {Number: 1},
								},
							},
							"tx": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "tx",
									PodIP:  "tx",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_end": {Number: 1},
								},
							},
							"littledrizzle": serverlessv1alpha1.Instance{
								Status: serverlessv1alpha1.InstanceStatus{
									HostIP: "littledrizzle",
									PodIP:  "littledrizzle",
								},
								ProcessRuntimes: map[string]serverlessv1alpha1.ProcessRuntime{
									"test_mid": {Number: 1},
								},
							},
						},
					},
				}
				*objects = append(*objects, workflow)
				*objects = append(*objects, workflowRuntime)
			},
			syncInfo: map[string]int{
				"test_mid":   1,
				"test_end":   1,
				"test_start": 2,
			},
		},
	}
	viper.Set(env.Local, true)
	for _, testcase := range testcases {
		if testcase.skipped {
			continue
		}
		Convey(testcase.caseName, t, func() {
			viper.Set(env.WorkflowName, testcase.workflowName)
			viper.Set(env.SelfName, testcase.selfName)
			viper.Set(env.Policy, testcase.policyName)
			// mock data
			k8sutils.WithInjectData = testcase.withInjectData
			k8sutils.Prepare()
			once = &sync.Once{}
			daemon := GetLSDSIns()
			time.Sleep(2 * time.Second)
			k8sutils.Sync(testcase.syncInfo)
			time.Sleep(1 * time.Second)
			wfrt, existed, err := daemon.getWorkflowRuntimeByName(daemon.workflowName)
			So(err, ShouldBeNil)
			So(existed, ShouldBeTrue)
			zap.S().Debugw("refresh WorkflowRuntime", "WorkflowRuntime", wfrt)
			So(wfrt.Status.Instances[testcase.selfName].ProcessRuntimes, ShouldResemble, ConvertMapToProcessRuntimes(testcase.syncInfo))
		})
	}
}
