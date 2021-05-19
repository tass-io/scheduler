package instance

import (
	"os/exec"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/store"
	"github.com/tass-io/scheduler/pkg/tools/base64"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	FunctionAPIVersion = "serverless.tass.io/v1alpha1"
	FunctionKind       = "function"
)

func TestProcessInstance(t *testing.T) {
	Convey("test process instance", t, func() {
		testcases := []struct {
			caseName       string
			skipped        bool
			functionName   string
			fileName       string
			request        map[string]interface{}
			withInjectData func(objects *[]runtime.Object)
			expect         map[string]interface{}
		}{
			{
				caseName:     "test with golang wrapper",
				skipped:      false,
				functionName: "default-golang-wrapper",
				fileName:     "../../../user-code/default-golang-wrapper.zip",
				request: map[string]interface{}{
					"a": "b",
				},
				withInjectData: func(objects *[]runtime.Object) {
					function := &serverlessv1alpha1.Function{
						TypeMeta: metav1.TypeMeta{
							APIVersion: FunctionAPIVersion,
							Kind:       FunctionKind,
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "default-golang-wrapper",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.FunctionSpec{
							Environment: serverlessv1alpha1.Golang,
							Resource: serverlessv1alpha1.Resource{
								Cpu:    "200%",
								Memory: "100Mi",
							},
						},
					}
					*objects = append(*objects, function)
				},
				expect: map[string]interface{}{
					"a":     "b",
					"motto": "Veni Vidi Vici",
				},
			},
			{
				caseName:     "test with golang wrapper and plugin",
				skipped:      false,
				functionName: "plugin-golang-wrapper",
				fileName:     "../../../user-code/plugin-golang-wrapper.zip",
				request: map[string]interface{}{
					"a": "b",
				},
				withInjectData: func(objects *[]runtime.Object) {
					function := &serverlessv1alpha1.Function{
						TypeMeta: metav1.TypeMeta{
							APIVersion: FunctionAPIVersion,
							Kind:       FunctionKind,
						},
						ObjectMeta: metav1.ObjectMeta{
							Name:      "plugin-golang-wrapper",
							Namespace: "default",
						},
						Spec: serverlessv1alpha1.FunctionSpec{
							Environment: serverlessv1alpha1.Golang,
							Resource: serverlessv1alpha1.Resource{
								Cpu:    "200%",
								Memory: "100Mi",
							},
						},
					}
					*objects = append(*objects, function)
				},
				expect: map[string]interface{}{
					"a":      "b",
					"plugin": "plugin",
				},
			},
		}
		viper.Set(env.Local, true)
		viper.Set(env.RedisIp, "10.0.0.96")
		viper.Set(env.RedisPort, "30285")
		viper.Set(env.RedisPassword, "")
		viper.Set(env.DefaultDb, 0)
		// build binary
		complieCmd := exec.Command("go", "build", "../../../main.go")
		err := complieCmd.Start()
		So(err, ShouldBeNil)
		err = complieCmd.Wait()
		So(err, ShouldBeNil)
		binary = "./main"
		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}
			code, err := base64.EncodeUserCode(testcase.fileName)
			So(err, ShouldBeNil)
			err = store.Set("default", testcase.functionName, code)
			So(err, ShouldBeNil)
			k8sutils.WithInjectData = testcase.withInjectData
			k8sutils.Prepare()
			time.Sleep(500 * time.Millisecond)
			process := NewProcessInstance(testcase.functionName)
			err = process.Start()
			So(err, ShouldBeNil)
			time.Sleep(500 * time.Millisecond)
			for i := 1; i < 50; i++ {
				result, err := process.Invoke(testcase.request)
				So(err, ShouldBeNil)
				So(result, ShouldResemble, testcase.expect)
			}
			process.Release()
			time.Sleep(1 * time.Second)
			So(process.getWaitNum(), ShouldEqual, 0)
		}
	})
}
