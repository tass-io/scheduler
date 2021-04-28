package workflow

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/pkg/runner"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/k8sutils"
	"k8s.io/apimachinery/pkg/runtime"
)

type SimpleFakeRunner struct {
}

func (r *SimpleFakeRunner) Run(parameters map[string]interface{}, span span.Span) (result map[string]interface{}, err error) {
	return nil, nil
}

func TestManager(t *testing.T) {
	Convey("test workflow work with mock client", t, func() {
		testcases := []struct {
			newRunner      func() runner.Runner
			withInjectData func(objects []runtime.Object)
			caseName       string
			sp             span.Span
			parameters     map[string]interface{}
			expect         map[string]interface{}
		}{
			{
				newRunner: func() runner.Runner {
					return &SimpleFakeRunner{}
				},
				withInjectData: func(objects []runtime.Object) {

				},
				caseName: "simple",
				sp: span.Span{
					FunctionName: "simple",
					WorkflowName: "simple",
				},
				parameters: map[string]interface{}{"a": "b"},
				expect:     map[string]interface{}{"b": "c"},
			},
			{},
		}

		for _, testcase := range testcases {
			// mock Runner
			runner.NewRunner = testcase.newRunner
			// mock data
			k8sutils.WithInjectData = testcase.withInjectData
			k8sutils.Prepare()
			mgr := NewManager()
			result, err := mgr.Invoke(testcase.parameters, testcase.sp.WorkflowName, testcase.sp.FunctionName)
			So(err, ShouldBeNil)
			So(result, ShouldAlmostEqual, testcase.expect)
		}
	})
}
