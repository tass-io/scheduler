package pipe

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/cmd"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/runner/fnscheduler"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"github.com/tass-io/scheduler/test"
	"testing"
	"time"
)

type PipeMockInstance struct {
}

func (p *PipeMockInstance) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	return parameters, nil
}

func (p *PipeMockInstance) Score() int {
	return 1
}

func (p *PipeMockInstance) Release() {
	return
}

func (p *PipeMockInstance) Start() error {
	return nil
}

func TestSchedulerPipeline(t *testing.T) {
	fnscheduler.NewInstance = func(functionName string) instance.Instance {
		return &PipeMockInstance{}
	}
	testcases := []struct {
		caseName     string
		skipped      bool
		args         []string
		workflowName string
		request      dto.InvokeRequest
		expect       dto.InvokeResponse
	}{
		{
			caseName: "direct test with http request",
			skipped:  false,
			args:     []string{"-l", "-w", "../sample/samples/pipeline/direct.yaml"},
			request: dto.InvokeRequest{
				WorkflowName: "direct",
				FlowName:     "",
				Parameters: map[string]interface{}{
					"a": "b",
				},
			},
			expect: dto.InvokeResponse{
				Success: true,
				Message: "ok",
				Result: map[string]interface{}{
					"next": map[string]interface{}{
						"end": map[string]interface{}{
							"a": "b",
						},
					},
				},
			},
		},
		{
			caseName: "multi end test with http request",
			skipped:  false,
			args:     []string{"-l", "-w", "../sample/samples/pipeline/multiend.yaml"},
			request: dto.InvokeRequest{
				WorkflowName: "multiend",
				FlowName:     "",
				Parameters: map[string]interface{}{
					"a": "b",
				},
			},
			expect: dto.InvokeResponse{
				Success: true,
				Message: "ok",
				Result: map[string]interface{}{
					"end1": map[string]interface{}{
						"a": "b",
					},
					"end2": map[string]interface{}{
						"a": "b",
					},
					"end3": map[string]interface{}{
						"a": "b",
					},
				},
			},
		},
		{
			caseName: "devide and merge test with http request",
			skipped:  false,
			args:     []string{"-l", "-w", "../sample/samples/pipeline/devide-and-merge.yaml"},
			request: dto.InvokeRequest{
				WorkflowName: "devide-and-merge",
				FlowName:     "",
				Parameters: map[string]interface{}{
					"a": "b",
				},
			},
			expect: dto.InvokeResponse{
				Success: true,
				Message: "ok",
				Result: map[string]interface{}{
					"region1": map[string]interface{}{
						"filter": map[string]interface{}{
							"result": map[string]interface{}{"a": "b"},
						},
					},
					"region2": map[string]interface{}{
						"filter": map[string]interface{}{
							"result": map[string]interface{}{"a": "b"},
						},
					},
					"region3": map[string]interface{}{
						"result": map[string]interface{}{
							"a": "b",
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
		Convey("use http request test scheduler", t, func(c C) {
			go func() {
				cmd.SetArgs(testcase.args)
				err := cmd.Execute()
				c.So(err, ShouldBeNil)
			}()
			time.Sleep(1 * time.Second)
			resp := &dto.InvokeResponse{}
			status, err := test.RequestJson("http://localhost:8080/v1/workflow/", "POST", map[string]string{}, testcase.request, resp)
			So(err, ShouldBeNil)
			So(status, ShouldEqual, 200)
			So(*resp, ShouldResemble, testcase.expect)
		})
	}
}
