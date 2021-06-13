package _switch

import (
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/cmd"
	"github.com/tass-io/scheduler/pkg/dto"
	"github.com/tass-io/scheduler/pkg/runner/fnscheduler"
	"github.com/tass-io/scheduler/pkg/runner/instance"
	"github.com/tass-io/scheduler/test"
)

//
type switchMockInstance struct {
	name string
}

func (s *switchMockInstance) Invoke(parameters map[string]interface{}) (map[string]interface{}, error) {
	parameters[s.name] = s.name
	return parameters, nil
}

func (s *switchMockInstance) Score() int {
	return 0
}

func (s *switchMockInstance) Release() {
	return
}

func (s *switchMockInstance) IsRunning() bool {
	return true
}

func (s *switchMockInstance) Start() error {
	return nil
}

func (s *switchMockInstance) HasRequests() bool {
	return false
}

func (s *switchMockInstance) InitDone() {}

func TestSchedulerSwitch(t *testing.T) {
	testcases := []struct {
		skipped      bool
		args         []string
		workflowName string
		requests     map[string]dto.InvokeRequest
		expects      map[string]dto.InvokeResponse
	}{
		{
			skipped: false,
			args:    []string{"-l", "-w", "../sample/samples/switch/if-else.yaml"},
			requests: map[string]dto.InvokeRequest{
				"left": {
					WorkflowName: "if-else",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 5},
				},
				"right": {
					WorkflowName: "if-else",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 1},
				},
			},
			expects: map[string]dto.InvokeResponse{
				"left": {
					Success: true,
					Message: "ok",
					Result:  map[string]interface{}{"flows": map[string]interface{}{"left": map[string]interface{}{"a": 5, "function1": "function1", "function3": "function3"}}},
				},
				"right": {
					Success: true,
					Message: "ok",
					Result:  map[string]interface{}{"flows": map[string]interface{}{"right": map[string]interface{}{"a": 1, "function1": "function1", "function2": "function2"}}},
				},
			},
		},
		{
			skipped: false,
			args:    []string{"-l", "-w", "../sample/samples/switch/switch.yaml"},
			requests: map[string]dto.InvokeRequest{
				"x > 10": {
					WorkflowName: "switch",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 11},
				},
				"x == 10": {
					WorkflowName: "switch",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 10},
				},
				"10 > x > 2": {
					WorkflowName: "switch",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 8},
				},
				"x <= 2": {
					WorkflowName: "switch",
					FlowName:     "",
					Parameters:   map[string]interface{}{"a": 1},
				},
			},
			expects: map[string]dto.InvokeResponse{
				"x > 10": {
					Success: true,
					Message: "ok",
					Result:  map[string]interface{}{"flows": map[string]interface{}{"case1": map[string]interface{}{"a": 11, "function1": "function1", "function4": "function4"}}},
				},
				"x == 10": {
					Success: true,
					Message: "ok",
					Result: map[string]interface{}{
						"conditions": map[string]interface{}{
							"equal": map[string]interface{}{
								"flows": map[string]interface{}{
									"case2": map[string]interface{}{
										"a":         10,
										"function1": "function1",
										"function3": "function3",
									},
								},
							},
						},
					},
				},
				"10 > x > 2": {
					Success: true,
					Message: "ok",
					Result: map[string]interface{}{
						"conditions": map[string]interface{}{
							"equal": map[string]interface{}{
								"conditions": map[string]interface{}{
									"narrow": map[string]interface{}{
										"flows": map[string]interface{}{
											"case3": map[string]interface{}{
												"a":         8,
												"function1": "function1",
												"function2": "function2",
											},
										},
									},
								},
							},
						},
					},
				},
				"x <= 2": {
					Success: true,
					Message: "ok",
					Result: map[string]interface{}{
						"conditions": map[string]interface{}{
							"equal": map[string]interface{}{
								"conditions": map[string]interface{}{
									"narrow": map[string]interface{}{
										"flows": map[string]interface{}{
											"case4": map[string]interface{}{
												"a":         1,
												"function1": "function1",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
	fnscheduler.NewInstance = func(functionName string) instance.Instance {
		return &switchMockInstance{name: functionName}
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
			time.Sleep(500 * time.Millisecond)
			for caseName := range testcase.requests {
				t.Logf("testcase %s\n", caseName)
				resp := &dto.InvokeResponse{}
				status, err := test.RequestJson("http://localhost:8080/v1/workflow/", "POST", map[string]string{}, testcase.requests[caseName], resp)
				So(err, ShouldBeNil)
				So(status, ShouldEqual, 200)
				expect := testcase.expects[caseName].Result
				// todo evil equal
				So(fmt.Sprintf("%v", resp.Result), ShouldResemble, fmt.Sprintf("%v", expect))
			}
		})

	}
}
