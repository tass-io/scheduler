package qps_test

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

func TestTTLReset(t *testing.T) {
	testcases := []struct {
		skipped      bool
		args         []string
		workflowName string
		requests     map[string]dto.InvokeRequest
		expects      map[string]dto.InvokeResponse
	}{
		{
			skipped: false,
			args:    []string{"-l", "-T", "5s", "-q", "-w", "../sample/samples/switch/if-else.yaml"},
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
					Result: map[string]interface{}{
						"flows": map[string]interface{}{
							"left": map[string]interface{}{
								"a":         5,
								"function1": "function1",
								"function3": "function3",
							},
						},
					},
				},
				"right": {
					Success: true,
					Message: "ok",
					Result: map[string]interface{}{
						"flows": map[string]interface{}{
							"right": map[string]interface{}{
								"a":         1,
								"function1": "function1",
								"function2": "function2",
							},
						},
					},
				},
			},
		},
	}

	fnscheduler.NewInstance = func(functionName string) instance.Instance {
		return instance.NewMockInstance(functionName)
	}

	for _, testcase := range testcases {
		if testcase.skipped {
			continue
		}

		Convey("use http request test scheduler with request", t, func(c C) {
			go func() {
				cmd.SetArgs(testcase.args)
				err := cmd.Execute()
				c.So(err, ShouldBeNil)
			}()
			time.Sleep(500 * time.Millisecond)

			t.Logf("do invocation \n")
			for caseName := range testcase.requests {
				t.Logf("testcase %s\n", caseName)
				resp := &dto.InvokeResponse{}
				status, err := test.RequestJson("http://localhost:8080/v1/workflow/", "POST",
					map[string]string{}, testcase.requests[caseName], resp)
				So(err, ShouldBeNil)
				So(status, ShouldEqual, 200)
				expect := testcase.expects[caseName].Result
				// todo evil equal
				So(fmt.Sprintf("%v", resp.Result), ShouldResemble, fmt.Sprintf("%v", expect))
			}

			time.Sleep(3 * time.Second) // the sleep time depends on qps refresh time
			stats := fnscheduler.GetFunctionScheduler().Stats()
			t.Logf("stats after 3 seconds %v\n", stats)
			for _, stat := range stats {
				So(stat, ShouldNotEqual, 0)
			}

			t.Logf("do invocation again\n")
			for caseName := range testcase.requests {
				t.Logf("testcase %s\n", caseName)
				resp := &dto.InvokeResponse{}
				status, err := test.RequestJson("http://localhost:8080/v1/workflow/", "POST",
					map[string]string{}, testcase.requests[caseName], resp)
				So(err, ShouldBeNil)
				So(status, ShouldEqual, 200)
				expect := testcase.expects[caseName].Result
				// todo evil equal
				So(fmt.Sprintf("%v", resp.Result), ShouldResemble, fmt.Sprintf("%v", expect))
			}

			time.Sleep(2 * time.Second)
			stats = fnscheduler.GetFunctionScheduler().Stats()
			t.Logf("stats after 5 seconds %v\n", stats)
			for _, stat := range stats {
				So(stat, ShouldNotEqual, 0)
			}

			time.Sleep(5 * time.Second)
			stats = fnscheduler.GetFunctionScheduler().Stats()
			t.Logf("stats after 10 seconds %v\n", stats)
			for _, stat := range stats {
				So(stat, ShouldEqual, 0)
			}
		})
	}
}
