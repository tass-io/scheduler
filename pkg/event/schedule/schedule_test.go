package schedule

// if your test wanna to see the zap log, please import "github.com/tass-io/scheduler/pkg/utils/log"
import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/pkg/event"
	"github.com/tass-io/scheduler/pkg/schedule"
	_ "github.com/tass-io/scheduler/pkg/utils/log"
	"go.uber.org/zap"
)

type fakeScheduler struct {
	stats map[string]int
}

func (f *fakeScheduler) Refresh(functionName string, target int) {
	zap.S().Debugw("fake scheduler info", "functionName", functionName, "target", target)
	f.stats[functionName] = target
}

func (f *fakeScheduler) ColdStartDone(functionName string) {
	zap.S().Debugw("fake scheduler cold start", "functionName", functionName)
}

func (f *fakeScheduler) NewInstanceSetIfNotExist(functionName string) {}

var _ schedule.Scheduler = &fakeScheduler{}

func TestScheduleHandler(t *testing.T) {

	testcases := []struct {
		caseName      string
		skipped       bool
		upstreams     []event.ScheduleEvent
		orders        func() []event.Source
		mockScheduler func() schedule.Scheduler
		exceptResult  map[string]int
	}{
		{
			caseName: "test single function with single-source event",
			skipped:  false,
			upstreams: []event.ScheduleEvent{
				{
					FunctionName: "a",
					Target:       2,
					Trend:        event.Increase,
					Source:       "First",
				},
				{
					FunctionName: "a",
					Target:       1,
					Trend:        event.Increase,
					Source:       "First",
				},
			},
			orders: func() []event.Source {
				return []event.Source{"First"}
			},
			exceptResult: map[string]int{
				"a": 2,
			},
		},
		{
			caseName: "test single function with different-order event",
			skipped:  false,
			upstreams: []event.ScheduleEvent{
				{
					FunctionName: "a",
					Target:       2,
					Trend:        event.Increase,
					Source:       "First",
				},
				{
					FunctionName: "a",
					Target:       1,
					Trend:        event.Increase,
					Source:       "Second",
				},
			},
			orders: func() []event.Source {
				return []event.Source{"First", "Second"}
			},
			exceptResult: map[string]int{
				"a": 2,
			},
		},
		{
			caseName: "test single function with different-order-different-trend event",
			skipped:  false,
			upstreams: []event.ScheduleEvent{
				{
					FunctionName: "a",
					Target:       1,
					Trend:        event.Decrease,
					Source:       "First",
				},
				{
					FunctionName: "a",
					Target:       2,
					Trend:        event.Increase,
					Source:       "Second",
				},
			},
			orders: func() []event.Source {
				return []event.Source{"First", "Second"}
			},
			exceptResult: map[string]int{
				"a": 1,
			},
		},
		{
			caseName: "test single function with same-source different-order-different-trend event",
			skipped:  false,
			upstreams: []event.ScheduleEvent{
				{
					FunctionName: "a",
					Target:       1,
					Trend:        event.Decrease,
					Source:       "First",
				},
				{
					FunctionName: "a",
					Target:       3,
					Trend:        event.Increase,
					Source:       "Second",
				},
				{
					FunctionName: "a",
					Target:       2,
					Trend:        event.Increase,
					Source:       "Second",
				},
			},
			orders: func() []event.Source {
				return []event.Source{"First", "Second"}
			},
			exceptResult: map[string]int{
				"a": 1,
			},
		},
		{
			caseName: "test single function with same-source mercy",
			skipped:  false,
			upstreams: []event.ScheduleEvent{
				{
					FunctionName: "a",
					Target:       1,
					Trend:        event.Increase,
					Source:       "First",
				},
				{
					FunctionName: "a",
					Target:       3,
					Trend:        event.Increase,
					Source:       "Second",
				},
				{
					FunctionName: "b",
					Target:       3,
					Trend:        event.Decrease,
					Source:       "First",
				},
				{
					FunctionName: "b",
					Target:       1,
					Trend:        event.Decrease,
					Source:       "Second",
				},
			},
			orders: func() []event.Source {
				return []event.Source{"First", "Second"}
			},
			exceptResult: map[string]int{
				"a": 3,
				"b": 1,
			},
		},
	}
	for _, testcase := range testcases {
		if testcase.skipped {
			continue
		}
		t.Log(testcase.caseName)
		Convey(testcase.caseName, t, func() {
			event.GetOrderedSources = testcase.orders
			fake := &fakeScheduler{stats: make(map[string]int)}
			schedule.GetScheduler = func() schedule.Scheduler {
				return fake
			}
			GetScheduleHandlerIns = func() event.Handler {
				return newScheduleHandler()
			}
			handlerIns := GetScheduleHandlerIns()
			So(handlerIns, ShouldNotBeNil)
			err := handlerIns.Start()
			So(err, ShouldBeNil)
			for _, e := range testcase.upstreams {
				handlerIns.AddEvent(e)
			}
			time.Sleep(500 * time.Millisecond)
			So(fake.stats, ShouldResemble, testcase.exceptResult)
		})
	}
}
