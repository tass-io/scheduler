package schedule_test

// if your test wanna to see the zap log, please import "github.com/tass-io/scheduler/pkg/tools/log"
import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tass-io/scheduler/pkg/event"
	eventschedule "github.com/tass-io/scheduler/pkg/event/schedule"
	"github.com/tass-io/scheduler/pkg/schedule"
	_ "github.com/tass-io/scheduler/pkg/tools/log"
	"go.uber.org/zap"
	"testing"
	"time"
)

type FakeScheduler struct {
	stats map[string]int
}

func (f *FakeScheduler) Refresh(functionName string, target int) {
	zap.S().Debugw("fake scheduler info", "functionName", functionName, "target", target)
	f.stats[functionName] = target
}

func TestScheduleHandler(t *testing.T) {

	Convey("test scheduler handler with mock upstream and downstream", t, func() {
		testcases := []struct {
			caseName      string
			skipped       bool
			upstreams     []eventschedule.ScheduleEvent
			orders        func() []event.Source
			mockScheduler func() schedule.Scheduler
			exceptResult  map[string]int
		}{
			{
				caseName: "test single event",
				skipped:  false,
				upstreams: []eventschedule.ScheduleEvent{
					{
						FunctionName: "a",
						Target:       1,
						Trend:        eventschedule.Increase,
						Source:       "Simple",
					},
				},
				orders: func() []event.Source {
					return []event.Source{"Simple"}
				},
				exceptResult: map[string]int{
					"a": 1,
				},
			},
		}
		for _, testcase := range testcases {
			if testcase.skipped {
				continue
			}
			Convey(testcase.caseName, func() {
				event.Orders = testcase.orders
				fake := &FakeScheduler{stats: make(map[string]int)}
				schedule.GetScheduler = func() schedule.Scheduler {
					return fake
				}
				handlerIns := eventschedule.GetScheduleHandlerIns()
				So(handlerIns, ShouldNotBeNil)
				err := handlerIns.Start()
				So(err, ShouldBeNil)
				for _, e := range testcase.upstreams {
					handlerIns.AddEvent(e)
				}
				time.Sleep(1 * time.Second)
				So(fake.stats, ShouldResemble, testcase.exceptResult)
			})
		}
	})
}
