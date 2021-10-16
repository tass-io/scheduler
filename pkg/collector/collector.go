package collector

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/tass-io/scheduler/pkg/predictmodel"
	"github.com/tass-io/scheduler/pkg/predictmodel/store"
	"go.uber.org/zap"
)

const (
	updateModelInterval = time.Second * 10
)

var collector *Collector

// Collector is responsible for collecting metrics for different functions.
type Collector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Locker
	wf      string
	ch      chan *record
	records map[string]*store.Object
}

// RecordType is the type of a record, which indicates the different phases of a function.
type RecordType string

// RecordType enum
const (
	RecordColdStart RecordType = "coldstart"
	RecordExec      RecordType = "exec"
)

type record struct {
	upstream string // upstream flow name
	flow     string // flow name
	fn       string // function name
	t        RecordType
	d        time.Duration
}

// Init initializes the Collector which is responsible for collecting metrics for different functions.
// Collector is a singleton, if workflow changes, it will be recreated.
func Init(workflow string) {
	if collector == nil {
		newCollector(workflow)
		collector.Start()
		return
	}
	if collector.wf != workflow {
		zap.S().Info("workflow changes, generate a new workflow collector")
		collector.cancel()
		newCollector(workflow)
		collector.Start()
	}
}

func newCollector(workflow string) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	collector = &Collector{
		ctx:     ctx,
		cancel:  cancel,
		mu:      &sync.Mutex{},
		wf:      workflow,
		ch:      make(chan *record, 100),
		records: map[string]*store.Object{},
	}
}

// GetCollector returns the Collector singleton.
func GetCollector() *Collector {
	return collector
}

// Record records one specific phase time cost of a function.
func (c *Collector) Record(upstream, flow, fn string, t RecordType, d time.Duration) {
	if viper.GetBool(env.Collector) {
		c.ch <- &record{
			upstream: upstream,
			flow:     flow,
			fn:       fn,
			t:        t,
			d:        d,
		}
	}
}

func (c *Collector) Start() {
	if viper.GetBool(env.Collector) {
		go c.publish(updateModelInterval)
		go c.startCollector()
	}
}

func (c *Collector) startCollector() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case r := <-c.ch:
			var obj *store.Object
			obj, ok := c.records[r.flow]
			if !ok {
				obj = &store.Object{
					Flow: r.flow,
					Fn:   r.fn,
				}
				c.records[r.flow] = obj
			}
			switch r.t {
			case RecordColdStart:
				obj.Coldstart = append(obj.Coldstart, time.Duration(r.d))
			case RecordExec:
				rawPathExist := false
				for index, path := range obj.Paths {
					if path.From == r.upstream {
						path.Exec = append(path.Exec, time.Duration(r.d))
						path.Count++
						obj.Paths[index] = path
						rawPathExist = true
						break
					}
				}
				if !rawPathExist {
					obj.Paths = append(obj.Paths, store.Path{
						From:  r.upstream,
						Exec:  []time.Duration{time.Duration(r.d)},
						Count: 1,
					},
					)
				}
			default:
				panic("unknown record type")
			}
		}
	}
}

// publish sends thr current metrics to the prediction model
func (c *Collector) publish(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.printMockdata()
			records := c.fetchAndClearRecords()
			err := predictmodel.GetPredictModelManager().PatchRecords(records)
			if err != nil {
				zap.S().Error("failed to patch records to prediction model manager", err)
			}
		}
	}
}

func (c *Collector) fetchAndClearRecords() map[string]*store.Object {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := c.records
	c.records = make(map[string]*store.Object)
	return records
}

// NOTE: Test help function for watching runtime status.
func (c *Collector) printMockdata() {
	fmt.Println("=======================COLLECTOR==========================")
	for key, r := range c.records {
		avgColdStart := avg(r.Coldstart)
		for _, path := range r.Paths {
			avgExec := avg(path.Exec)
			fmt.Printf("upstream: %v \n", path.From)
			fmt.Printf("flow: %v \n", key)
			fmt.Printf("coldstart: %v \n", r.Coldstart)
			fmt.Printf("exec: %v \n", path.Exec)
			fmt.Println(key, "avg exec:", avgExec)
		}
		fmt.Println(key, "avg coldstart:", avgColdStart)
	}
	fmt.Println("==========================================================")
}

func avg(sli []time.Duration) time.Duration {
	if len(sli) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range sli {
		sum += d
	}
	return sum / time.Duration(len(sli))
}
