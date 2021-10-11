package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"go.uber.org/zap"
)

var collector *Collector

// Collector is responsible for collecting metrics for different functions.
type Collector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	wf      string
	ch      chan *record
	metrcis map[string]metric
}

// RecordType is the type of a record, which indicates the different phases of a function.
type RecordType string

// RecordType enum
const (
	RecordColdStart RecordType = "coldstart"
	RecordExec      RecordType = "exec"
)

type metric struct {
	coldstart []time.Duration
	exec      []time.Duration
}

type record struct {
	flow string
	fn   string
	t    RecordType
	d    time.Duration
}

// Init initializes the Collector which is responsible for collecting metrics for different functions.
// Collector is a singleton, if workflow changes, it will be recreated.
func Init(workflow string) {
	if collector == nil {
		newCollector(workflow)
		return
	}
	if collector.wf != workflow {
		zap.S().Info("workflow changes, generate a new workflow collector")
		collector.cancel()
		newCollector(workflow)
	}
}

func newCollector(workflow string) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	collector = &Collector{
		ctx:     ctx,
		cancel:  cancel,
		wf:      workflow,
		ch:      make(chan *record, 100),
		metrcis: make(map[string]metric),
	}
}

// GetCollector returns the Collector singleton.
func GetCollector() *Collector {
	return collector
}

// Record records one specific phase time cost of a function.
// TODO: we may need add Flow name s a parameter.
func (c *Collector) Record(flow, fn string, t RecordType, d time.Duration) {
	if viper.GetBool(env.Collector) {
		c.ch <- &record{
			flow: flow,
			fn:   fn,
			t:    t,
			d:    d,
		}
	}
}

func (c *Collector) Start() {
	if viper.GetBool(env.Collector) {
		go c.publish()
		go c.startCollector()
	}
}

func (c *Collector) startCollector() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case r := <-c.ch:
			key := fmt.Sprintf("%v/%v", r.flow, r.fn)
			m := c.metrcis[key]
			switch r.t {
			case RecordColdStart:
				m.coldstart = append(m.coldstart, time.Duration(r.d))
			case RecordExec:
				m.exec = append(m.exec, time.Duration(r.d))
			default:
				panic("unknown record type")
			}
			c.metrcis[key] = m
		}
	}
}

// publish sends thr current metrics to the prediction model
func (c *Collector) publish() {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			c.printMockdata()
			time.Sleep(time.Second * 10)
		}
	}
}

// FIXME: delete this function once business logic is ready.
func (c *Collector) printMockdata() {
	fmt.Println("=======================COLLECTOR==========================")
	for key, m := range c.metrcis {
		avgColdStart := avg(m.coldstart)
		avgExec := avg(m.exec)
		fmt.Printf("function: %v \n", key)
		fmt.Printf("coldstart: %v \n", m.coldstart)
		fmt.Printf("exec: %v \n", m.exec)
		fmt.Println(key, "avg coldstart:", avgColdStart, "avg exec:", avgExec)
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
