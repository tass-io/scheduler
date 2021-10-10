package collector

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
)

var collector *Collector

// Collector is responsible for collecting metrics for different functions.
type Collector struct {
	ch         chan *record
	fnsMetrics map[string]metrics
}

// RecordType is the type of a record, which indicates the different phases of a function.
type RecordType string

// RecordType enum
const (
	RecordColdStart RecordType = "coldstart"
	RecordExec      RecordType = "exec"
)

type metrics struct {
	coldstart []time.Duration
	exec      []time.Duration
}

type record struct {
	fn string
	t  RecordType
	d  time.Duration
}

// Init initializes the Collector which is responsible for collecting metrics for different functions.
// Collector is a singleton.
func Init() {
	collector = &Collector{
		ch:         make(chan *record, 100),
		fnsMetrics: make(map[string]metrics),
	}
}

// GetCollector returns the Collector singleton.
func GetCollector() *Collector {
	return collector
}

// Record records one specific phase time cost of a function.
// TODO: we may need add Flow name s a parameter.
func (c *Collector) Record(f string, t RecordType, d time.Duration) {
	if viper.GetBool(env.Collector) {
		c.ch <- &record{
			fn: f,
			t:  t,
			d:  d,
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
	for r := range c.ch {
		m := c.fnsMetrics[r.fn]
		switch r.t {
		case RecordColdStart:
			m.coldstart = append(m.coldstart, time.Duration(r.d))
		case RecordExec:
			m.exec = append(m.exec, time.Duration(r.d))
		default:
			panic("unknown record type")
		}
		c.fnsMetrics[r.fn] = m
	}
}

// publish sends thr current metrics to the prediction model
func (c *Collector) publish() {
	for {
		fmt.Println("=======================COLLECTOR==========================")
		for fn, m := range c.fnsMetrics {
			avgColdStart := avg(m.coldstart)
			avgExec := avg(m.exec)
			fmt.Printf("function: %v \n", fn)
			fmt.Printf("coldstart: %v \n", m.coldstart)
			fmt.Printf("exec: %v \n", m.exec)
			fmt.Println(fn, "avg coldstart:", avgColdStart, "avg exec:", avgExec)
		}
		fmt.Println("==========================================================")
		time.Sleep(time.Second * 10)
	}
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
