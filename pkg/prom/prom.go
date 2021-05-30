package prom

import "github.com/prometheus/client_golang/prometheus"

var Demo = prometheus.NewGauge(prometheus.GaugeOpts{
	Name:        "demo",
	Help:        "demo var for tass",
	ConstLabels: nil,
})

// run before route define, now at root.go
func init() {
	_ = prometheus.Register(Demo)
}
