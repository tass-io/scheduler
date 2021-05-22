package main

import (
	"fmt"

	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
)

const (
	DemoMiddlewareSource middleware.Source = "Demo"
)

var (
	demomiddle *DemoMiddleware
)

func init() {
	demomiddle = newDemoMiddleware()
	register()
}

// register for the first/second time inject, if you think this middleware is a common one, register at init()
func register() {
	fmt.Println("demo register")
	middleware.Register(DemoMiddlewareSource, demomiddle, 0)
}

type DemoMiddleware struct {
}

func newDemoMiddleware() *DemoMiddleware {
	return &DemoMiddleware{}
}

func (demo *DemoMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, middleware.Decision, error) {
	return map[string]interface{}{"demo": "demo"}, middleware.Abort, nil
}

func (demo *DemoMiddleware) GetSource() middleware.Source {
	return DemoMiddlewareSource
}
