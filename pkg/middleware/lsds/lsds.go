package lsds

import (
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
)

const (
	LSDSMiddlewareSource middleware.Source = "LSDS"
)

var lsdsmiddle *LSDSMiddleware

func init() {
	lsdsmiddle = NewLSDSMiddleware()
	middleware.Register(LSDSMiddlewareSource, lsdsmiddle, 1)
}

// LSDSMiddleware will check fs status and use some policy to handle request, which make the request have chance to redirect to other Local Scheduler
type LSDSMiddleware struct {
}

func NewLSDSMiddleware() *LSDSMiddleware {
	return &LSDSMiddleware{}
}

func GetLSDSMiddleware() *LSDSMiddleware {
	return lsdsmiddle
}

func (lsds *LSDSMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, middleware.Decision) {
	return nil, middleware.Next
}

func (lsds *LSDSMiddleware) GetSource() middleware.Source {
	return LSDSMiddlewareSource
}
