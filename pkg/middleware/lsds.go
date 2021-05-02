package middleware

import "github.com/tass-io/scheduler/pkg/span"

const (
	LSDSMiddlewareSource Source = "LSDS"
)

var lsdsmiddle *LSDSMiddleware

func init() {
	lsdsmiddle = NewLSDSMiddleware()
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

func (lsds *LSDSMiddleware) Handle(body map[string]interface{}, sp *span.Span) (map[string]interface{}, Decision) {
	return nil, Next
}

func (lsds *LSDSMiddleware) GetSource() Source {
	return LSDSMiddlewareSource
}
