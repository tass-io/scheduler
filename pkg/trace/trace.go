package trace

import (
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

const (
	TraceToken = "tass"
)

// Init initializes a jaeger client
func Init() {
	cfg := &jaegercfg.Configuration{}

	zap.S().Infow("use jaeger agent host and port", "HostAndPort", viper.GetString(env.TraceAgentHostPort))
	cfg.Reporter = &jaegercfg.ReporterConfig{
		QueueSize:           100,
		BufferFlushInterval: 1 * time.Millisecond,
		LogSpans:            false,
		LocalAgentHostPort:  viper.GetString(env.TraceAgentHostPort),
	}
	cfg.Sampler = &jaegercfg.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: 1.0,
	}

	// closer ignore here, Assuming it doesn't close until the pod gets killed
	_, err := cfg.InitGlobalTracer(TraceToken)
	if err != nil {
		zap.S().Panic(err)
	}
}

// GetSpanContextFromHeaders extracts the span context from http header.
// If no span context is set in the request, it will return a "ErrSpanContextNotFound" error
func GetSpanContextFromHeaders(workflowName string, header http.Header) (opentracing.SpanContext, error) {
	carrier := opentracing.HTTPHeadersCarrier(header)
	return opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier)
}
