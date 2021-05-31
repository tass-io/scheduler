package trace

import (
	"net/http"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/viper"
	"github.com/tass-io/scheduler/pkg/env"
	"github.com/uber/jaeger-client-go"
	tracer_config "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
)

func TraceInit() {
	cfg := &tracer_config.Configuration{}
	cfg.Sampler = &tracer_config.SamplerConfig{
		Type:  jaeger.SamplerTypeConst,
		Param: 1.0,
	}
	zap.S().Infow("use jaeger agent host and port", "HostAndPort", viper.GetString(env.TraceAgentHostPort))
	cfg.Reporter = &tracer_config.ReporterConfig{
		QueueSize:           100,
		BufferFlushInterval: 1 * time.Millisecond,
		LogSpans:            false,
		LocalAgentHostPort:  viper.GetString(env.TraceAgentHostPort),
	}

	_, err := cfg.InitGlobalTracer("tass") // closer ignore here, Assuming it doesn't close until the pod gets killed
	if err != nil {
		panic(err)
	}
}

func GetSpanContextFromHeaders(workflowName string, header http.Header) (opentracing.SpanContext, error) {
	carrier := opentracing.HTTPHeadersCarrier(header)
	return opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier)
}
