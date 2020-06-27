package tracer

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaeger_prometheus "github.com/uber/jaeger-lib/metrics/prometheus"
)

// NewTracer ...
func NewTracer() (opentracing.Tracer, io.Closer, error) {
	conf := jaegercfg.Configuration{
		ServiceName: "qpu",
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	return conf.NewTracer(config.Metrics(jaeger_prometheus.New()))
}
