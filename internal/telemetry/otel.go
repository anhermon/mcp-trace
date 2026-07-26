package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Provider wraps the OTel TracerProvider with a shutdown function.
type Provider struct {
	tp     *sdktrace.TracerProvider
	Tracer trace.Tracer
}

// Shutdown flushes and stops the provider. Should be deferred by callers.
func (p *Provider) Shutdown(ctx context.Context) error {
	return p.tp.Shutdown(ctx)
}

// Config holds the exporter settings needed to initialise the provider.
type Config struct {
	// UseHTTP switches from gRPC to HTTP OTLP exporter.
	UseHTTP bool
	// GRPCEndpoint is the gRPC OTLP endpoint (host:port, no scheme).
	GRPCEndpoint string
	// HTTPEndpoint is the HTTP OTLP endpoint (full URL).
	HTTPEndpoint string
	// Insecure disables TLS verification.
	Insecure bool
	// ServiceName is set as resource.service.name.
	ServiceName string
	// Logger receives asynchronous OTel export errors. Optional; defaults to slog.Default.
	Logger *slog.Logger
}

// New initialises an OTel TracerProvider from cfg.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	var (
		exp sdktrace.SpanExporter
		err error
	)

	// Span export happens asynchronously in the batcher, so failures are
	// otherwise completely silent. Surface them on the application logger.
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("otel error", "err", err)
	}))

	if cfg.UseHTTP {
		exp, err = newHTTPExporter(ctx, cfg)
	} else {
		exp, err = newGRPCExporter(ctx, cfg)
	}
	if err != nil {
		return nil, fmt.Errorf("creating OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OTel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	// Default propagator is a no-op; without this, traceparent injection is silently dead.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return &Provider{
		tp:     tp,
		Tracer: tp.Tracer("mcp-trace"),
	}, nil
}

func newGRPCExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.GRPCEndpoint),
	}
	if cfg.Insecure {
		// Must be WithInsecure, not WithDialOption(grpc.WithTransportCredentials(...)):
		// the exporter appends its own TLS credentials after user dial options, and the
		// last WithTransportCredentials wins — so a dial option here is silently overridden.
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return otlptracegrpc.New(ctx, opts...)
}

func newHTTPExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(cfg.HTTPEndpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	return otlptracehttp.New(ctx, opts...)
}
