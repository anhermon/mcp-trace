package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// SpanAttrs holds the attributes captured at request time.
type SpanAttrs struct {
	Method    string
	ToolName  string // non-empty for tools/call
	RequestID string
	Target    string
}

// StartSpan starts a new span for an incoming JSON-RPC method.
// The span name follows the convention:
//   - tools/call: "mcp tools/call {tool_name}"
//   - other:      "mcp {method}"
func StartSpan(ctx context.Context, tracer trace.Tracer, attrs SpanAttrs) (context.Context, trace.Span) {
	name := spanName(attrs.Method, attrs.ToolName)

	spanAttrs := []attribute.KeyValue{
		attribute.String("mcp.method", attrs.Method),
		attribute.String("mcp.request.id", attrs.RequestID),
		attribute.String("mcp.server.target", attrs.Target),
	}
	if attrs.ToolName != "" {
		spanAttrs = append(spanAttrs, attribute.String("mcp.tool.name", attrs.ToolName))
	}

	return tracer.Start(ctx, name, trace.WithAttributes(spanAttrs...))
}

// EndSpan finalises a span with duration and status.
func EndSpan(span trace.Span, durationMS float64, isErr bool, errMsg string) {
	span.SetAttributes(
		attribute.Float64("mcp.tool.duration_ms", durationMS),
	)

	if isErr {
		span.SetAttributes(
			attribute.String("mcp.tool.status", "error"),
			attribute.Bool("error", true),
			attribute.String("error.message", errMsg),
		)
		span.SetStatus(codes.Error, errMsg)
	} else {
		span.SetAttributes(attribute.String("mcp.tool.status", "ok"))
		span.SetStatus(codes.Ok, "")
	}

	span.End()
}

// EndSpanTimeout marks a span as timed-out (treated as error).
func EndSpanTimeout(span trace.Span) {
	msg := "span timed out — no response received within deadline"
	span.SetAttributes(
		attribute.String("mcp.tool.status", "error"),
		attribute.Bool("error", true),
		attribute.String("error.message", msg),
	)
	span.SetStatus(codes.Error, msg)
	span.End()
}

func spanName(method, toolName string) string {
	if method == "tools/call" && toolName != "" {
		return fmt.Sprintf("mcp tools/call %s", toolName)
	}
	return fmt.Sprintf("mcp %s", method)
}
