package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Status values for mcp.status. A call that never got an answer is not the same
// failure as a tool that returned an error, and the difference decides where you
// look next — so it gets its own value rather than collapsing into "error".
const (
	StatusOK        = "ok"
	StatusError     = "error"     // upstream answered with a JSON-RPC or tool-level error
	StatusTimeout   = "timeout"   // no answer within the eviction deadline
	StatusAbandoned = "abandoned" // stream closed before the answer arrived
)

// SpanAttrs holds the attributes captured at request time.
type SpanAttrs struct {
	Method     string
	ToolName   string // non-empty for tools/call
	RequestID  string
	SessionID  string
	Target     string
	ArgKeys    string // comma-separated tool argument names
	ArgsJSON   string // full tool arguments; only set when capture is enabled
	ClientName string
	ClientVer  string
}

// EndAttrs holds what is only known once the response comes back.
type EndAttrs struct {
	DurationMS   float64
	Status       string // one of the Status* constants
	ErrMsg       string
	ErrCode      int // JSON-RPC error code; 0 when absent
	ToolName     string
	ResponseSize int
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
	optional := []struct {
		key, val string
	}{
		{"mcp.tool.name", attrs.ToolName},
		{"mcp.session.id", attrs.SessionID},
		{"mcp.tool.argument_keys", attrs.ArgKeys},
		{"mcp.tool.arguments", attrs.ArgsJSON},
		{"mcp.client.name", attrs.ClientName},
		{"mcp.client.version", attrs.ClientVer},
	}
	for _, o := range optional {
		if o.val != "" {
			spanAttrs = append(spanAttrs, attribute.String(o.key, o.val))
		}
	}

	// Client kind: the proxy is calling out to the upstream MCP server.
	return tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(spanAttrs...))
}

// EndSpan finalises a span with duration and status.
// attrs.ToolName must be non-empty only for tools/call spans; it gates the
// mcp.tool.* attributes.
func EndSpan(span trace.Span, attrs EndAttrs) {
	status := attrs.Status
	if status == "" {
		status = StatusOK
	}

	spanAttrs := []attribute.KeyValue{
		attribute.Float64("mcp.duration_ms", attrs.DurationMS),
		attribute.String("mcp.status", status),
	}
	if attrs.ToolName != "" {
		spanAttrs = append(spanAttrs,
			attribute.Float64("mcp.tool.duration_ms", attrs.DurationMS),
			attribute.String("mcp.tool.status", status))
	}
	if attrs.ResponseSize > 0 {
		spanAttrs = append(spanAttrs, attribute.Int("mcp.response.size_bytes", attrs.ResponseSize))
	}
	if attrs.ErrCode != 0 {
		spanAttrs = append(spanAttrs, attribute.Int("mcp.rpc.error_code", attrs.ErrCode))
	}
	if status != StatusOK {
		spanAttrs = append(spanAttrs,
			attribute.Bool("error", true),
			attribute.String("error.message", attrs.ErrMsg))
	}
	span.SetAttributes(spanAttrs...)

	if status == StatusOK {
		span.SetStatus(codes.Ok, "")
	} else {
		span.SetStatus(codes.Error, attrs.ErrMsg)
	}
	span.End()
}

func spanName(method, toolName string) string {
	if method == "tools/call" && toolName != "" {
		return fmt.Sprintf("mcp tools/call %s", toolName)
	}
	return fmt.Sprintf("mcp %s", method)
}
