package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace/noop"
)

// spanName is unexported; we test it directly from within the package.

func TestSpanName_ToolCall(t *testing.T) {
	got := spanName("tools/call", "read_file")
	want := "mcp tools/call read_file"
	if got != want {
		t.Errorf("spanName = %q; want %q", got, want)
	}
}

func TestSpanName_ToolCall_EmptyTool(t *testing.T) {
	// tools/call without a resolved tool name falls back to plain method.
	got := spanName("tools/call", "")
	want := "mcp tools/call"
	if got != want {
		t.Errorf("spanName = %q; want %q", got, want)
	}
}

func TestSpanName_Other(t *testing.T) {
	got := spanName("tools/list", "")
	want := "mcp tools/list"
	if got != want {
		t.Errorf("spanName = %q; want %q", got, want)
	}
}

func TestStartSpan_ToolCall(t *testing.T) {
	tr := noop.NewTracerProvider().Tracer("test")
	ctx, span := StartSpan(context.Background(), tr, SpanAttrs{
		Method:    "tools/call",
		ToolName:  "read_file",
		RequestID: "42",
		Target:    "http://localhost:8000",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	if span == nil {
		t.Fatal("expected non-nil span")
	}
	span.End()
}

func TestStartSpan_NonTool(t *testing.T) {
	tr := noop.NewTracerProvider().Tracer("test")
	ctx, span := StartSpan(context.Background(), tr, SpanAttrs{
		Method:    "tools/list",
		RequestID: "7",
		Target:    "http://localhost:8000",
	})
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
	if span == nil {
		t.Fatal("expected non-nil span")
	}
	span.End()
}

func TestEndSpan_OK_Tool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	// Must not panic; noop span discards attributes silently.
	EndSpan(span, 12.5, false, "", "read_file")
}

func TestEndSpan_OK_NonTool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, 3.0, false, "", "")
}

func TestEndSpan_Error_Tool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, 5.0, true, "upstream error", "read_file")
}

func TestEndSpan_Error_NonTool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, 0, true, "upstream error", "")
}

func TestEndSpanTimeout_Tool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpanTimeout(span, "read_file")
}

func TestEndSpanTimeout_NonTool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpanTimeout(span, "")
}
