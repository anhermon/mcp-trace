package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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
	EndSpan(span, EndAttrs{DurationMS: 12.5, Status: StatusOK, ToolName: "read_file"})
}

func TestEndSpan_OK_NonTool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, EndAttrs{DurationMS: 3.0, Status: StatusOK})
}

func TestEndSpan_Error_Tool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, EndAttrs{DurationMS: 5.0, Status: StatusError, ErrMsg: "upstream error", ToolName: "read_file"})
}

func TestEndSpan_Error_NonTool(t *testing.T) {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	EndSpan(span, EndAttrs{Status: StatusError, ErrMsg: "upstream error"})
}

// TestTimeoutSpan_MatchesDocumentedSchema pins the README's claim that
// mcp.duration_ms is present on *all* spans — timeout spans used to omit it
// (and mcp.tool.duration_ms) entirely.
func TestTimeoutSpan_MatchesDocumentedSchema(t *testing.T) {
	for _, tc := range []struct {
		name     string
		toolName string
		want     []string
	}{
		{"tool", "read_file", []string{"mcp.duration_ms", "mcp.tool.duration_ms", "mcp.status", "mcp.tool.status", "error", "error.message"}},
		{"non-tool", "", []string{"mcp.duration_ms", "mcp.status", "error", "error.message"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
			_, span := tp.Tracer("test").Start(context.Background(), "test")

			EndSpan(span, EndAttrs{
				DurationMS: 42.5,
				Status:     StatusTimeout,
				ErrMsg:     "no response received within the span deadline",
				ToolName:   tc.toolName,
			})

			stubs := exp.GetSpans()
			if len(stubs) != 1 {
				t.Fatalf("expected 1 exported span, got %d", len(stubs))
			}
			got := map[attribute.Key]attribute.Value{}
			for _, a := range stubs[0].Attributes {
				got[a.Key] = a.Value
			}
			for _, key := range tc.want {
				if _, ok := got[attribute.Key(key)]; !ok {
					t.Errorf("timeout span missing documented attribute %q", key)
				}
			}
			if d := got["mcp.duration_ms"].AsFloat64(); d != 42.5 {
				t.Errorf("mcp.duration_ms = %v, want 42.5", d)
			}
			if tc.toolName == "" {
				if _, ok := got["mcp.tool.duration_ms"]; ok {
					t.Error("non-tool span must not carry mcp.tool.duration_ms")
				}
			}
		})
	}
}
