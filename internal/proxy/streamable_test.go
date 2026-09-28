package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// fakeStreamableUpstream is a minimal in-process MCP Streamable HTTP server.
// POST endpoint: receives JSON-RPC and returns the response directly on the POST.
type fakeStreamableUpstream struct {
	lastHeader http.Header
}

func newFakeStreamableUpstream() *fakeStreamableUpstream {
	return &fakeStreamableUpstream{}
}

func (f *fakeStreamableUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	f.lastHeader = r.Header.Clone()

	var req RPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Return a tool error for "fail_tool"; success for everything else.
	var result json.RawMessage
	switch {
	case req.Method == "tools/call" && ToolCallParams(req.Params) == "fail_tool":
		result = json.RawMessage(`{"isError":true,"content":[{"text":"tool execution failed"}]}`)
	default:
		result = json.RawMessage(`{"result":"ok"}`)
	}

	resp := RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
	data, _ := json.Marshal(resp)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

type streamableTestHarness struct {
	up       *fakeStreamableUpstream
	proxySrv *httptest.Server
	exporter *tracetest.InMemoryExporter
	tp       *sdktrace.TracerProvider
}

func newStreamableTestHarness(t *testing.T, filter *Filter) *streamableTestHarness {
	t.Helper()

	up := newFakeStreamableUpstream()
	upSrv := httptest.NewServer(up)

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	tracer := tp.Tracer("streamable-test")

	ctx := context.Background()
	p, err := NewStreamableProxy(ctx, upSrv.URL, filter, tracer, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(p)

	t.Cleanup(func() {
		proxySrv.Close()
		upSrv.Close()
		_ = tp.Shutdown(context.Background())
	})

	return &streamableTestHarness{
		up:       up,
		proxySrv: proxySrv,
		exporter: exp,
		tp:       tp,
	}
}

func (h *streamableTestHarness) post(t *testing.T, body string) string {
	t.Helper()
	resp, err := http.Post(h.proxySrv.URL, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}

	// Small pause to let span finalize
	time.Sleep(20 * time.Millisecond)

	return string(data)
}

func (h *streamableTestHarness) spans() tracetest.SpanStubs {
	stubs := h.exporter.GetSpans()
	h.exporter.Reset()
	return stubs
}

func TestStreamable_ToolsCallProducesSpan(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})

	respData := h.post(t, `{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"my_tool"}}`)

	// Verify response
	var resp RPCResponse
	if err := json.Unmarshal([]byte(respData), &resp); err != nil {
		t.Fatalf("parsing response: %v", err)
	}
	if resp.ID == nil {
		t.Fatal("response missing ID")
	}

	spans := h.spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]
	if s.Name != "mcp tools/call my_tool" {
		t.Errorf("span name = %q, want %q", s.Name, "mcp tools/call my_tool")
	}
	requireAttr(t, s, "mcp.method", "tools/call")
	requireAttr(t, s, "mcp.tool.name", "my_tool")
	requireAttr(t, s, "mcp.tool.status", "ok")
}

func TestStreamable_ErrorResponseSetsErrorStatus(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})

	h.post(t, `{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"fail_tool"}}`)

	spans := h.spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	requireAttr(t, spans[0], "mcp.tool.status", "error")
}

func TestStreamable_NonToolsCallNoSpan(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})

	h.post(t, `{"jsonrpc":"2.0","id":"3","method":"tools/list"}`)

	if spans := h.spans(); len(spans) != 0 {
		t.Fatalf("expected 0 spans for tools/list, got %d", len(spans))
	}
}

func TestStreamable_InitializeRemembersClient(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{TraceAll: true, IncludeLifecycle: true})

	// Send initialize
	h.post(t, `{"jsonrpc":"2.0","id":"1","method":"initialize","params":{"clientInfo":{"name":"test-client","version":"1.0"}}}`)

	// Send tool call
	h.post(t, `{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"test_tool"}}`)

	spans := h.spans()
	if len(spans) < 1 {
		t.Fatalf("expected at least 1 span, got %d", len(spans))
	}

	// Find the tool call span
	var toolSpan *tracetest.SpanStub
	for i := range spans {
		if strings.Contains(spans[i].Name, "tools/call") {
			toolSpan = &spans[i]
			break
		}
	}

	if toolSpan == nil {
		t.Fatal("no tool call span found")
	}

	// Tool call span should have client info
	requireAttr(t, *toolSpan, "mcp.client.name", "test-client")
	requireAttr(t, *toolSpan, "mcp.client.version", "1.0")
}

func TestStreamable_OnlyPostAllowed(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})

	resp, err := http.Get(h.proxySrv.URL)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestStreamable_TraceparentInjected(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})

	// Send a request with traceparent
	req, _ := http.NewRequest(http.MethodPost, h.proxySrv.URL, strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"test"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()
	io.ReadAll(resp.Body)

	time.Sleep(20 * time.Millisecond)

	// Check that upstream received a traceparent header
	if h.up.lastHeader.Get("traceparent") == "" {
		t.Error("upstream did not receive traceparent header")
	}
}
