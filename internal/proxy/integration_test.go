package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// ---------------------------------------------------------------------------
// Fake upstream MCP SSE server
// ---------------------------------------------------------------------------

// fakeUpstream is a minimal in-process MCP SSE server.
// GET /sse: sends event:endpoint then streams pushed messages.
// POST /message: receives a JSON-RPC request and pushes the response via SSE.
type fakeUpstream struct {
	pushCh chan string // SSE data payloads pushed by handlePost

	// endpointPath is advertised via event:endpoint. Defaults to /message.
	endpointPath string

	mu         sync.Mutex
	lastHeader http.Header // headers of the most recent POST
}

func newFakeUpstream() *fakeUpstream {
	return &fakeUpstream{pushCh: make(chan string, 16), endpointPath: "/message"}
}

func (f *fakeUpstream) lastPostHeader() http.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastHeader
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		f.serveSSE(w, r)
	case http.MethodPost:
		f.servePost(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeUpstream) serveSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	// Advertise the POST endpoint.
	fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", f.endpointPath)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-f.pushCh:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (f *fakeUpstream) servePost(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.lastHeader = r.Header.Clone()
	f.mu.Unlock()

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
	case req.Method == "tools/call" && ToolCallParams(req.Params) == "big_tool":
		// A tool result far larger than bufio.Scanner's 64KB default token cap.
		result, _ = json.Marshal(map[string]any{"content": []map[string]string{{"text": strings.Repeat("x", bigPayloadSize)}}})
	default:
		result = json.RawMessage(`{"result":"ok"}`)
	}

	resp := RPCResponse{JSONRPC: "2.0", ID: req.ID, Result: result}
	data, _ := json.Marshal(resp)
	f.pushCh <- string(data)
	w.WriteHeader(http.StatusAccepted)
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

type testHarness struct {
	up       *fakeUpstream
	proxySrv *httptest.Server
	exporter *tracetest.InMemoryExporter
	tp       *sdktrace.TracerProvider
	cancel   context.CancelFunc // cancels SSE subscription
	sseCh    <-chan string      // receives SSE data values (post-strip)
}

func newTestHarness(t *testing.T, filter *Filter) *testHarness {
	return newTestHarnessWith(t, filter, newFakeUpstream())
}

func newTestHarnessWith(t *testing.T, filter *Filter, up *fakeUpstream) *testHarness {
	t.Helper()

	upSrv := httptest.NewServer(up)

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	tracer := tp.Tracer("integration-test")

	p, err := New(upSrv.URL, filter, tracer, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(p)

	t.Cleanup(func() {
		proxySrv.Close()
		upSrv.Close()
		_ = tp.Shutdown(context.Background())
	})

	// Open SSE connection to the proxy.
	ctx, cancel := context.WithCancel(context.Background())
	sseCh := openSSE(t, ctx, proxySrv.URL+"/sse")

	h := &testHarness{
		up:       up,
		proxySrv: proxySrv,
		exporter: exp,
		tp:       tp,
		cancel:   cancel,
		sseCh:    sseCh,
	}

	// Wait until the proxy has discovered the upstream POST endpoint.
	h.waitForData(t, up.endpointPath, 3*time.Second)
	return h
}

// openSSE connects to url via GET and returns a channel of SSE data values.
// The goroutine terminates when ctx is cancelled or the stream closes.
func openSSE(t *testing.T, ctx context.Context, url string) <-chan string {
	t.Helper()
	ch := make(chan string, 32)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("SSE connect: %v", err)
	}
	go func() {
		defer resp.Body.Close()
		defer close(ch)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") {
				val := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				select {
				case ch <- val:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch
}

// waitForData reads from sseCh until a data value equal to want is found.
func (h *testHarness) waitForData(t *testing.T, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case got, ok := <-h.sseCh:
			if !ok {
				t.Fatal("SSE stream closed unexpectedly")
			}
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for SSE data %q", want)
		}
	}
}

// waitForSSEResponse blocks until one SSE data line arrives (any value).
func (h *testHarness) waitForSSEResponse(t *testing.T) {
	t.Helper()
	select {
	case _, ok := <-h.sseCh:
		if !ok {
			t.Fatal("SSE stream closed before response")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for SSE response")
	}
	// Small pause: the span is ended by the proxy immediately after it forwards
	// the SSE line, so we yield to let that goroutine complete.
	time.Sleep(20 * time.Millisecond)
}

// recvSSE returns the next SSE data value, failing on timeout.
func (h *testHarness) recvSSE(t *testing.T) string {
	t.Helper()
	select {
	case v, ok := <-h.sseCh:
		if !ok {
			t.Fatal("SSE stream closed before response")
		}
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SSE response")
		return ""
	}
}

// post sends a JSON-RPC request body to the proxy.
func (h *testHarness) post(t *testing.T, body string) {
	t.Helper()
	resp, err := http.Post(h.proxySrv.URL+"/message", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()
}

// spans returns exported spans, resetting the exporter.
func (h *testHarness) spans() tracetest.SpanStubs {
	stubs := h.exporter.GetSpans()
	h.exporter.Reset()
	return stubs
}

// ---------------------------------------------------------------------------
// Attribute assertion helper
// ---------------------------------------------------------------------------

func requireAttr(t *testing.T, span tracetest.SpanStub, key, want string) {
	t.Helper()
	for _, a := range span.Attributes {
		if a.Key == attribute.Key(key) {
			if got := a.Value.AsString(); got != want {
				t.Errorf("span attr %q = %q, want %q", key, got, want)
			}
			return
		}
	}
	t.Errorf("span missing attribute %q (wanted %q)", key, want)
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

// TestIntegration_ToolsCallProducesSpan verifies that a tools/call request
// through the proxy results in exactly one span with correct attributes.
func TestIntegration_ToolsCallProducesSpan(t *testing.T) {
	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"my_tool"}}`)
	h.waitForSSEResponse(t)

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

// TestIntegration_NonToolsCallNoSpan verifies that non-tools/call methods
// produce no span when TraceAll is not set.
func TestIntegration_NonToolsCallNoSpan(t *testing.T) {
	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"2","method":"tools/list"}`)
	h.waitForSSEResponse(t)

	if spans := h.spans(); len(spans) != 0 {
		t.Fatalf("expected 0 spans for tools/list, got %d", len(spans))
	}
}

// TestIntegration_LifecycleMethodNoSpanWithoutFlag verifies that lifecycle
// methods (initialize) produce no span unless IncludeLifecycle is set.
func TestIntegration_LifecycleMethodNoSpanWithoutFlag(t *testing.T) {
	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"3","method":"initialize"}`)
	h.waitForSSEResponse(t)

	if spans := h.spans(); len(spans) != 0 {
		t.Fatalf("expected 0 spans for initialize without IncludeLifecycle, got %d", len(spans))
	}
}

// TestIntegration_ErrorResponseSetsErrorStatus verifies that a tool error
// response sets mcp.tool.status = "error" on the span.
func TestIntegration_ErrorResponseSetsErrorStatus(t *testing.T) {
	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"4","method":"tools/call","params":{"name":"fail_tool"}}`)
	h.waitForSSEResponse(t)

	spans := h.spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	requireAttr(t, spans[0], "mcp.tool.status", "error")
}
