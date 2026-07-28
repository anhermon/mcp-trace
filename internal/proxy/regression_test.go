package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// bigPayloadSize is comfortably past bufio.Scanner's 64KB default token cap.
const bigPayloadSize = 200 * 1024

// TestRegression_LargePayloadSurvivesSSE is the regression test for the 64KB
// truncation bug: a default bufio.Scanner made the data line vanish and killed
// the stream with "token too long", silently losing the tool result.
func TestRegression_LargePayloadSurvivesSSE(t *testing.T) {
	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"big","method":"tools/call","params":{"name":"big_tool"}}`)

	got := h.recvSSE(t)
	if len(got) < bigPayloadSize {
		t.Fatalf("SSE payload truncated: got %d bytes, want >= %d", len(got), bigPayloadSize)
	}
	if !strings.Contains(got, strings.Repeat("x", bigPayloadSize)) {
		t.Error("large tool result did not survive the proxy intact")
	}

	time.Sleep(50 * time.Millisecond)
	if spans := h.spans(); len(spans) != 1 {
		t.Fatalf("expected 1 span for the large tool call, got %d", len(spans))
	}
}

// TestRegression_PerSessionPostRouting is the regression test for the shared
// postEndpoint field. A single proxy serving two concurrent SSE sessions used
// to keep one global endpoint, so whichever session connected last captured
// the other's POSTs. Both sessions run through ONE proxy here — that is the
// only shape in which the original bug reproduces.
func TestRegression_PerSessionPostRouting(t *testing.T) {
	up := newSessionUpstream()
	upSrv := httptest.NewServer(up)
	defer upSrv.Close()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	proxyCtx, proxyCancel := context.WithCancel(context.Background())
	defer proxyCancel()
	p, err := New(proxyCtx, upSrv.URL, &Filter{}, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	// Two sessions on the SAME proxy. "bbb" connects last.
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	chA := openSSE(t, ctxA, proxySrv.URL+"/sse?want=aaa")
	waitFor(t, chA, "/messages/?session_id=aaa")

	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	chB := openSSE(t, ctxB, proxySrv.URL+"/sse?want=bbb")
	waitFor(t, chB, "/messages/?session_id=bbb")

	// POST belonging to session A. With a shared field this went to bbb.
	resp, err := http.Post(proxySrv.URL+"/messages/?session_id=aaa", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"t"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	select {
	case got := <-up.posted:
		if !strings.Contains(got, "session_id=aaa") {
			t.Errorf("session A's POST was routed to %q, want the session_id=aaa endpoint", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never received the POST")
	}
}

// TestRegression_StaleSpanEvictedWithoutSSEStream is the regression test for the
// leaked in-flight span: the eviction ticker used to be created inside
// handleSSE, so a request with no SSE stream open (or one in flight when a
// stream dropped) was never ended, never exported, and never freed from reqMap.
// Nothing in this test ever opens an SSE stream — on the old code it hangs.
func TestRegression_StaleSpanEvictedWithoutSSEStream(t *testing.T) {
	// Upstream that accepts the POST and then simply never answers.
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upSrv.Close()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := newWithEviction(ctx, upSrv.URL, &Filter{}, tp.Tracer("t"),
		slog.New(slog.NewTextHandler(io.Discard, nil)), 5*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	resp, err := http.Post(proxySrv.URL+"/message", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":"stale","method":"tools/call","params":{"name":"slow_tool"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	var spans tracetest.SpanStubs
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if spans = exp.GetSpans(); len(spans) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(spans) != 1 {
		t.Fatalf("stale span was never evicted or exported: got %d spans, want 1", len(spans))
	}

	attrs := map[attribute.Key]attribute.Value{}
	for _, a := range spans[0].Attributes {
		attrs[a.Key] = a.Value
	}
	// Timeout spans must carry the documented duration attributes too.
	for _, key := range []string{"mcp.duration_ms", "mcp.tool.duration_ms", "mcp.status", "mcp.tool.status"} {
		if _, ok := attrs[attribute.Key(key)]; !ok {
			t.Errorf("evicted span missing documented attribute %q", key)
		}
	}
	if got := attrs["mcp.status"].AsString(); got != "error" {
		t.Errorf("mcp.status = %q, want %q", got, "error")
	}

	// The map entry must be freed, not just the span ended.
	if p.reqMap.Take("stale") != nil {
		t.Error("evicted request is still in the request map")
	}
}

// TestRegression_CamelCaseSessionIDFallsBackToPathReconstruction covers the
// untested fallback: sessionIDOf only recognises the literal query key
// session_id, so a server advertising sessionId registers no session and the
// POST must still reach upstream via path reconstruction.
func TestRegression_CamelCaseSessionIDFallsBackToPathReconstruction(t *testing.T) {
	up := newSessionUpstream()
	up.queryKey = "sessionId"
	upSrv := httptest.NewServer(up)
	defer upSrv.Close()

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := New(ctx, upSrv.URL, &Filter{}, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	sseCtx, sseCancel := context.WithCancel(context.Background())
	defer sseCancel()
	ch := openSSE(t, sseCtx, proxySrv.URL+"/sse?want=aaa")
	waitFor(t, ch, "/messages/?sessionId=aaa")

	// No session was registered, so the proxy must rebuild the upstream URL from
	// the incoming path and query.
	resp, err := http.Post(proxySrv.URL+"/messages/?sessionId=aaa", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"t"}}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	select {
	case got := <-up.posted:
		if got != "/messages/?sessionId=aaa" {
			t.Errorf("POST landed at %q, want %q", got, "/messages/?sessionId=aaa")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never received the POST via path reconstruction")
	}
}

// sessionUpstream advertises a distinct per-session endpoint, like a real MCP server.
type sessionUpstream struct {
	posted   chan string // request URIs of POSTs received
	queryKey string      // session id query key it advertises
}

func newSessionUpstream() *sessionUpstream {
	return &sessionUpstream{posted: make(chan string, 8), queryKey: "session_id"}
}

func (s *sessionUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		s.posted <- r.URL.RequestURI()
		w.WriteHeader(http.StatusAccepted)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "event: endpoint\ndata: /messages/?%s=%s\n\n", s.queryKey, r.URL.Query().Get("want"))
	flusher.Flush()
	<-r.Context().Done()
}

// waitFor blocks until ch yields want.
func waitFor(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got, ok := <-ch:
			if !ok {
				t.Fatalf("SSE stream closed before %q", want)
			}
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}

// TestRegression_TraceparentInjectedUpstream verifies the proxy propagates
// trace context rather than emitting orphan spans.
func TestRegression_TraceparentInjectedUpstream(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	h := newTestHarness(t, &Filter{})
	defer h.cancel()

	h.post(t, `{"jsonrpc":"2.0","id":"tp","method":"tools/call","params":{"name":"my_tool"}}`)
	h.waitForSSEResponse(t)

	tp := h.up.lastPostHeader().Get("traceparent")
	if tp == "" {
		t.Fatal("no traceparent injected into the upstream request")
	}

	spans := h.spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].SpanKind; got != trace.SpanKindClient {
		t.Errorf("span kind = %v, want %v", got, trace.SpanKindClient)
	}
	// The injected traceparent must carry this span's trace id.
	if !strings.Contains(tp, spans[0].SpanContext.TraceID().String()) {
		t.Errorf("traceparent %q does not carry span trace id %s", tp, spans[0].SpanContext.TraceID())
	}
}
