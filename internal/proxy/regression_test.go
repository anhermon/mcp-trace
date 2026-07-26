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
	p, err := New(upSrv.URL, &Filter{}, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
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

// sessionUpstream advertises a distinct per-session endpoint, like a real MCP server.
type sessionUpstream struct {
	posted chan string // request URIs of POSTs received
}

func newSessionUpstream() *sessionUpstream {
	return &sessionUpstream{posted: make(chan string, 8)}
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
	fmt.Fprintf(w, "event: endpoint\ndata: /messages/?session_id=%s\n\n", r.URL.Query().Get("want"))
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
