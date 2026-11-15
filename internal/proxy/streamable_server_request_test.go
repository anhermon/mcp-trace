package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Regression tests for issue #24, found with the official MCP conformance
// suite (tools-call-sampling, tools-call-elicitation and the elicitation
// SEP scenarios timed out only through mcp-trace).
//
// A server that needs the client mid tools/call sends it a request
// (sampling/createMessage, elicitation/create) as an event on the open POST
// event stream, or on the GET stream, and blocks until the client answers on a
// separate POST. The proxy must deliver that event immediately, relay the
// answer upstream, and trace the round-trip.

const testSession = "sess-24"

// serverRequestUpstream is a Streamable HTTP server whose tools/call asks the
// client for something and only answers once the client has replied.
type serverRequestUpstream struct {
	method  string        // server-to-client method to send
	answers chan string   // client replies (JSON bodies) received on POST
	getOpen chan struct{} // closed once a GET stream is open (useGET only)
	useGET  bool          // send the server request on the GET stream
	getW    chan io.Writer
}

func newServerRequestUpstream(method string, useGET bool) *serverRequestUpstream {
	return &serverRequestUpstream{
		method:  method,
		answers: make(chan string, 1),
		getOpen: make(chan struct{}),
		useGET:  useGET,
		getW:    make(chan io.Writer, 1),
	}
}

func (u *serverRequestUpstream) sendEvent(w io.Writer, data string) {
	fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
	w.(http.Flusher).Flush()
}

func (u *serverRequestUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(mcpSessionHeader) != testSession {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		u.getW <- w
		close(u.getOpen)
		<-r.Context().Done()
		return
	case http.MethodPost:
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(r.Body)
	req, err := ParseRequest(body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Method == "" { // the client's answer to our request
		u.answers <- string(body)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
	serverReq := fmt.Sprintf(`{"jsonrpc":"2.0","id":7,"method":%q,"params":{}}`, u.method)
	if u.useGET {
		gw := <-u.getW
		u.sendEvent(gw, serverReq)
	} else {
		u.sendEvent(w, serverReq)
	}
	select {
	case answer := <-u.answers:
		u.sendEvent(w, fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":%q}]}}`, req.ID, answer))
	case <-time.After(3 * time.Second):
		// Never answered: end the stream without a result.
	case <-r.Context().Done():
	}
}

type srHarness struct {
	proxyURL string
	exporter *tracetest.InMemoryExporter
}

func newSRHarness(t *testing.T, up http.Handler, filter *Filter) *srHarness {
	t.Helper()
	upSrv := httptest.NewServer(up)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	p, err := NewStreamableProxy(context.Background(), upSrv.URL, filter, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ps := httptest.NewServer(p)
	t.Cleanup(func() {
		ps.Close()
		upSrv.Close()
		_ = tp.Shutdown(context.Background())
	})
	return &srHarness{proxyURL: ps.URL, exporter: exp}
}

func srPost(ctx context.Context, url, body string) (*http.Response, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(mcpSessionHeader, testSession)
	return http.DefaultClient.Do(req)
}

// readEvents sends every SSE data payload from r to out until r ends.
func readEvents(r io.Reader, out chan<- string) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
			out <- v
		}
	}
	close(out)
}

// callTool runs one tools/call (or untraced method) through the proxy and
// answers the server's request with answer. It returns the final result data.
func callTool(t *testing.T, h *srHarness, method, answer string, useGET bool) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	serverReqs := make(chan string, 4)
	if useGET {
		greq, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.proxyURL, nil)
		greq.Header.Set("Accept", "text/event-stream")
		greq.Header.Set(mcpSessionHeader, testSession)
		gresp, err := http.DefaultClient.Do(greq)
		if err != nil {
			t.Fatalf("GET stream through proxy: %v", err)
		}
		defer gresp.Body.Close()
		if ct := gresp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
			t.Fatalf("GET Content-Type = %q, want text/event-stream", ct)
		}
		go readEvents(gresp.Body, serverReqs)
	}

	resp, err := srPost(ctx, h.proxyURL, `{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":{"name":"test_sampling","arguments":{"prompt":"hi"}}}`)
	if err != nil {
		t.Fatalf("POST through proxy: %v (headers buffered by the proxy?)", err)
	}
	defer resp.Body.Close()
	postEvents := make(chan string, 4)
	go readEvents(resp.Body, postEvents)

	reqEvents := postEvents
	if useGET {
		reqEvents = serverReqs
	}
	select {
	case ev := <-reqEvents:
		if !strings.Contains(ev, `"method"`) {
			t.Fatalf("first event is not the server request: %s", ev)
		}
	case <-ctx.Done():
		t.Fatal("server-to-client request never reached the client: the proxy buffered it")
	}

	ar, err := srPost(ctx, h.proxyURL, answer)
	if err != nil {
		t.Fatalf("relaying the client's answer: %v", err)
	}
	ar.Body.Close()
	if ar.StatusCode != http.StatusAccepted {
		t.Fatalf("answer POST status = %d, want 202", ar.StatusCode)
	}

	select {
	case ev, ok := <-postEvents:
		if !ok {
			t.Fatal("POST stream ended without the tool result")
		}
		return ev
	case <-ctx.Done():
		t.Fatal("tool result never arrived: the client's answer was not relayed upstream")
	}
	return ""
}

func spanNamed(spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	return nil
}

func waitSpans(exp *tracetest.InMemoryExporter, n int) tracetest.SpanStubs {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := exp.GetSpans(); len(s) >= n {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	return exp.GetSpans()
}

func TestStreamable_ServerRequestRelayedOnPostStream(t *testing.T) {
	for _, tc := range []struct {
		method string
		traced bool
	}{
		{"sampling/createMessage", true},
		{"elicitation/create", true},
		{"sampling/createMessage", false}, // untraced client method: plain forwarding path
	} {
		t.Run(fmt.Sprintf("%s/traced=%v", tc.method, tc.traced), func(t *testing.T) {
			h := newSRHarness(t, newServerRequestUpstream(tc.method, false), &Filter{})
			clientMethod := "tools/call"
			if !tc.traced {
				clientMethod = "resources/read"
			}
			answer := `{"jsonrpc":"2.0","id":7,"result":{"role":"assistant","content":{"type":"text","text":"ok"}}}`
			got := callTool(t, h, clientMethod, answer, false)
			if !strings.Contains(got, `"id":1`) || !strings.Contains(got, "assistant") {
				t.Fatalf("tool result does not carry the relayed answer: %s", got)
			}

			want := 2
			if !tc.traced {
				want = 1
			}
			spans := waitSpans(h.exporter, want)
			sr := spanNamed(spans, "mcp "+tc.method)
			if sr == nil {
				t.Fatalf("no span for the relayed %s; got %d spans", tc.method, len(spans))
			}
			requireAttr(t, *sr, "mcp.direction", "server_to_client")
			requireAttr(t, *sr, "mcp.status", "ok")
			requireAttr(t, *sr, "mcp.request.id", "7")
			requireAttr(t, *sr, "mcp.session.id", testSession)
			if sr.Status.Code != codes.Ok {
				t.Errorf("server request span status = %v, want Ok", sr.Status.Code)
			}
			if tc.traced {
				call := spanNamed(spans, "mcp tools/call test_sampling")
				if call == nil {
					t.Fatal("no tools/call span")
				}
				if sr.Parent.SpanID() != call.SpanContext.SpanID() {
					t.Error("server request span is not a child of the tools/call span it happened in")
				}
				requireAttr(t, *call, "mcp.status", "ok")
			}
		})
	}
}

func TestStreamable_ServerRequestRelayedOnGetStream(t *testing.T) {
	h := newSRHarness(t, newServerRequestUpstream("elicitation/create", true), &Filter{})
	answer := `{"jsonrpc":"2.0","id":7,"error":{"code":-1,"message":"user declined"}}`
	got := callTool(t, h, "tools/call", answer, true)
	if !strings.Contains(got, "user declined") {
		t.Fatalf("tool result does not carry the relayed answer: %s", got)
	}
	spans := waitSpans(h.exporter, 2)
	sr := spanNamed(spans, "mcp elicitation/create")
	if sr == nil {
		t.Fatalf("no span for elicitation/create relayed on the GET stream; got %d spans", len(spans))
	}
	requireAttr(t, *sr, "mcp.direction", "server_to_client")
	requireAttr(t, *sr, "mcp.status", "error")
	requireAttr(t, *sr, "error.message", "user declined")
}

func TestStreamable_UnansweredServerRequestEndsAbandoned(t *testing.T) {
	h := newSRHarness(t, newServerRequestUpstream("sampling/createMessage", false), &Filter{})
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	resp, err := srPost(ctx, h.proxyURL, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"test_sampling"}}`)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body) // never answer; upstream gives up after 3s
	resp.Body.Close()

	spans := waitSpans(h.exporter, 2)
	sr := spanNamed(spans, "mcp sampling/createMessage")
	if sr == nil {
		t.Fatalf("unanswered server request left no span; got %d spans", len(spans))
	}
	requireAttr(t, *sr, "mcp.status", "abandoned")
}

func TestStreamable_ServerPingFollowsLifecycleFilter(t *testing.T) {
	if (&Filter{}).ShouldTraceServerRequest("ping") {
		t.Error("server ping traced without --include-lifecycle")
	}
	if !(&Filter{IncludeLifecycle: true}).ShouldTraceServerRequest("ping") {
		t.Error("server ping not traced with --include-lifecycle")
	}
	for _, m := range []string{"sampling/createMessage", "elicitation/create", "roots/list"} {
		if !(&Filter{}).ShouldTraceServerRequest(m) {
			t.Errorf("%s not traced by default", m)
		}
	}
}

// A client response must never open a span of its own, even with --trace-all.
func TestStreamable_ClientResponseIsNotTracedAsRequest(t *testing.T) {
	h := newSRHarness(t, newServerRequestUpstream("sampling/createMessage", false), &Filter{TraceAll: true, IncludeLifecycle: true})
	resp, err := srPost(context.Background(), h.proxyURL, `{"jsonrpc":"2.0","id":99,"result":{}}`)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	time.Sleep(50 * time.Millisecond)
	if n := len(h.exporter.GetSpans()); n != 0 {
		t.Errorf("a bare client response produced %d spans, want 0", n)
	}
}
