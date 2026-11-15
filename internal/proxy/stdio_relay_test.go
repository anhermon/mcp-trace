package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Regression tests for issue #27: --stdio was a POST-only request/response
// bridge. It dropped the subprocess's notifications (logging, progress), never
// relayed its server-to-client requests (sampling, elicitation), and had no
// GET stream, so those MCP conformance scenarios failed through it.

// relayChild is an MCP-shaped stdio server. tools/call "ask" logs, reports
// progress, asks the client to sample, waits for the answer on stdin, then
// returns it. "slow"/"fast" answer after a delay on their own thread, so
// responses can come back out of order.
const relayChild = `
import sys, json, threading, time
lock = threading.Lock()
def send(m):
    with lock:
        print(json.dumps(m), flush=True)
pending = {}
def later(req, delay, text):
    time.sleep(delay)
    send({"jsonrpc":"2.0","id":req["id"],"result":{"content":[{"type":"text","text":text}]}})
for line in sys.stdin:
    m = json.loads(line)
    if "method" not in m:
        req = pending.pop(m["id"], None)
        if req is not None:
            ans = m.get("result") or m.get("error")
            send({"jsonrpc":"2.0","id":req["id"],"result":{"content":[{"type":"text","text":json.dumps(ans)}]}})
        continue
    if "id" not in m:
        continue
    p = m.get("params") or {}
    name = p.get("name")
    if m["method"] == "tools/call" and name == "ask":
        tok = (p.get("_meta") or {}).get("progressToken")
        send({"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"working"}})
        if tok is not None:
            send({"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":tok,"progress":1,"total":2}})
        sid = "s-%s" % m["id"]
        pending[sid] = m
        send({"jsonrpc":"2.0","id":sid,"method":"sampling/createMessage","params":{"messages":[],"maxTokens":5}})
    elif m["method"] == "tools/call" and name in ("slow", "fast"):
        threading.Thread(target=later, args=(m, 0.3 if name == "slow" else 0, name)).start()
    else:
        send({"jsonrpc":"2.0","id":m["id"],"result":{}})
`

type relayHarness struct {
	url string
	exp *tracetest.InMemoryExporter
}

func newRelayHarness(t *testing.T) *relayHarness {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	ctx, cancel := context.WithCancel(context.Background())
	p, err := NewStdioProxy(ctx, "python3", []string{"-u", "-c", relayChild}, &Filter{}, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(func() {
		srv.Close()
		_ = p.Stop()
		cancel()
		_ = tp.Shutdown(context.Background())
	})
	return &relayHarness{url: srv.URL, exp: exp}
}

func (h *relayHarness) do(t *testing.T, ctx context.Context, method, body string, sse bool) *http.Response {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, _ := http.NewRequestWithContext(ctx, method, h.url, r)
	req.Header.Set("Content-Type", "application/json")
	if sse {
		req.Header.Set("Accept", "application/json, text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", method, err)
	}
	return resp
}

// events returns a channel of SSE data payloads read from r.
func events(r io.Reader) <-chan string {
	out := make(chan string, 16)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				out <- v
			}
		}
	}()
	return out
}

func next(t *testing.T, ch <-chan string, what string) map[string]any {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatalf("stream ended before %s", what)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ev), &m); err != nil {
			t.Fatalf("%s: bad JSON %q", what, ev)
		}
		return m
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	return nil
}

func TestStdio_RelaysNotificationsAndServerRequestsOnPostStream(t *testing.T) {
	h := newRelayHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":"call-1","method":"tools/call","params":{"name":"ask","_meta":{"progressToken":"tok-1"}}}`, true)
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	ev := events(resp.Body)

	if m := next(t, ev, "logging notification"); m["method"] != "notifications/message" {
		t.Fatalf("first event = %v, want notifications/message", m)
	}
	if m := next(t, ev, "progress notification"); m["method"] != "notifications/progress" {
		t.Fatalf("second event = %v, want notifications/progress", m)
	}
	sr := next(t, ev, "sampling request")
	if sr["method"] != "sampling/createMessage" {
		t.Fatalf("third event = %v, want sampling/createMessage", sr)
	}

	srID, _ := json.Marshal(sr["id"])
	ans := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":`+string(srID)+`,"result":{"role":"assistant","content":{"type":"text","text":"sampled"}}}`, true)
	ans.Body.Close()
	if ans.StatusCode != http.StatusAccepted {
		t.Fatalf("answer POST status = %d, want 202", ans.StatusCode)
	}

	final := next(t, ev, "tool result")
	if final["id"] != "call-1" {
		t.Errorf("response id = %v, want the client's own id call-1", final["id"])
	}
	if b, _ := json.Marshal(final["result"]); !strings.Contains(string(b), "sampled") {
		t.Errorf("tool result does not carry the relayed answer: %s", b)
	}

	spans := waitSpans(h.exp, 2)
	srSpan := spanNamed(spans, "mcp sampling/createMessage")
	call := spanNamed(spans, "mcp tools/call ask")
	if srSpan == nil || call == nil {
		t.Fatalf("want tools/call and sampling spans, got %d spans", len(spans))
	}
	requireAttr(t, *srSpan, "mcp.direction", "server_to_client")
	requireAttr(t, *srSpan, "mcp.status", "ok")
	requireAttr(t, *srSpan, "mcp.request.id", "s-1")
	requireAttr(t, *call, "mcp.request.id", "call-1")
	if srSpan.Parent.SpanID() != call.SpanContext.SpanID() {
		t.Error("sampling span is not a child of the tools/call span")
	}
}

// With no POST event stream open, the server's request goes to the GET stream;
// the JSON-only POST still gets its response once the client answers.
func TestStdio_ServerRequestOnGetStream(t *testing.T) {
	h := newRelayHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	get := h.do(t, ctx, http.MethodGet, "", true)
	defer get.Body.Close()
	if get.StatusCode != http.StatusOK || get.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("GET = %d %q, want 200 text/event-stream", get.StatusCode, get.Header.Get("Content-Type"))
	}
	gev := events(get.Body)

	type result struct {
		body string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"ask"}}`, false)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		done <- result{string(b), err}
	}()

	next(t, gev, "logging notification on GET stream")
	sr := next(t, gev, "sampling request on GET stream")
	if sr["method"] != "sampling/createMessage" {
		t.Fatalf("GET event = %v, want sampling/createMessage", sr)
	}
	srID, _ := json.Marshal(sr["id"])
	h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":`+string(srID)+`,"error":{"code":-1,"message":"declined"}}`, true).Body.Close()

	select {
	case r := <-done:
		if !strings.Contains(r.body, `"id":7`) || !strings.Contains(r.body, "declined") {
			t.Fatalf("JSON response = %s, want id 7 carrying the answer", r.body)
		}
	case <-ctx.Done():
		t.Fatal("POST never got its response")
	}
	spans := waitSpans(h.exp, 2)
	srSpan := spanNamed(spans, "mcp sampling/createMessage")
	if srSpan == nil {
		t.Fatal("no span for the request relayed on the GET stream")
	}
	requireAttr(t, *srSpan, "mcp.status", "error")
	requireAttr(t, *srSpan, "error.message", "declined")
}

// No client stream can carry the request: the subprocess gets an error answer
// instead of waiting forever, and the POST completes.
func TestStdio_ServerRequestWithNoStreamIsRejected(t *testing.T) {
	h := newRelayHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ask"}}`, false)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "no client stream") {
		t.Fatalf("response = %s, want the proxy's rejection relayed by the tool", b)
	}
}

// Two clients both using id 1 must each get their own response, even when
// the subprocess answers out of order.
func TestStdio_ConcurrentRequestsWithSameIDAreNotCrossed(t *testing.T) {
	h := newRelayHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	got := map[string]string{}
	var mu sync.Mutex
	for _, name := range []string{"slow", "fast"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"`+name+`"}}`, false)
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			mu.Lock()
			got[name] = string(b)
			mu.Unlock()
		}(name)
	}
	wg.Wait()
	for _, name := range []string{"slow", "fast"} {
		if !strings.Contains(got[name], `"text":"`+name+`"`) || !strings.Contains(got[name], `"id":1`) {
			t.Errorf("%s got %s", name, got[name])
		}
	}
}

func TestStdio_InitializeIssuesSessionID(t *testing.T) {
	h := newRelayHarness(t)
	resp := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"clientInfo":{"name":"c","version":"1"}}}`, false)
	resp.Body.Close()
	if resp.Header.Get(mcpSessionHeader) == "" {
		t.Error("initialize response has no Mcp-Session-Id")
	}
}

// A person answering an elicitation can take longer than the response
// timeout; the wait must not fail while the request is with the client.
func TestStdio_TimeoutPausedWhileClientAnswers(t *testing.T) {
	old := stdioResponseTimeout
	stdioResponseTimeout = 200 * time.Millisecond
	t.Cleanup(func() { stdioResponseTimeout = old })

	h := newRelayHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ask"}}`, true)
	defer resp.Body.Close()
	ev := events(resp.Body)
	next(t, ev, "logging notification")
	sr := next(t, ev, "sampling request")
	time.Sleep(600 * time.Millisecond) // three timeouts' worth
	srID, _ := json.Marshal(sr["id"])
	h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":`+string(srID)+`,"result":{"ok":true}}`, true).Body.Close()
	if final := next(t, ev, "tool result"); final["result"] == nil {
		t.Fatalf("got %v, want the tool result", final)
	}
}

func TestStdio_CancelledRequestIDIsTranslated(t *testing.T) {
	p := &StdioProxy{router: newStdioRouter()}
	st := &stdioStream{msgs: make(chan stdioMsg, 1)}
	id := p.register(st, "client-9")
	out := p.translateCancel([]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"client-9","reason":"x"}}`))
	var n struct {
		Params struct {
			RequestID json.RawMessage `json:"requestId"`
		} `json:"params"`
	}
	if err := json.Unmarshal(out, &n); err != nil {
		t.Fatal(err)
	}
	if IDString(n.Params.RequestID) != strconv.FormatUint(id, 10) {
		t.Errorf("requestId = %s, want the subprocess's id %d", n.Params.RequestID, id)
	}
}
