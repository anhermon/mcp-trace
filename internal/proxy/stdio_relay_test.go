package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
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
last = {"answer": None}
def later(req, delay, text):
    time.sleep(delay)
    send({"jsonrpc":"2.0","id":req["id"],"result":{"content":[{"type":"text","text":text}]}})
for line in sys.stdin:
    m = json.loads(line)
    if "method" not in m:
        req = pending.pop(m["id"], None)
        last["answer"] = m
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
    elif m["method"] == "tools/call" and name == "flood":
        blob = "x" * 100000
        for i in range(200):
            send({"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":blob}})
        send({"jsonrpc":"2.0","id":m["id"],"result":{"content":[{"type":"text","text":"flooded"}]}})
    elif m["method"] == "tools/call" and name == "last":
        send({"jsonrpc":"2.0","id":m["id"],"result":{"content":[{"type":"text","text":json.dumps(last["answer"])}]}})
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
	id := p.register(st, clientKey("sess-a", "client-9"))
	other := p.register(&stdioStream{msgs: make(chan stdioMsg, 1)}, clientKey("sess-b", "client-9"))
	out := p.translateCancel([]byte(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"client-9","reason":"x"}}`), "sess-a")
	var n struct {
		Params struct {
			RequestID json.RawMessage `json:"requestId"`
		} `json:"params"`
	}
	if err := json.Unmarshal(out, &n); err != nil {
		t.Fatal(err)
	}
	if IDString(n.Params.RequestID) != strconv.FormatUint(id, 10) {
		t.Errorf("requestId = %s, want the subprocess's id %d (not the other session's %d)", n.Params.RequestID, id, other)
	}
}

// Two clients that pick the same progress token (SDKs reuse the request id)
// must each get only their own progress, with their own token restored.
func TestStdio_ProgressTokensDoNotCollideAcrossClients(t *testing.T) {
	p := &StdioProxy{router: newStdioRouter(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	a := &stdioStream{msgs: make(chan stdioMsg, 4), sse: true, clientToken: json.RawMessage(`1`)}
	b := &stdioStream{msgs: make(chan stdioMsg, 4), sse: true, clientToken: json.RawMessage(`1`)}
	p.register(a, clientKey("sess-a", "1"))
	p.register(b, clientKey("sess-b", "1"))
	if a.progressToken == b.progressToken {
		t.Fatalf("both streams got subprocess token %q", a.progressToken)
	}
	p.dispatch([]byte(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"` + a.progressToken + `","progress":1}}`))
	select {
	case m := <-a.msgs:
		if !strings.Contains(string(m.data), `"progressToken":1`) {
			t.Errorf("client token not restored: %s", m.data)
		}
	default:
		t.Fatal("A did not get its own progress")
	}
	if len(b.msgs) != 0 {
		t.Fatal("B got A's progress")
	}
}

func TestStdio_InvalidBodiesAreRejected(t *testing.T) {
	h := newRelayHarness(t)
	for _, tc := range []struct {
		name, body string
		code       string
	}{
		{"batch", `[{"jsonrpc":"2.0","id":1,"method":"ping"}]`, "-32600"},
		{"empty object", `{}`, "-32600"},
		{"null id request", `{"jsonrpc":"2.0","id":null,"method":"ping"}`, "-32600"},
		{"null id response", `{"jsonrpc":"2.0","id":null,"result":{}}`, "-32600"},
		{"bad json", `{"jsonrpc":`, "-32700"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := h.do(t, context.Background(), http.MethodPost, tc.body, false)
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"code":`+tc.code) {
				t.Errorf("got %d %s, want 400 with code %s", resp.StatusCode, b, tc.code)
			}
		})
	}
	// None of them reached the subprocess: it still answers normally and
	// has seen no stray message.
	resp := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"last"}}`, false)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), `"text":"null"`) {
		t.Errorf("subprocess saw a rejected body: %s", b)
	}
}

// A client that drops its stream after the server's request was delivered
// can no longer answer it there; the subprocess gets an error instead of
// waiting forever.
func TestStdio_ServerRequestRejectedWhenStreamClosesAfterDelivery(t *testing.T) {
	h := newRelayHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	resp := h.do(t, ctx, http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ask"}}`, true)
	ev := events(resp.Body)
	next(t, ev, "logging notification")
	next(t, ev, "sampling request")
	cancel()
	resp.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for {
		r := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"last"}}`, false)
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if strings.Contains(string(b), "client stream closed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("subprocess never got an error for the undeliverable request; last answer: %s", b)
		}
		time.Sleep(50 * time.Millisecond)
	}
	spans := waitSpans(h.exp, 2)
	if sr := spanNamed(spans, "mcp sampling/createMessage"); sr == nil {
		t.Fatal("no sampling span")
	} else {
		requireAttr(t, *sr, "mcp.status", "abandoned")
	}
}

// A client that opened the GET stream and then stopped reading (here: never
// reads, tiny receive buffer) must not stall everyone else. Its writes time
// out, its stream is dropped, and other clients keep getting answers.
func TestStdio_StalledReaderDoesNotBlockOthers(t *testing.T) {
	old := stdioWriteTimeout
	stdioWriteTimeout = 200 * time.Millisecond
	t.Cleanup(func() { stdioWriteTimeout = old })

	h := newRelayHarness(t)
	addr := strings.TrimPrefix(h.url, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.(*net.TCPConn).SetReadBuffer(4096)
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nAccept: text/event-stream\r\n\r\n", addr)
	time.Sleep(100 * time.Millisecond) // let the GET stream register; never read from it

	// A JSON-only client: the flood of log notifications goes to the stalled
	// GET stream, ~20 MB, far more than socket buffers hold.
	done := make(chan string, 1)
	go func() {
		resp := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"flood"}}`, false)
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		done <- string(b)
	}()
	select {
	case b := <-done:
		if !strings.Contains(b, "flooded") {
			t.Fatalf("response = %s", b)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a client that stopped reading stalled the bridge for everyone")
	}
}

// Progress whose proxy token matches no open stream (its request ended, or
// its client takes only JSON) is dropped, not handed to another client.
func TestStdio_UnmatchedProgressIsDropped(t *testing.T) {
	p := &StdioProxy{router: newStdioRouter(), logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	other := &stdioStream{msgs: make(chan stdioMsg, 4), sse: true}
	p.register(other, clientKey("sess-b", "1"))
	get := &stdioStream{msgs: make(chan stdioMsg, 4), sse: true, get: true}
	p.register(get, "")

	p.dispatch([]byte(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"mcp-trace-99","progress":1}}`))
	if len(other.msgs) != 0 || len(get.msgs) != 0 {
		t.Fatal("progress for an unknown token was delivered to another stream")
	}
	// Ordinary notifications still route to the newest request stream.
	p.dispatch([]byte(`{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"x"}}`))
	if len(other.msgs) != 1 {
		t.Fatal("log notification was not routed to the open request stream")
	}
}

// When the client stream closes while a server request is being delivered,
// the subprocess gets exactly one error answer: from unregister, not a
// second one from the failed delivery.
func TestStdio_UndeliverableServerRequestAnsweredOnce(t *testing.T) {
	var stdin lockedBuffer
	p := &StdioProxy{
		router:  newStdioRouter(),
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		filter:  &Filter{},
		tracer:  sdktrace.NewTracerProvider().Tracer("t"),
		stdin:   &stdin,
		running: true,
	}
	st := &stdioStream{msgs: make(chan stdioMsg), sse: true, ctx: context.Background()} // unbuffered, never read
	p.register(st, clientKey("s", "1"))

	dispatched := make(chan struct{})
	go func() {
		p.dispatch([]byte(`{"jsonrpc":"2.0","id":"srv-1","method":"sampling/createMessage","params":{}}`))
		close(dispatched)
	}()
	// Wait until the request is registered and delivery is blocked.
	deadline := time.Now().Add(2 * time.Second)
	for st.awaiting.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server request never registered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	p.unregister(st)
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch stayed blocked after the stream closed")
	}
	if n := strings.Count(stdin.String(), `"id":"srv-1"`); n != 1 {
		t.Fatalf("subprocess got %d answers for the server request, want 1:\n%s", n, stdin.String())
	}
	if !strings.Contains(stdin.String(), "-32603") {
		t.Errorf("answer is not a JSON-RPC error: %s", stdin.String())
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *lockedBuffer) Close() error { return nil }
func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStdio_EmptyStringIDIsARequest(t *testing.T) {
	h := newRelayHarness(t)
	resp := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":"","method":"ping"}`, false)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"id":""`) {
		t.Fatalf("got %d %s, want 200 with the response for id \"\"", resp.StatusCode, b)
	}
}

func TestStdio_ErrorResponseWithoutIDIsAcceptedAndDropped(t *testing.T) {
	h := newRelayHarness(t)
	resp := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","error":{"code":-32700,"message":"parse error"}}`, false)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	r := h.do(t, context.Background(), http.MethodPost, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"last"}}`, false)
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if !strings.Contains(string(b), `"text":"null"`) {
		t.Errorf("the dropped error response reached the subprocess: %s", b)
	}
}
