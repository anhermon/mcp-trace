// Command agent-inspect-join is a self-contained proof that MCP wire-level tool
// call spans emitted by the mcp-trace proxy can be joined to an agent's own
// in-process intent records (agent-inspect v1.0 JSONL) by span id alone.
//
//	go run ./examples/agent-inspect-join                  # with W3C propagation
//	go run ./examples/agent-inspect-join -no-propagate    # negative control
//
// Everything runs in one process on ephemeral ports: a fake MCP HTTP+SSE server
// that fails a tool twice and succeeds on the third attempt, the real mcp-trace
// proxy, an OTLP/HTTP receiver standing in for a collector, and a client that
// retries. No Docker, no collector, no Jaeger.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/anhermon/mcp-trace/internal/proxy"
	"github.com/anhermon/mcp-trace/internal/telemetry"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	toolName    = "fetch-inventory"
	runID       = "run_mcp_retry_v1"
	maxAttempts = 3
	argsJSON    = `{"sku":"demo-sku-1"}`
)

func main() {
	noPropagate := flag.Bool("no-propagate", false, "omit traceparent from the client's POSTs (negative control)")
	outDir := flag.String("out", "", "directory for spans.json and intent.jsonl (default: a new temp dir)")
	flag.Parse()

	if err := run(*noPropagate, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "agent-inspect-join: %v\n", err)
		os.Exit(1)
	}
}

func run(noPropagate bool, outDir string) error {
	if outDir == "" {
		d, err := os.MkdirTemp("", "agent-inspect-join")
		if err != nil {
			return err
		}
		outDir = d
	} else if err := os.MkdirAll(outDir, 0o750); err != nil {
		return err
	}
	spansPath := filepath.Join(outDir, "spans.json")
	intentPath := filepath.Join(outDir, "intent.jsonl")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Warn level: the proxy's own logging would drown out the demo narrative.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Collector stand-in first, so the exporter has somewhere to go.
	sink := &spanSink{path: spansPath}
	otlpAddr, stopOTLP, err := listen(sink.handler())
	if err != nil {
		return err
	}
	defer stopOTLP()

	provider, err := telemetry.New(ctx, telemetry.Config{
		UseHTTP:      true,
		HTTPEndpoint: "http://" + otlpAddr,
		Insecure:     true,
		ServiceName:  "mcp-trace",
		Logger:       logger,
	})
	if err != nil {
		return fmt.Errorf("initialising OTel: %w", err)
	}

	upstreamAddr, stopUpstream, err := listen(newMCPServer())
	if err != nil {
		return err
	}
	defer stopUpstream()

	p, err := proxy.New(ctx, "http://"+upstreamAddr, &proxy.Filter{}, provider.Tracer, logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	proxyAddr, stopProxy, err := listen(p)
	if err != nil {
		return err
	}
	defer stopProxy()

	fmt.Printf("upstream MCP server  http://%s\n", upstreamAddr)
	fmt.Printf("mcp-trace proxy      http://%s\n", proxyAddr)
	fmt.Printf("OTLP receiver        http://%s/v1/traces\n", otlpAddr)
	fmt.Printf("propagation          %s\n\n", map[bool]string{true: "OFF (-no-propagate)", false: "ON"}[noPropagate])

	events, err := callWithRetries("http://"+proxyAddr, noPropagate)
	if err != nil {
		return err
	}
	if err := writeJSONL(intentPath, events); err != nil {
		return err
	}

	// Shutdown flushes the batcher and returns only after the receiver has
	// answered, so spans.json is complete on the next line.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := provider.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("flushing spans: %w", err)
	}

	spans, err := readSpans(spansPath)
	if err != nil {
		return fmt.Errorf("reading %s: %w", spansPath, err)
	}

	fmt.Printf("intent JSONL         %s (%d events)\n", intentPath, len(events))
	fmt.Printf("wire spans           %s (%d spans)\n\n", spansPath, len(spans))

	return report(events, spans, noPropagate)
}

// listen starts h on an ephemeral loopback port and returns its address.
// ":0" with ListenAndServe would give no way to learn the port.
func listen(h http.Handler) (addr string, stop func(), err error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(l) }()
	return l.Addr().String(), func() { _ = srv.Close() }, nil
}

// ---------------------------------------------------------------------------
// fake MCP server (HTTP+SSE), adapted from examples/demo
// ---------------------------------------------------------------------------

// mcpServer speaks just enough of the MCP HTTP+SSE transport for the proxy to
// trace it, and fails fetch-inventory twice before succeeding.
type mcpServer struct {
	mu       sync.Mutex
	sessions map[string]chan string
	attempts int
}

func newMCPServer() http.Handler {
	s := &mcpServer{sessions: make(map[string]chan string)}
	mux := http.NewServeMux()
	mux.HandleFunc("/sse", s.sse)
	mux.HandleFunc("/messages", s.messages)
	return mux
}

func (s *mcpServer) sse(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	sid := fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	ch := make(chan string, 16)
	s.mu.Lock()
	s.sessions[sid] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, sid)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "event: endpoint\ndata: /messages?session_id=%s\n\n", sid)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			// The empty sentinel means the last response has been delivered.
			// Ending the stream here is what lets the proxy's upstream reader
			// finish on EOF instead of on a cancelled request.
			if msg == "" {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *mcpServer) messages(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session_id")
	s.mu.Lock()
	ch, ok := s.sessions[sid]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}

	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)

	s.mu.Lock()
	s.attempts++
	n := s.attempts
	s.mu.Unlock()

	// Answer asynchronously on the SSE stream, like a real MCP server.
	go func() {
		// Roughly mirrors the agent-inspect retry fixture's per-attempt work.
		time.Sleep([]time.Duration{40, 45, 60}[min(n, 3)-1] * time.Millisecond)
		msg := map[string]any{"jsonrpc": "2.0", "id": req.ID}
		if n < maxAttempts {
			// A JSON-RPC error object, not result.isError: only this path
			// populates mcp.rpc.error_code on the proxy's span.
			msg["error"] = map[string]any{"code": -32003, "message": "synthetic 503 from upstream"}
		} else {
			msg["result"] = map[string]any{"content": []map[string]string{{"text": `{"sku":"demo-sku-1","onHand":7}`}}}
		}
		body, _ := json.Marshal(msg)
		select {
		case ch <- string(body):
		case <-time.After(time.Second):
			return
		}
		if n >= maxAttempts {
			ch <- "" // no further responses on this session; end the stream
		}
	}()
}

// ---------------------------------------------------------------------------
// client: one logical tool call, three attempts
// ---------------------------------------------------------------------------

// callWithRetries performs the retry sequence and returns the agent-inspect
// events describing it. Each attempt injects its own span context as
// traceparent, unless noPropagate is set.
func callWithRetries(proxyBase string, noPropagate bool) ([]intentEvent, error) {
	// A local provider with no exporter: the client's spans only need real,
	// sampled span contexts to propagate. telemetry.New already owns the global.
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tracer := tp.Tracer("agent-inspect-demo")
	prop := propagation.TraceContext{}

	base, err := url.Parse(proxyBase)
	if err != nil {
		return nil, err
	}
	endpoint, stream, err := subscribe(base)
	if err != nil {
		return nil, err
	}
	responses := make(chan string, 8)
	streamDone := make(chan struct{})
	go func() { scanSSE(stream, responses); close(streamDone) }()
	// The upstream ends the stream once the last response is out. Waiting for
	// that to reach us means Close() tears down a finished stream instead of
	// cancelling a live one, which is what makes the proxy log a stream error.
	// The timeout keeps a lost end-of-stream from hanging the demo.
	defer func() {
		select {
		case <-streamDone:
		case <-time.After(2 * time.Second):
		}
		stream.Close()
	}()

	runStart := time.Now()
	runCtx, runSpan := tracer.Start(context.Background(), "agent run")
	toolStart := time.Now()
	toolCtx, toolSpan := tracer.Start(runCtx, "tool-call "+toolName)

	src := intentSource{Type: "otel", Name: "mcp-trace-demo", Version: "1.0-demo"}
	traceID := trace.SpanContextFromContext(toolCtx).TraceID().String()
	runSpanID := trace.SpanContextFromContext(runCtx).SpanID().String()
	toolSpanID := trace.SpanContextFromContext(toolCtx).SpanID().String()

	attempts := make([]intentEvent, 0, maxAttempts)
	toolStatus := "error"
	for i := 1; i <= maxAttempts; i++ {
		if i > 1 {
			// 100ms then 200ms, as in the agent-inspect retry fixture.
			time.Sleep(time.Duration(100*(i-1)) * time.Millisecond)
		}
		attemptCtx, attemptSpan := tracer.Start(toolCtx, fmt.Sprintf("%s attempt %d", toolName, i))
		start := time.Now()

		hdr := http.Header{"Content-Type": []string{"application/json"}}
		if !noPropagate {
			prop.Inject(attemptCtx, propagation.HeaderCarrier(hdr))
		}
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"%d","method":"tools/call","params":{"name":%q,"arguments":%s}}`,
			i, toolName, argsJSON)
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, endpoint, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header = hdr
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()

		status, errMsg := "error", "no response"
		select {
		case raw := <-responses:
			status, errMsg = rpcStatus(raw)
		case <-time.After(5 * time.Second):
		}
		end := time.Now()
		attemptSpan.End()

		ev := intentEvent{
			SchemaVersion: "1.0",
			EventID:       fmt.Sprintf("evt_attempt_%d", i),
			RunID:         runID,
			ParentID:      "evt_tool",
			Kind:          "TOOL",
			Name:          toolName,
			Status:        status,
			Timestamp:     isoMillis(start),
			StartedAt:     isoMillis(start),
			EndedAt:       isoMillis(end),
			DurationMs:    end.Sub(start).Milliseconds(),
			Confidence:    "explicit",
			Source:        src,
			Attributes:    map[string]any{"input.value.summary": argSummary(argsJSON)},
			Metadata: map[string]any{
				"toolName":    toolName,
				"attempt":     i,
				"maxAttempts": maxAttempts,
				"backoffMs":   100 * (i - 1),
				"arguments":   json.RawMessage(argsJSON),
			},
			Trace: &intentTrace{
				TraceID:      traceID,
				SpanID:       trace.SpanContextFromContext(attemptCtx).SpanID().String(),
				ParentSpanID: toolSpanID,
			},
			start: start,
			end:   end,
		}
		if status != "ok" {
			ev.Metadata["error"] = errMsg
		}
		attempts = append(attempts, ev)
		if status == "ok" {
			toolStatus = "ok"
			break
		}
	}

	toolEnd := time.Now()
	toolSpan.End()
	runSpan.End()
	runEnd := time.Now()

	toolEvent := intentEvent{
		SchemaVersion: "1.0",
		EventID:       "evt_tool",
		RunID:         runID,
		ParentID:      "evt_run",
		Kind:          "TOOL",
		Name:          toolName,
		Status:        toolStatus,
		Timestamp:     isoMillis(toolStart),
		StartedAt:     isoMillis(toolStart),
		EndedAt:       isoMillis(toolEnd),
		DurationMs:    toolEnd.Sub(toolStart).Milliseconds(),
		Confidence:    "explicit",
		Source:        src,
		Attributes:    map[string]any{"input.value.summary": argSummary(argsJSON)},
		Metadata: map[string]any{
			"toolName":    toolName,
			"maxAttempts": maxAttempts,
			"arguments":   json.RawMessage(argsJSON),
		},
		Trace: &intentTrace{TraceID: traceID, SpanID: toolSpanID, ParentSpanID: runSpanID},
		start: toolStart,
		end:   toolEnd,
	}
	runEvent := intentEvent{
		SchemaVersion: "1.0",
		EventID:       "evt_run",
		RunID:         runID,
		Kind:          "RUN",
		Name:          "mcp-retry-run",
		Status:        toolStatus,
		Timestamp:     isoMillis(runStart),
		StartedAt:     isoMillis(runStart),
		EndedAt:       isoMillis(runEnd),
		DurationMs:    runEnd.Sub(runStart).Milliseconds(),
		Confidence:    "explicit",
		Source:        src,
		Trace:         &intentTrace{TraceID: traceID, SpanID: runSpanID},
		start:         runStart,
		end:           runEnd,
	}

	return append([]intentEvent{runEvent, toolEvent}, attempts...), nil
}

// rpcStatus reduces a JSON-RPC response to the agent-inspect status vocabulary.
func rpcStatus(raw string) (status, errMsg string) {
	resp, err := proxy.ParseResponse([]byte(raw))
	if err != nil {
		return "error", err.Error()
	}
	if isErr, msg, _ := proxy.IsError(resp); isErr {
		return "error", msg
	}
	return "ok", ""
}

// subscribe opens the SSE stream on the proxy and returns the absolute POST
// endpoint the upstream advertised, plus the still-open stream.
func subscribe(base *url.URL) (string, io.ReadCloser, error) {
	sseURL := base.JoinPath("/sse").String()
	resp, err := http.Get(sseURL) //nolint:gosec,noctx // demo
	if err != nil {
		return "", nil, fmt.Errorf("connecting to %s: %w", sseURL, err)
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: /") {
			continue
		}
		rel, err := url.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		if err != nil {
			return "", nil, err
		}
		return base.ResolveReference(rel).String(), resp.Body, nil
	}
	resp.Body.Close()
	return "", nil, fmt.Errorf("no endpoint event from %s", sseURL)
}

// scanSSE forwards JSON-RPC response payloads off the stream. The calls are
// serial, so a single channel is enough demultiplexing.
func scanSSE(r io.Reader, out chan<- string) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: {") {
			out <- strings.TrimPrefix(line, "data: ")
		}
	}
}

// ---------------------------------------------------------------------------
// report: the join, the merged tree, and the honest negative control
// ---------------------------------------------------------------------------

func report(events []intentEvent, spans []wireSpan, noPropagate bool) error {
	// The join input: span identity only. Nothing else crosses this boundary.
	intentBySpanID := make(map[string]string, len(events))
	for _, e := range events {
		if e.Trace != nil {
			intentBySpanID[e.Trace.SpanID] = e.EventID
		}
	}
	keys := make([]joinKey, 0, len(spans))
	for _, s := range spans {
		keys = append(keys, joinKey{spanID: s.SpanID, parentSpanID: s.ParentSpanID})
	}
	joined := joinBySpanID(intentBySpanID, keys)

	spanByID := make(map[string]wireSpan, len(spans))
	for _, s := range spans {
		spanByID[s.SpanID] = s
	}
	// eventID -> wire spans attached to it.
	attached := make(map[string][]wireSpan, len(joined))
	for spanID, evID := range joined {
		attached[evID] = append(attached[evID], spanByID[spanID])
	}

	fmt.Println("merged tree — wire spans attached by span id only")
	fmt.Println("────────────────────────────────────────────────────────────────────────")
	for _, e := range events {
		indent := map[string]string{"evt_run": "", "evt_tool": "  "}[e.EventID]
		if indent == "" && e.EventID != "evt_run" {
			indent = "    "
		}
		label := e.Kind + " " + e.Name
		if strings.HasPrefix(e.EventID, "evt_attempt_") {
			label = "attempt " + strings.TrimPrefix(e.EventID, "evt_attempt_")
		} else if e.EventID == "evt_tool" {
			label = "TOOL " + e.Name + " (logical)"
		}
		fmt.Printf("%s%-32s %-14s span=%s parent=%-16s agent %4dms  %s\n",
			indent, label, e.EventID, e.Trace.SpanID, orDash(e.Trace.ParentSpanID), e.DurationMs, e.Status)
		for _, s := range attached[e.EventID] {
			fmt.Printf("%s  └─ wire %-27s span=%s parent=%s  proxy %sms  %s\n",
				indent, s.Name, s.SpanID, s.ParentSpanID,
				s.Attributes["mcp.duration_ms"], wireStatus(s))
		}
	}
	fmt.Printf("\nattached %d/%d wire spans by span id (tool name and timestamps were not consulted: see joinKey in join.go)\n",
		len(joined), len(spans))

	if len(joined) == 0 {
		negativeControl(spans)
	}
	heuristicSweep(events, spans)

	return assertAttachments(events, joined, spanByID, noPropagate)
}

// negativeControl states what is actually lost when traceparent is missing.
func negativeControl(spans []wireSpan) {
	traces := map[string]bool{}
	orphans := 0
	for _, s := range spans {
		traces[s.TraceID] = true
		if s.ParentSpanID == "" || s.ParentSpanID == "0000000000000000" {
			orphans++
		}
	}
	fmt.Println("\nNEGATIVE CONTROL — no traceparent")
	fmt.Println("────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  %d/%d wire spans have no parent span, spread over %d distinct trace ids.\n", orphans, len(spans), len(traces))
	fmt.Println("  There is no evidence in the wire data that these spans belong to one")
	fmt.Println("  logical tool call at all: they are indistinguishable from three")
	fmt.Println("  independent calls made by three different agents.")
	fmt.Println("  The span-id join needs no tolerance and no clock assumption to say so.")
}

// heuristicSweep shows what the name+time correlator has to guess. The finding
// is not "the heuristic is always wrong" — it is that its answer is a function
// of a skew knob nobody can derive, and that even at zero skew it needs a
// "narrowest containing node" tiebreak the span-id join never needs.
func heuristicSweep(events []intentEvent, spans []wireSpan) {
	var nodes []nameTimeWindow
	for _, e := range events {
		if e.Kind == "TOOL" {
			nodes = append(nodes, nameTimeWindow{eventID: e.EventID, toolName: e.Name, start: e.start, end: e.end})
		}
	}
	fmt.Println("\nname + time-window heuristic — candidate intent nodes per wire span")
	fmt.Println("────────────────────────────────────────────────────────────────────────")
	fmt.Printf("  %-18s %-8s %s\n", "wire span", "skew", "candidates")
	for _, s := range spans {
		for _, skew := range []time.Duration{0, 50 * time.Millisecond, 150 * time.Millisecond, 250 * time.Millisecond} {
			c := nameTimeCandidates(nodes, s.Attributes["mcp.tool.name"], s.start(), s.end(), skew)
			fmt.Printf("  %-18s %-8s %d  %v\n", "id="+s.Attributes["mcp.request.id"]+" "+s.SpanID[:8], skew.String(), len(c), c)
		}
	}
	fmt.Println("  At 0ms the heuristic is already unreliable in both directions: a wire")
	fmt.Println("  span usually matches two nodes — its attempt and the logical TOOL node")
	fmt.Println("  containing it, so it needs a tiebreak — while the last attempt often")
	fmt.Println("  matches none, because the proxy closes its span microseconds after the")
	fmt.Println("  agent stamped that attempt's end. As the assumed skew grows the")
	fmt.Println("  heuristic stops telling the attempts apart instead.")
	fmt.Println("  The retries are byte-identical (same sha256 in input.value.summary), so")
	fmt.Println("  arguments cannot break the tie either.")
}

// assertAttachments is the self-check: mcp.request.id is the attempt number the
// upstream actually saw, so a wire span landing on the wrong attempt node fails
// here. It is independent evidence — the join never looks at it.
func assertAttachments(events []intentEvent, joined map[string]string, spanByID map[string]wireSpan, noPropagate bool) error {
	// Zero attachments is the expected result only without propagation. With
	// propagation it means the join broke, so it must not be waved through as
	// "nothing to check".
	if noPropagate {
		if len(joined) != 0 {
			return fmt.Errorf("SELF-CHECK FAILED: %d attachments without traceparent", len(joined))
		}
		fmt.Println("\nSELF-CHECK: PASSED — no traceparent, no attachments, as expected")
		return nil
	}
	wantAttempts := 0
	for _, e := range events {
		if strings.HasPrefix(e.EventID, "evt_attempt_") {
			wantAttempts++
		}
	}
	if len(joined) != wantAttempts {
		return fmt.Errorf("SELF-CHECK FAILED: %d attachments for %d attempts", len(joined), wantAttempts)
	}
	for spanID, evID := range joined {
		want := "evt_attempt_" + spanByID[spanID].Attributes["mcp.request.id"]
		if evID != want {
			return fmt.Errorf("SELF-CHECK FAILED: span %s (mcp.request.id=%s) attached to %s, want %s",
				spanID, spanByID[spanID].Attributes["mcp.request.id"], evID, want)
		}
	}
	fmt.Printf("\nSELF-CHECK: PASSED — all %d wire spans attached to the attempt whose\n", len(joined))
	fmt.Println("            mcp.request.id matches, using span identity alone.")
	return nil
}

func wireStatus(s wireSpan) string {
	out := "mcp.status=" + s.Attributes["mcp.status"]
	if code := s.Attributes["mcp.rpc.error_code"]; code != "" {
		out += " mcp.rpc.error_code=" + code
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
