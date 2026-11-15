// Package proxy implements stdio transport for MCP.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anhermon/mcp-trace/v2/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// StdioProxy wraps a local MCP server subprocess and proxies JSON-RPC over stdio.
type StdioProxy struct {
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	stderr      io.ReadCloser
	filter      *Filter
	tracer      trace.Tracer
	logger      *slog.Logger
	CaptureArgs bool
	clientName  string
	clientVer   string
	mu          sync.Mutex // guards running, stopped, clientName, clientVer
	writeMu     sync.Mutex // serialises writes to stdin
	childGone   chan struct{}
	router      stdioRouter
	running     bool
	stopped     bool // tracks if Stop() was called
	ctx         context.Context
	cancel      context.CancelFunc
	waitDone    chan struct{} // closed when cmd.Wait() completes
	exitErr     chan error    // receives subprocess exit errors
}

// NewStdioProxy creates a proxy that wraps a local MCP server subprocess.
func NewStdioProxy(ctx context.Context, command string, args []string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*StdioProxy, error) {
	cmd := exec.CommandContext(ctx, command, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("creating stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, fmt.Errorf("creating stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		stdin.Close()
		stdout.Close()
		return nil, fmt.Errorf("creating stderr pipe: %w", err)
	}

	proxyCtx, cancel := context.WithCancel(ctx)

	p := &StdioProxy{
		cmd:       cmd,
		stdin:     stdin,
		stdout:    stdout,
		stderr:    stderr,
		filter:    filter,
		tracer:    tracer,
		router:    newStdioRouter(),
		logger:    logger,
		childGone: make(chan struct{}),
		ctx:       proxyCtx,
		cancel:    cancel,
		waitDone:  make(chan struct{}),
		exitErr:   make(chan error, 1),
	}

	return p, nil
}

// Start begins the subprocess and response reader.
func (p *StdioProxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("proxy already running")
	}

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("starting subprocess: %w", err)
	}

	p.running = true

	// Read stdout in a goroutine
	go p.readResponses()

	// Forward stderr to logger
	go p.forwardStderr()

	// Monitor subprocess
	go p.monitorProcess()

	p.logger.Info("stdio subprocess started", "pid", p.cmd.Process.Pid)
	return nil
}

// Stop terminates the subprocess.
func (p *StdioProxy) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()

	p.cancel()

	// Close stdin to signal subprocess to exit
	p.stdin.Close()

	// Wait for subprocess to exit (with timeout)
	select {
	case <-p.waitDone:
		// Process exited cleanly
	case <-time.After(5 * time.Second):
		p.logger.Warn("subprocess did not exit cleanly, killing")
		if err := p.cmd.Process.Kill(); err != nil {
			p.logger.Error("failed to kill subprocess", "err", err)
		}
		<-p.waitDone // wait for the wait to complete after kill
	}

	return nil
}

// readResponses reads the subprocess's stdout, one JSON-RPC message per line,
// and dispatches each message (see dispatch). When stdout ends, every open
// client stream is released.
func (p *StdioProxy) readResponses() {
	scanner := bufio.NewScanner(p.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		// Copy: the scanner reuses its buffer.
		data := make([]byte, len(line))
		copy(data, line)
		p.dispatch(data)
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, os.ErrClosed) {
		p.logger.Error("reading subprocess stdout", "err", err)
	}
	close(p.childGone)
}

func (p *StdioProxy) forwardStderr() {
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.logger.Info("subprocess stderr", "line", scanner.Text())
	}
}

func (p *StdioProxy) monitorProcess() {
	err := p.cmd.Wait()
	close(p.waitDone)

	p.mu.Lock()
	wasStopped := p.stopped
	p.running = false
	p.mu.Unlock()

	if !wasStopped {
		if err != nil {
			p.logger.Error("subprocess exited unexpectedly", "err", err)
			select {
			case p.exitErr <- err:
			default:
			}
		} else {
			p.logger.Warn("subprocess exited")
			select {
			case p.exitErr <- fmt.Errorf("subprocess exited"):
			default:
			}
		}
	}
}

// ExitErr returns a channel that receives an error when the subprocess exits unexpectedly.
// This allows the caller to shut down the proxy when the subprocess dies.
func (p *StdioProxy) ExitErr() <-chan error {
	return p.exitErr
}

// ServeHTTP is the Streamable HTTP face of the stdio bridge.
//
//   - POST with a request: written to the subprocess. If the client accepts
//     text/event-stream, the answer is an SSE stream that carries the
//     subprocess's notifications and server-to-client requests (sampling,
//     elicitation, roots/list) for that request, then the response. A
//     JSON-only client gets the response as application/json.
//   - POST with a notification or a response (the client answering a
//     server-to-client request): written to the subprocess, 202 Accepted.
//   - GET with Accept: text/event-stream: the standalone stream for messages
//     not tied to an open request. Without that Accept header, 405.
func (p *StdioProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.handlePost(w, r)
	case http.MethodGet:
		if !acceptsEventStream(r) {
			http.Error(w, "GET requires Accept: text/event-stream", http.StatusMethodNotAllowed)
			return
		}
		p.handleGet(w, r)
	default:
		http.Error(w, "method not allowed for stdio transport", http.StatusMethodNotAllowed)
	}
}

func acceptsEventStream(r *http.Request) bool {
	for _, v := range r.Header.Values("Accept") {
		for _, part := range strings.Split(v, ",") {
			media := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
			if strings.EqualFold(media, "text/event-stream") {
				return true
			}
		}
	}
	return false
}

// writeChild writes one JSON-RPC message as a single line. Writes are
// serialised: concurrent requests must never interleave on stdin.
func (p *StdioProxy) writeChild(msg []byte) error {
	p.mu.Lock()
	running := p.running
	p.mu.Unlock()
	if !running {
		return errChildNotRunning
	}
	line := make([]byte, 0, len(msg)+1)
	line = append(line, bytes.TrimRight(msg, "\r\n")...)
	line = append(line, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err := p.stdin.Write(line)
	return err
}

var errChildNotRunning = errors.New("subprocess not running")

func (p *StdioProxy) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body failed", http.StatusBadRequest)
		return
	}

	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] == '[' && json.Valid(trimmed) {
		// JSON-RPC batches were removed from MCP in protocol 2025-06-18.
		writeBadRequest(w, nil, -32600, "batch requests are not supported")
		return
	}
	rpcReq, err := ParseRequest(body)
	if err != nil {
		p.logger.Warn("unparseable JSON-RPC request", "bytes", len(body), "err", err)
		writeBadRequest(w, nil, -32700, "parse error")
		return
	}
	requestID := IDString(rpcReq.ID)
	// Presence, not value, decides: "" is a valid JSON-RPC id, so a request
	// with "id": "" expects a response like any other.
	hasID := len(rpcReq.ID) > 0
	switch {
	case string(rpcReq.ID) == "null":
		if rpcReq.Method == "" && isErrorResponse(body) {
			// JSON-RPC's shape for an error whose request id is unknown
			// (e.g. a parse error): nothing to correlate, accept and drop.
			p.logger.Debug("dropping client error response with a null id")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		// Neither a request nor a response the bridge can correlate.
		writeBadRequest(w, nil, -32600, "invalid request: id must not be null")
		return
	case rpcReq.Method == "" && !hasID:
		if isErrorResponse(body) {
			// An error response without an id (JSON-RPC allows one for a
			// parse error) answers nothing the subprocess could correlate:
			// accept it and drop it.
			p.logger.Debug("dropping client error response without an id")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeBadRequest(w, nil, -32600, "invalid request: no method and no id")
		return
	}

	// The client answering a server-to-client request, or a notification:
	// forward and acknowledge, nothing comes back on this POST.
	if rpcReq.Method == "" || !hasID {
		msg := body
		switch rpcReq.Method {
		case "":
			p.finishServerRequest(requestID, body)
		case "notifications/cancelled":
			msg = p.translateCancel(body, r.Header.Get(mcpSessionHeader))
		}
		if err := p.writeChild(msg); err != nil {
			p.childWriteFailed(w, err)
			return
		}
		p.logger.Debug("forwarded client message", "method", rpcReq.Method, "id", requestID)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	sessionID := r.Header.Get(mcpSessionHeader)
	if rpcReq.Method == "initialize" {
		info := ParseClientInfo(rpcReq.Params)
		p.mu.Lock()
		p.clientName, p.clientVer = info.Name, info.Version
		p.mu.Unlock()
		// Streamable HTTP clients key multi-stream behaviour off a session id,
		// and a stdio subprocess cannot issue one, so the bridge does. Requests
		// without it are still served: every client shares the one subprocess.
		sessionID = newSessionID()
		w.Header().Set(mcpSessionHeader, sessionID)
	}

	spanCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	var inflight *InFlightRequest
	if p.filter.ShouldTrace(rpcReq.Method) {
		p.mu.Lock()
		attrs := telemetry.SpanAttrs{
			Method:     rpcReq.Method,
			RequestID:  requestID,
			SessionID:  sessionID,
			Target:     "stdio",
			ClientName: p.clientName,
			ClientVer:  p.clientVer,
		}
		p.mu.Unlock()
		if rpcReq.Method == "tools/call" {
			attrs.ToolName = ToolCallParams(rpcReq.Params)
			attrs.ArgKeys = ToolCallArgKeys(rpcReq.Params)
			if p.CaptureArgs {
				attrs.ArgsJSON = ToolCallArgsJSON(rpcReq.Params, maxArgsLen)
			}
		}
		var span trace.Span
		spanCtx, span = telemetry.StartSpan(spanCtx, p.tracer, attrs)
		inflight = &InFlightRequest{
			Span:      span,
			Method:    rpcReq.Method,
			ToolName:  attrs.ToolName,
			RequestID: requestID,
			StartTime: time.Now(),
		}
		p.logger.Debug("span started", "id", requestID, "method", rpcReq.Method, "tool", attrs.ToolName)
	}

	// JSON-RPC ids are only unique per client, and several HTTP clients can
	// share the one subprocess, so the subprocess sees a proxy-assigned id;
	// the client gets its own id back on the response.
	st := &stdioStream{
		msgs:        make(chan stdioMsg, 64),
		answered:    make(chan struct{}, 1),
		sse:         acceptsEventStream(r),
		ctx:         spanCtx,
		sessionID:   sessionID,
		clientID:    rpcReq.ID,
		clientToken: progressTokenRaw(rpcReq.Params),
	}
	childID := p.register(st, clientKey(sessionID, requestID))
	defer p.unregister(st)

	msg, err := setJSONRPCID(body, []byte(strconv.FormatUint(childID, 10)))
	if err == nil && st.progressToken != "" {
		tok, _ := json.Marshal(st.progressToken)
		msg, err = setProgressToken(msg, true, tok)
	}
	if err != nil {
		p.endInFlightStdio(inflight, telemetry.StatusError, "rewriting request id failed")
		writeBadRequest(w, rpcReq.ID, -32600, "invalid request")
		return
	}
	if err := p.writeChild(msg); err != nil {
		p.endInFlightStdio(inflight, telemetry.StatusError, "failed to write to subprocess")
		p.childWriteFailed(w, err)
		return
	}

	if st.sse {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		flush(w)
	}

	idle := time.NewTimer(stdioResponseTimeout)
	defer idle.Stop()
	for {
		select {
		case <-r.Context().Done():
			p.endInFlightStdio(inflight, telemetry.StatusAbandoned, "client disconnected")
			return
		case <-p.childGone:
			p.endInFlightStdio(inflight, telemetry.StatusAbandoned, "subprocess stdout closed")
			if !st.sse {
				http.Error(w, "subprocess closed", http.StatusBadGateway)
			}
			return
		case <-st.answered:
			resetTimer(idle, stdioResponseTimeout)
		case <-idle.C:
			if st.awaitingClient() {
				// The subprocess is waiting on the client (a person answering
				// an elicitation, a model sampling): not a stalled server.
				idle.Reset(stdioResponseTimeout)
				continue
			}
			p.endInFlightStdio(inflight, telemetry.StatusTimeout, "response timeout")
			if !st.sse {
				http.Error(w, "request timeout", http.StatusGatewayTimeout)
			}
			return
		case m := <-st.msgs:
			resetTimer(idle, stdioResponseTimeout)
			if !m.final {
				if st.sse {
					if err := writeSSEMessage(w, m.data); err != nil {
						p.logger.Warn("dropping client stream: write failed or timed out", "err", err)
						p.endInFlightStdio(inflight, telemetry.StatusAbandoned, "client stopped reading the event stream")
						return
					}
				}
				continue
			}
			data, err := setJSONRPCID(m.data, st.clientID)
			if err != nil {
				data = m.data
			}
			if inflight != nil {
				resp, perr := ParseResponse(data)
				status, errMsg, errCode := telemetry.StatusOK, "", 0
				if perr == nil {
					var isErr bool
					isErr, errMsg, errCode = IsError(resp)
					if isErr {
						status = telemetry.StatusError
					}
				}
				durationMS := float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0
				telemetry.EndSpan(inflight.Span, telemetry.EndAttrs{
					DurationMS:   durationMS,
					Status:       status,
					ErrMsg:       errMsg,
					ErrCode:      errCode,
					ToolName:     inflight.ToolName,
					ResponseSize: len(data),
				})
				p.logger.Debug("span ended", "id", requestID, "method", inflight.Method, "duration_ms", durationMS)
			}
			if st.sse {
				if err := writeSSEMessage(w, data); err != nil {
					p.logger.Debug("writing final response to client failed", "err", err)
				}
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(data); err != nil {
				p.logger.Debug("failed to write response to client", "err", err)
			}
			return
		}
	}
}

// handleGet serves the standalone server-to-client stream.
func (p *StdioProxy) handleGet(w http.ResponseWriter, r *http.Request) {
	st := &stdioStream{
		msgs:      make(chan stdioMsg, 64),
		sse:       true,
		get:       true,
		sessionID: r.Header.Get(mcpSessionHeader),
		ctx:       otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)),
	}
	p.register(st, "")
	defer p.unregister(st)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flush(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-p.childGone:
			return
		case m := <-st.msgs:
			if err := writeSSEMessage(w, m.data); err != nil {
				p.logger.Warn("dropping GET stream: write failed or timed out", "err", err)
				return
			}
		}
	}
}

func (p *StdioProxy) childWriteFailed(w http.ResponseWriter, err error) {
	if errors.Is(err, errChildNotRunning) {
		http.Error(w, "subprocess not running", http.StatusServiceUnavailable)
		return
	}
	if !errors.Is(err, os.ErrClosed) {
		p.logger.Error("writing to subprocess stdin", "err", err)
	}
	http.Error(w, "subprocess write failed", http.StatusBadGateway)
}

func (p *StdioProxy) endInFlightStdio(e *InFlightRequest, status, errMsg string) {
	if e == nil {
		return
	}
	telemetry.EndSpan(e.Span, telemetry.EndAttrs{
		DurationMS: float64(time.Since(e.StartTime).Microseconds()) / 1000.0,
		Status:     status,
		ErrMsg:     errMsg,
		ToolName:   e.ToolName,
	})
}

func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// stdioWriteTimeout bounds each event-stream write. One reader serves every
// client, so a client that stops reading (or half-closed its connection and
// went quiet) would otherwise stall everyone once its buffer filled; after
// this long its stream is dropped instead.
var stdioWriteTimeout = 10 * time.Second

// writeSSEMessage writes one event and flushes it, failing if the client has
// not taken it within stdioWriteTimeout.
func writeSSEMessage(w http.ResponseWriter, data []byte) error {
	rc := http.NewResponseController(w)
	// ErrNotSupported (a writer without deadlines) just means no bound.
	_ = rc.SetWriteDeadline(time.Now().Add(stdioWriteTimeout))
	if _, err := fmt.Fprintf(w, "event: message\ndata: %s\n\n", data); err != nil {
		return err
	}
	if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	// Clear it: the deadline is per connection and would otherwise outlive
	// this response on a kept-alive connection.
	_ = rc.SetWriteDeadline(time.Time{})
	return nil
}

// isErrorResponse reports whether body is a JSON-RPC error response.
func isErrorResponse(body []byte) bool {
	var m struct {
		Error json.RawMessage `json:"error"`
	}
	return json.Unmarshal(body, &m) == nil && len(m.Error) > 0 && string(m.Error) != "null"
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// writeBadRequest answers 400 with a JSON-RPC error body.
func writeBadRequest(w http.ResponseWriter, id json.RawMessage, code int, msg string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": code, "message": msg},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write(body)
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
