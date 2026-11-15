// Package proxy implements Streamable HTTP transport for MCP.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/anhermon/mcp-trace/v2/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// StreamableProxy handles Streamable HTTP transport, where both request and
// response happen on a single POST endpoint.
type StreamableProxy struct {
	target      string
	filter      *Filter
	tracer      trace.Tracer
	reqMap      *RequestMap
	logger      *slog.Logger
	CaptureArgs bool
	clientName  string
	clientVer   string

	// serverReqs holds server-to-client requests (sampling/createMessage,
	// elicitation/create, roots/list, ...) relayed on an event stream and
	// still waiting for the client's answer, keyed by (session, request id).
	serverReqs *RequestMap
}

// NewStreamableProxy creates a proxy for Streamable HTTP transport.
func NewStreamableProxy(ctx context.Context, targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*StreamableProxy, error) {
	return &StreamableProxy{
		target:     targetURL,
		filter:     filter,
		tracer:     tracer,
		reqMap:     NewRequestMap(),
		serverReqs: NewRequestMap(),
		logger:     logger,
	}, nil
}

// mcpSessionHeader is the Streamable HTTP session header. Header lookups are
// case-insensitive, so this also matches mcp-session-id.
const mcpSessionHeader = "Mcp-Session-Id"

func (p *StreamableProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		p.handlePost(w, r)
	case http.MethodGet, http.MethodDelete:
		// GET opens the standalone server-to-client event stream; DELETE ends
		// the session. Both are forwarded unchanged: the upstream decides
		// whether it supports them (405 is a valid answer to either).
		p.handleOther(w, r)
	default:
		http.Error(w, "method not allowed for streamable transport", http.StatusMethodNotAllowed)
	}
}

// handleOther forwards a GET or DELETE. A GET stream can carry server-to-client
// requests, so its events are relayed as they arrive and those requests traced.
func (p *StreamableProxy) handleOther(w http.ResponseWriter, r *http.Request) {
	method := http.MethodGet
	if r.Method == http.MethodDelete {
		method = http.MethodDelete
	}
	upReq, err := http.NewRequestWithContext(r.Context(), method, p.target, nil)
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	copyHeaders(upReq.Header, r.Header)
	spanCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	resp, err := http.DefaultClient.Do(upReq)
	if err != nil {
		p.logger.Error("upstream request failed", "method", method, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	forwardResponseHeaders(w, resp)
	fw := newFlushWriter(w)
	if resp.StatusCode < 400 && isEventStream(resp.Header.Get("Content-Type")) {
		p.relayEventStream(fw, resp.Body, spanCtx, sessionOf(r, resp), nil)
		return
	}
	_, _ = io.Copy(fw, resp.Body)
}

// sessionOf returns the MCP session id for an exchange: the client's header,
// or the one the upstream just assigned (initialize).
func sessionOf(r *http.Request, resp *http.Response) string {
	if sid := r.Header.Get(mcpSessionHeader); sid != "" {
		return sid
	}
	if resp != nil {
		return resp.Header.Get(mcpSessionHeader)
	}
	return ""
}

func forwardResponseHeaders(w http.ResponseWriter, resp *http.Response) {
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	// Send the headers now: an event stream (notably the GET stream) can stay
	// silent for a long time, and the client waits for headers before it
	// treats the stream as open.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func (p *StreamableProxy) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body failed", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Continue the caller's trace if it sent one; otherwise this becomes a root.
	spanCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	var spanStarted bool
	var inflight *InFlightRequest

	rpcReq, err := ParseRequest(body)
	if err != nil {
		p.logger.Warn("unparseable JSON-RPC request, forwarding untraced",
			"bytes", len(body), "err", err)
	}

	if err == nil && rpcReq.Method == "" && len(rpcReq.ID) > 0 {
		// The client answering a server-to-client request (sampling,
		// elicitation) relayed earlier on an event stream.
		p.finishServerRequest(r.Header.Get(mcpSessionHeader), body)
	}

	if err == nil {
		// Remember client info from initialize
		if rpcReq.Method == "initialize" {
			info := ParseClientInfo(rpcReq.Params)
			p.clientName, p.clientVer = info.Name, info.Version
		}
	}

	if err == nil && rpcReq.Method != "" && p.filter.ShouldTrace(rpcReq.Method) {
		id := IDString(rpcReq.ID)
		if id != "" {
			attrs := telemetry.SpanAttrs{
				Method:     rpcReq.Method,
				RequestID:  id,
				Target:     p.target,
				ClientName: p.clientName,
				ClientVer:  p.clientVer,
			}
			if rpcReq.Method == "tools/call" {
				attrs.ToolName = ToolCallParams(rpcReq.Params)
				attrs.ArgKeys = ToolCallArgKeys(rpcReq.Params)
				if p.CaptureArgs {
					attrs.ArgsJSON = ToolCallArgsJSON(rpcReq.Params, maxArgsLen)
				}
			}

			ctx, span := telemetry.StartSpan(spanCtx, p.tracer, attrs)
			spanCtx = ctx
			inflight = &InFlightRequest{
				Span:      span,
				Method:    rpcReq.Method,
				ToolName:  attrs.ToolName,
				RequestID: id,
				StartTime: time.Now(),
			}
			spanStarted = true
			p.logger.Debug("span started", "id", id, "method", rpcReq.Method, "tool", attrs.ToolName)
		}
	}

	// Forward request to upstream
	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, p.target, bytes.NewReader(body))
	if err != nil {
		if spanStarted {
			p.endInFlight(inflight, telemetry.StatusError, "upstream request creation failed")
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	copyHeaders(upReq.Header, r.Header)
	upReq.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(spanCtx, propagation.HeaderCarrier(upReq.Header))

	resp, err := http.DefaultClient.Do(upReq)
	if err != nil {
		p.logger.Error("upstream POST failed", "err", err)
		if spanStarted {
			p.endInFlight(inflight, telemetry.StatusError, err.Error())
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		if spanStarted {
			p.endInFlight(inflight, telemetry.StatusError,
				fmt.Sprintf("upstream rejected the request with HTTP %d", resp.StatusCode))
		}
	}

	forwardResponseHeaders(w, resp)

	// Every write is flushed: a text/event-stream answer can carry a server
	// request (sampling/createMessage, elicitation/create) that the server
	// blocks on until the client answers on a separate POST. Buffering that
	// event deadlocks the call until the client's request timeout.
	fw := newFlushWriter(w)
	contentType := resp.Header.Get("Content-Type")

	// application/json is one document. The Python SDK's default Streamable
	// HTTP mode (json_response=False), and the TypeScript SDK, answer the same
	// POST with text/event-stream instead, which is relayed event by event.
	switch {
	case resp.StatusCode < 400 && isEventStream(contentType):
		if !spanStarted {
			inflight = nil
		}
		p.relayEventStream(fw, resp.Body, spanCtx, sessionOf(r, resp), inflight)
	case spanStarted && resp.StatusCode < 400:
		p.copyAndParseJSON(fw, resp.Body, inflight)
	default:
		_, _ = io.Copy(fw, resp.Body)
	}
}

// flushWriter flushes after every write so streamed events reach the client
// as soon as the upstream sends them.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func newFlushWriter(w http.ResponseWriter) *flushWriter {
	f, _ := w.(http.Flusher)
	return &flushWriter{w: w, f: f}
}

func (fw *flushWriter) Write(b []byte) (int, error) {
	n, err := fw.w.Write(b)
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}

// copyAndParseJSON forwards a single application/json response unchanged and
// closes inflight from it.
func (p *StreamableProxy) copyAndParseJSON(w io.Writer, r io.Reader, inflight *InFlightRequest) {
	var buf bytes.Buffer
	written, _ := io.Copy(w, io.TeeReader(r, &buf))
	if written == 0 {
		return
	}
	data := buf.Bytes()
	resp, err := ParseResponse(data)
	if err != nil {
		p.logger.Debug("could not parse response as JSON-RPC", "err", err)
		return
	}
	p.finishIfMatch(inflight, resp, len(data))
}

// isEventStream reports whether the upstream Content-Type is SSE. Parameters
// such as charset=utf-8, which sse-starlette adds, do not change the media type.
func isEventStream(contentType string) bool {
	media, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return strings.EqualFold(media, "text/event-stream")
}

// relayEventStream copies an upstream text/event-stream to w unchanged, one
// line at a time, and inspects each complete event as it passes:
//
//   - the JSON-RPC response for inflight (if any) ends that span;
//   - a server-to-client request (it has both "method" and "id") starts a span
//     that stays open until the client's answer arrives on a POST.
//
// A priming event (empty data), notifications and responses for other ids are
// only forwarded. If the stream ends with no matching response, inflight is
// still ended so it is exported instead of leaking, and any server request
// first seen on this stream and never answered is ended as abandoned.
func (p *StreamableProxy) relayEventStream(w io.Writer, body io.Reader, parent context.Context, sessionID string, inflight *InFlightRequest) {
	br := bufio.NewReaderSize(body, 64*1024)

	var (
		dataLines []string
		dataBytes int
		ended     bool
		opened    []string // ids of server requests first seen on this stream
	)
	finishEvent := func() {
		payload := strings.Join(dataLines, "\n")
		dataLines, dataBytes = nil, 0
		if strings.TrimSpace(payload) == "" {
			return
		}
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal([]byte(payload), &msg) != nil || len(msg.ID) == 0 || IDString(msg.ID) == "" {
			return // notification, batch, or not JSON-RPC
		}
		if msg.Method != "" {
			if id := p.startServerRequest(parent, sessionID, msg.Method, IDString(msg.ID)); id != "" {
				opened = append(opened, id)
			}
			return
		}
		if inflight == nil || ended {
			return
		}
		resp, err := ParseResponse([]byte(payload))
		if err != nil {
			return
		}
		if p.finishIfMatch(inflight, resp, len(payload)) {
			ended = true
		}
	}

	for {
		// Each line is forwarded as soon as it is complete. ReadBytes keeps
		// the original line ending, so the client gets the bytes unchanged.
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := w.Write(line); werr != nil {
				p.logger.Debug("client went away during event stream", "err", werr)
				break
			}
			if len(line) > maxSSELine {
				dataLines, dataBytes = nil, maxSSELine+1 // forwarded, too large to parse
			} else {
				dataBytes = p.parseSSELine(strings.TrimRight(string(line), "\r\n"), &dataLines, dataBytes, finishEvent)
			}
		}
		if err != nil {
			if err != io.EOF {
				p.logger.Debug("reading streamable event stream", "err", err)
			}
			break
		}
	}
	if len(dataLines) > 0 {
		finishEvent()
	}
	if inflight != nil && !ended {
		p.endInFlight(inflight, telemetry.StatusAbandoned,
			"streamable HTTP event stream closed before a JSON-RPC response arrived")
	}
	for _, id := range opened {
		if e := p.serverReqs.Take(sessionID, id); e != nil {
			p.endInFlight(e, telemetry.StatusAbandoned,
				"event stream closed before the client answered the server request")
		}
	}
}

// parseSSELine applies one SSE line (without its line ending) to the event
// being assembled and returns the new byte count of its data. A blank line
// dispatches the event. Events whose data grows past maxSSELine are dropped
// from parsing (they are still forwarded).
func (p *StreamableProxy) parseSSELine(line string, dataLines *[]string, dataBytes int, dispatch func()) int {
	switch {
	case line == "":
		dispatch()
		return 0
	case strings.HasPrefix(line, "data:"):
		v := sseFieldValue(line, "data:")
		if dataBytes+len(v) > maxSSELine {
			*dataLines = nil
			return maxSSELine + 1
		}
		*dataLines = append(*dataLines, v)
		return dataBytes + len(v)
	default:
		// comments (":"), event:, id:, retry: carry no JSON-RPC payload.
		return dataBytes
	}
}

// startServerRequest opens a span for a server-to-client request seen on an
// event stream and remembers it until the client answers. It returns the
// request id when a span was started.
func (p *StreamableProxy) startServerRequest(parent context.Context, sessionID, method, id string) string {
	if !p.filter.ShouldTraceServerRequest(method) {
		return ""
	}
	_, span := telemetry.StartSpan(parent, p.tracer, telemetry.SpanAttrs{
		Method:     method,
		RequestID:  id,
		SessionID:  sessionID,
		Target:     p.target,
		Direction:  telemetry.DirectionServerToClient,
		ClientName: p.clientName,
		ClientVer:  p.clientVer,
	})
	p.serverReqs.Store(sessionID, id, &InFlightRequest{
		Span:      span,
		Method:    method,
		RequestID: id,
		SessionID: sessionID,
		StartTime: time.Now(),
	})
	p.logger.Debug("server request relayed to client", "id", id, "method", method, "session", sessionID)
	return id
}

// finishServerRequest ends the span of the server-to-client request that the
// client is answering with body. Unknown ids are ignored.
func (p *StreamableProxy) finishServerRequest(sessionID string, body []byte) {
	resp, err := ParseResponse(body)
	if err != nil || resp.ID == nil {
		return
	}
	id := IDString(resp.ID)
	e := p.serverReqs.Take(sessionID, id)
	if e == nil {
		return
	}
	isErr, errMsg, errCode := IsError(resp)
	status := telemetry.StatusOK
	if isErr {
		status = telemetry.StatusError
	}
	durationMS := float64(time.Since(e.StartTime).Microseconds()) / 1000.0
	telemetry.EndSpan(e.Span, telemetry.EndAttrs{
		DurationMS:   durationMS,
		Status:       status,
		ErrMsg:       errMsg,
		ErrCode:      errCode,
		ResponseSize: len(body),
	})
	p.logger.Debug("client answered server request", "id", id, "method", e.Method, "duration_ms", durationMS, "error", isErr)
}

// sseFieldValue returns an SSE field value, dropping the single optional space
// the spec allows after the colon and nothing else.
func sseFieldValue(line, prefix string) string {
	v := strings.TrimPrefix(line, prefix)
	return strings.TrimPrefix(v, " ")
}

// finishIfMatch ends inflight when resp is the JSON-RPC response for it.
// Returns false when the message is for a different id, so the caller can
// keep reading.
func (p *StreamableProxy) finishIfMatch(inflight *InFlightRequest, resp *RPCResponse, responseSize int) bool {
	if resp == nil || resp.ID == nil {
		return false
	}
	id := IDString(resp.ID)
	if id == "" || id != inflight.RequestID {
		return false
	}

	durationMS := float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0
	isErr, errMsg, errCode := IsError(resp)
	status := telemetry.StatusOK
	if isErr {
		status = telemetry.StatusError
	}

	telemetry.EndSpan(inflight.Span, telemetry.EndAttrs{
		DurationMS:   durationMS,
		Status:       status,
		ErrMsg:       errMsg,
		ErrCode:      errCode,
		ToolName:     inflight.ToolName,
		ResponseSize: responseSize,
	})
	p.logger.Debug("span ended", "id", id, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
	return true
}

func (p *StreamableProxy) endInFlight(e *InFlightRequest, status, errMsg string) {
	telemetry.EndSpan(e.Span, telemetry.EndAttrs{
		DurationMS: float64(time.Since(e.StartTime).Microseconds()) / 1000.0,
		Status:     status,
		ErrMsg:     errMsg,
		ToolName:   e.ToolName,
	})
}
