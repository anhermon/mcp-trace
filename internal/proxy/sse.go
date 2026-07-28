// Package proxy implements the transparent MCP SSE proxy with OTel span emission.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/anhermon/mcp-trace/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	spanTimeout   = 30 * time.Second
	evictInterval = 10 * time.Second
)

// maxSSELine caps a single SSE line. bufio.Scanner defaults to 64KB, which
// silently truncates and then kills the stream on large tool results; file
// reads and search results routinely exceed that.
const maxSSELine = 8 << 20 // 8 MiB

// maxArgsLen caps recorded tool arguments. Trace backends reject or silently
// drop oversized attributes, and a truncated argument still identifies the call.
const maxArgsLen = 2048

// Proxy is the transparent MCP SSE proxy.
type Proxy struct {
	target *url.URL
	filter *Filter
	tracer trace.Tracer
	reqMap *RequestMap
	logger *slog.Logger

	// CaptureArgs records full tool arguments on spans. Off by default: tool
	// arguments are user data and routinely carry paths, queries and secrets.
	// Set before serving.
	CaptureArgs bool

	// sessions maps an MCP session id to what we know about that session.
	// Keyed per session because concurrent clients each get their own endpoint
	// and would otherwise cross-route.
	sessionsMu sync.RWMutex
	sessions   map[string]*session
}

// session is the per-connection state the proxy learns as a client talks to it.
type session struct {
	endpoint   string // upstream POST endpoint from event: endpoint
	clientName string // from initialize params.clientInfo
	clientVer  string
}

// sessionIDOf extracts the MCP session id from an endpoint URL or request URL.
// Returns "" when the URL carries no session id.
//
// Both spellings are checked on purpose: the Python SDK advertises
// `session_id`, the TypeScript SDK advertises `sessionId`. Accepting only one
// silently disables every session-scoped behaviour against half the ecosystem.
func sessionIDOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	q := u.Query()
	if id := q.Get("session_id"); id != "" {
		return id
	}
	return q.Get("sessionId")
}

// New creates a Proxy that forwards to target. The stale-span evictor runs for
// as long as ctx lives.
func New(ctx context.Context, targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*Proxy, error) {
	return newWithEviction(ctx, targetURL, filter, tracer, logger, evictInterval, spanTimeout)
}

// newWithEviction is New with the evictor's timings injectable, so tests do not
// have to wait 30 seconds.
func newWithEviction(ctx context.Context, targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger, interval, timeout time.Duration) (*Proxy, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("parsing target URL: %w", err)
	}
	p := &Proxy{
		target:   u,
		filter:   filter,
		tracer:   tracer,
		reqMap:   NewRequestMap(),
		logger:   logger,
		sessions: make(map[string]*session),
	}
	go p.evictLoop(ctx, interval, timeout)
	return p, nil
}

// evictLoop ends spans whose response never arrived. It is owned by the proxy,
// not by an SSE connection: a request in flight when a stream drops (client
// reconnect, network blip) would otherwise never be ended, never exported, and
// never freed from reqMap. Returning on ctx.Done() is what stops the goroutine —
// ticker.Stop() alone does not.
func (p *Proxy) evictLoop(ctx context.Context, interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, e := range p.reqMap.EvictStale(timeout) {
				p.logger.Warn("evicting stale span", "id", e.RequestID, "session", e.SessionID, "method", e.Method)
				p.endInFlight(e, telemetry.StatusTimeout,
					"no response received within the span deadline")
			}
		}
	}
}

// ServeHTTP dispatches incoming requests.
// GET requests are treated as SSE stream subscriptions.
// POST requests are treated as JSON-RPC tool invocations.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		p.handleSSE(w, r)
		return
	}
	if r.Method == http.MethodPost {
		p.handlePost(w, r)
		return
	}
	// Proxy everything else transparently.
	p.reverseProxy().ServeHTTP(w, r)
}

// handleSSE opens the upstream SSE stream, forwards it to the client, and
// intercepts JSON-RPC responses to close in-flight spans.
func (p *Proxy) handleSSE(w http.ResponseWriter, r *http.Request) {
	upstreamURL := *p.target
	upstreamURL.Path = r.URL.Path
	upstreamURL.RawQuery = r.URL.RawQuery

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstreamURL.String(), nil)
	if err != nil {
		http.Error(w, "upstream request error", http.StatusBadGateway)
		return
	}
	copyHeaders(req.Header, r.Header)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		p.logger.Error("upstream SSE connect failed", "err", err)
		http.Error(w, "upstream connect failed", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Forward status and headers to the client.
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)

	flusher, ok := w.(http.Flusher)
	if !ok {
		p.logger.Warn("ResponseWriter does not support flushing")
	}

	// Forget this connection's session when the stream ends, and close out any
	// call still waiting on it. Those calls can never be answered — the channel
	// their response would have arrived on is gone — so letting them sit until
	// the eviction deadline would report the deadline as the call's duration
	// rather than when it actually broke.
	var sessionID string
	defer func() {
		if sessionID == "" {
			return
		}
		p.sessionsMu.Lock()
		delete(p.sessions, sessionID)
		p.sessionsMu.Unlock()

		// Whose fault the stream ended decides where you look next: the client
		// going away is a client bug, the upstream going away is a server bug.
		reason := "upstream SSE stream closed before the response arrived (server died, restarted, or dropped the connection)"
		if r.Context().Err() != nil {
			reason = "client disconnected before the response arrived"
		}
		for _, e := range p.reqMap.TakeSession(sessionID) {
			p.logger.Warn("stream closed with request in flight",
				"id", e.RequestID, "session", sessionID, "method", e.Method, "reason", reason)
			p.endInFlight(e, telemetry.StatusAbandoned, reason)
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)
	var eventType string

	for scanner.Scan() {
		line := scanner.Text()

		// Forward raw line to client immediately.
		fmt.Fprintln(w, line)
		if flusher != nil {
			flusher.Flush()
		}

		// Parse SSE fields.
		if line == "" {
			// Blank line = end of event.
			eventType = ""
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if sid := p.handleSSEData(eventType, data, sessionID); sid != "" {
				sessionID = sid
			}
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		p.logger.Error("SSE stream error", "err", err)
	}
}

// handleSSEData processes a data payload from the SSE stream. sessionID is the
// session this stream belongs to, learned from the endpoint event that precedes
// every message. It returns the session id if this payload registered one.
func (p *Proxy) handleSSEData(eventType, data, sessionID string) string {
	switch eventType {
	case "endpoint":
		// The MCP server sends the POST endpoint URL in this event. Only
		// endpoints carrying a session id are registered; without one there is
		// nothing to key on, and POSTs fall back to path reconstruction.
		sid := sessionIDOf(data)
		if sid == "" {
			p.logger.Debug("post endpoint has no session id, using path fallback", "url", data)
			return ""
		}
		p.sessionsMu.Lock()
		s, ok := p.sessions[sid]
		if !ok {
			s = &session{}
			p.sessions[sid] = s
		}
		s.endpoint = data
		p.sessionsMu.Unlock()
		p.logger.Debug("discovered post endpoint", "url", data, "session_id", sid)
		return sid

	case "message", "":
		// A JSON-RPC response.
		resp, err := ParseResponse([]byte(data))
		if err != nil || resp.ID == nil {
			return ""
		}
		id := IDString(resp.ID)
		if id == "" {
			return ""
		}
		inflight := p.reqMap.Take(sessionID, id)
		if inflight == nil {
			return ""
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
			ResponseSize: len(data),
		})
		p.logger.Debug("span ended", "id", id, "session", sessionID, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
	}
	return ""
}

// handlePost proxies a JSON-RPC POST, starting a span if the method should be traced.
func (p *Proxy) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body failed", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	// Continue the caller's trace if it sent one; otherwise this becomes a root.
	spanCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	sessionID := sessionIDOf(r.URL.String())

	var spanStarted bool
	rpcReq, err := ParseRequest(body)
	if err != nil {
		// Not JSON-RPC we can read. Forwarded verbatim, but worth a log line:
		// a malformed request produces no span at all, so without this the
		// call is invisible in both the trace and the proxy.
		p.logger.Warn("unparseable JSON-RPC request, forwarding untraced",
			"session", sessionID, "bytes", len(body), "err", err)
	}
	if err == nil {
		// clientInfo is announced once, in initialize, and is the only thing
		// that names the client — record it even when initialize itself is not
		// traced, so later spans on this session can carry it.
		if rpcReq.Method == "initialize" && sessionID != "" {
			p.rememberClient(sessionID, ParseClientInfo(rpcReq.Params))
		}
	}
	if err == nil && p.filter.ShouldTrace(rpcReq.Method) {
		id := IDString(rpcReq.ID)
		if id != "" {
			attrs := telemetry.SpanAttrs{
				Method:    rpcReq.Method,
				RequestID: id,
				SessionID: sessionID,
				Target:    p.target.String(),
			}
			if rpcReq.Method == "tools/call" {
				attrs.ToolName = ToolCallParams(rpcReq.Params)
				attrs.ArgKeys = ToolCallArgKeys(rpcReq.Params)
				if p.CaptureArgs {
					attrs.ArgsJSON = ToolCallArgsJSON(rpcReq.Params, maxArgsLen)
				}
			}
			attrs.ClientName, attrs.ClientVer = p.clientOf(sessionID)

			ctx, span := telemetry.StartSpan(spanCtx, p.tracer, attrs)
			spanCtx = ctx
			p.reqMap.Store(sessionID, id, &InFlightRequest{
				Span:      span,
				Method:    rpcReq.Method,
				ToolName:  attrs.ToolName,
				RequestID: id,
				SessionID: sessionID,
				StartTime: time.Now(),
			})
			spanStarted = true
			p.logger.Debug("span started", "id", id, "session", sessionID, "method", rpcReq.Method, "tool", attrs.ToolName)
		}
	}

	// Determine upstream POST URL from this request's own session, never a
	// shared field — concurrent clients must not cross-route.
	var postEndpoint string
	if sessionID != "" {
		p.sessionsMu.RLock()
		if s := p.sessions[sessionID]; s != nil {
			postEndpoint = s.endpoint
		}
		p.sessionsMu.RUnlock()
	}

	var upstreamURL string
	if postEndpoint != "" {
		// Use the endpoint advertised by the server via event:endpoint.
		if strings.HasPrefix(postEndpoint, "http") {
			upstreamURL = postEndpoint
		} else {
			// Relative URL — resolve against target.
			rel, err := url.Parse(postEndpoint)
			if err == nil {
				upstreamURL = p.target.ResolveReference(rel).String()
			}
		}
	}
	if upstreamURL == "" {
		// Fall back to proxying to target directly.
		u := *p.target
		u.Path = r.URL.Path
		u.RawQuery = r.URL.RawQuery
		upstreamURL = u.String()
	}

	upReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		if spanStarted {
			p.cleanupSpan(sessionID, rpcReq, "upstream request creation failed")
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	copyHeaders(upReq.Header, r.Header)
	upReq.Header.Set("Content-Type", "application/json")
	// Inject after copyHeaders so our span wins over any inbound traceparent.
	otel.GetTextMapPropagator().Inject(spanCtx, propagation.HeaderCarrier(upReq.Header))

	resp, err := http.DefaultClient.Do(upReq)
	if err != nil {
		p.logger.Error("upstream POST failed", "err", err)
		if spanStarted {
			p.cleanupSpan(sessionID, rpcReq, err.Error())
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// A rejected POST never produces a response on the SSE stream, so its span
	// would otherwise hang until the eviction deadline and report the deadline
	// as the call's duration. The rejection is knowable right here.
	if spanStarted && resp.StatusCode >= 400 {
		p.cleanupSpan(sessionID, rpcReq,
			fmt.Sprintf("upstream rejected the request with HTTP %d", resp.StatusCode))
	}

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) cleanupSpan(sessionID string, req *RPCRequest, errMsg string) {
	if req == nil {
		return
	}
	inflight := p.reqMap.Take(sessionID, IDString(req.ID))
	if inflight == nil {
		return
	}
	p.endInFlight(inflight, telemetry.StatusError, errMsg)
}

// endInFlight closes a span that never got a response, with the real elapsed
// time rather than whatever deadline noticed it.
func (p *Proxy) endInFlight(e *InFlightRequest, status, errMsg string) {
	telemetry.EndSpan(e.Span, telemetry.EndAttrs{
		DurationMS: float64(time.Since(e.StartTime).Microseconds()) / 1000.0,
		Status:     status,
		ErrMsg:     errMsg,
		ToolName:   e.ToolName,
	})
}

// rememberClient records the client identity announced for a session.
func (p *Proxy) rememberClient(sessionID string, info ClientInfo) {
	if info.Name == "" {
		return
	}
	p.sessionsMu.Lock()
	defer p.sessionsMu.Unlock()
	s, ok := p.sessions[sessionID]
	if !ok {
		s = &session{}
		p.sessions[sessionID] = s
	}
	s.clientName, s.clientVer = info.Name, info.Version
}

// clientOf returns the client name and version recorded for a session.
func (p *Proxy) clientOf(sessionID string) (string, string) {
	if sessionID == "" {
		return "", ""
	}
	p.sessionsMu.RLock()
	defer p.sessionsMu.RUnlock()
	if s := p.sessions[sessionID]; s != nil {
		return s.clientName, s.clientVer
	}
	return "", ""
}

func (p *Proxy) reverseProxy() *httputil.ReverseProxy {
	return httputil.NewSingleHostReverseProxy(p.target)
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		// Skip hop-by-hop headers.
		switch strings.ToLower(k) {
		case "connection", "keep-alive", "proxy-authenticate",
			"proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade":
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
