// Package proxy implements the transparent MCP SSE proxy with OTel span emission.
package proxy

import (
	"bufio"
	"bytes"
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

const spanTimeout = 30 * time.Second

// maxSSELine caps a single SSE line. bufio.Scanner defaults to 64KB, which
// silently truncates and then kills the stream on large tool results; file
// reads and search results routinely exceed that.
const maxSSELine = 8 << 20 // 8 MiB

// Proxy is the transparent MCP SSE proxy.
type Proxy struct {
	target *url.URL
	filter *Filter
	tracer trace.Tracer
	reqMap *RequestMap
	logger *slog.Logger

	// sessions maps an MCP session id to the upstream POST endpoint advertised
	// for that session via event: endpoint. Keyed per session because concurrent
	// clients each get their own endpoint and would otherwise cross-route.
	sessionsMu sync.RWMutex
	sessions   map[string]string
}

// sessionIDOf extracts the MCP session id from an endpoint URL or request URL.
// Returns "" when the URL carries no session id.
func sessionIDOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("session_id")
}

// New creates a Proxy that forwards to target.
func New(targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*Proxy, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("parsing target URL: %w", err)
	}
	return &Proxy{
		target:   u,
		filter:   filter,
		tracer:   tracer,
		reqMap:   NewRequestMap(),
		logger:   logger,
		sessions: make(map[string]string),
	}, nil
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

	// Start stale-span eviction ticker. done closes when this connection ends —
	// ticker.Stop() alone does not close the channel, so the goroutine would leak.
	ticker := time.NewTicker(10 * time.Second)
	done := make(chan struct{})
	defer func() {
		ticker.Stop()
		close(done)
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for _, e := range p.reqMap.EvictStale(spanTimeout) {
					p.logger.Warn("evicting stale span", "id", e.ID, "method", e.Req.Method)
					telemetry.EndSpanTimeout(e.Req.Span, e.Req.ToolName)
				}
			}
		}
	}()

	// Forget this connection's session endpoint when the stream ends.
	var sessionID string
	defer func() {
		if sessionID != "" {
			p.sessionsMu.Lock()
			delete(p.sessions, sessionID)
			p.sessionsMu.Unlock()
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
			if sid := p.handleSSEData(eventType, data); sid != "" {
				sessionID = sid
			}
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		p.logger.Error("SSE stream error", "err", err)
	}
}

// handleSSEData processes a data payload from the SSE stream.
// It returns the session id if this payload registered a session endpoint.
func (p *Proxy) handleSSEData(eventType, data string) string {
	switch eventType {
	case "endpoint":
		// The MCP server sends the POST endpoint URL in this event. Only
		// endpoints carrying a session id are registered; without one there is
		// nothing to key on, and POSTs fall back to path reconstruction.
		sid := sessionIDOf(data)
		if sid == "" {
			p.logger.Debug("post endpoint has no session_id, using path fallback", "url", data)
			return ""
		}
		p.sessionsMu.Lock()
		p.sessions[sid] = data
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
		inflight := p.reqMap.Take(id)
		if inflight == nil {
			return ""
		}

		durationMS := float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0
		isErr, errMsg := IsError(resp)
		telemetry.EndSpan(inflight.Span, durationMS, isErr, errMsg, inflight.ToolName)
		p.logger.Debug("span ended", "id", id, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
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

	var spanStarted bool
	rpcReq, err := ParseRequest(body)
	if err == nil && p.filter.ShouldTrace(rpcReq.Method) {
		id := IDString(rpcReq.ID)
		if id != "" {
			toolName := ""
			if rpcReq.Method == "tools/call" {
				toolName = ToolCallParams(rpcReq.Params)
			}
			ctx, span := telemetry.StartSpan(spanCtx, p.tracer, telemetry.SpanAttrs{
				Method:    rpcReq.Method,
				ToolName:  toolName,
				RequestID: id,
				Target:    p.target.String(),
			})
			spanCtx = ctx
			p.reqMap.Store(id, &InFlightRequest{
				Span:      span,
				Method:    rpcReq.Method,
				ToolName:  toolName,
				StartTime: time.Now(),
			})
			spanStarted = true
			p.logger.Debug("span started", "id", id, "method", rpcReq.Method, "tool", toolName)
		}
	}

	// Determine upstream POST URL from this request's own session, never a
	// shared field — concurrent clients must not cross-route.
	var postEndpoint string
	if sid := sessionIDOf(r.URL.String()); sid != "" {
		p.sessionsMu.RLock()
		postEndpoint = p.sessions[sid]
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
			p.cleanupSpan(rpcReq, "upstream request creation failed")
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
			p.cleanupSpan(rpcReq, err.Error())
		}
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (p *Proxy) cleanupSpan(req *RPCRequest, errMsg string) {
	if req == nil {
		return
	}
	id := IDString(req.ID)
	inflight := p.reqMap.Take(id)
	if inflight == nil {
		return
	}
	telemetry.EndSpan(inflight.Span, 0, true, errMsg, inflight.ToolName)
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
