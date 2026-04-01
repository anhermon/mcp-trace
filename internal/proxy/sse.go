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

	"github.com/paperclipai/mcp-trace/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

const spanTimeout = 30 * time.Second

// Proxy is the transparent MCP SSE proxy.
type Proxy struct {
	target *url.URL
	filter *Filter
	tracer trace.Tracer
	reqMap *RequestMap
	logger *slog.Logger

	// postEndpoint is the upstream POST URL discovered from event: endpoint in the SSE stream.
	postEndpointMu sync.RWMutex
	postEndpoint   string
}

// New creates a Proxy that forwards to target.
func New(targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*Proxy, error) {
	u, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("parsing target URL: %w", err)
	}
	return &Proxy{
		target: u,
		filter: filter,
		tracer: tracer,
		reqMap: NewRequestMap(),
		logger: logger,
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

	// Start stale-span eviction ticker.
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			stale := p.reqMap.EvictStale(spanTimeout)
			for _, e := range stale {
				p.logger.Warn("evicting stale span", "id", e.ID, "method", e.Req.Method)
				telemetry.EndSpanTimeout(e.Req.Span, e.Req.ToolName)
			}
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
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
			p.handleSSEData(eventType, data)
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		p.logger.Error("SSE stream error", "err", err)
	}
}

// handleSSEData processes a data payload from the SSE stream.
func (p *Proxy) handleSSEData(eventType, data string) {
	switch eventType {
	case "endpoint":
		// The MCP server sends the POST endpoint URL in this event.
		p.postEndpointMu.Lock()
		p.postEndpoint = data
		p.postEndpointMu.Unlock()
		p.logger.Debug("discovered post endpoint", "url", data)

	case "message", "":
		// A JSON-RPC response.
		resp, err := ParseResponse([]byte(data))
		if err != nil || resp.ID == nil {
			return
		}
		id := IDString(resp.ID)
		if id == "" {
			return
		}
		inflight := p.reqMap.Take(id)
		if inflight == nil {
			return
		}

		durationMS := float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0
		isErr, errMsg := IsError(resp)
		telemetry.EndSpan(inflight.Span, durationMS, isErr, errMsg, inflight.ToolName)
		p.logger.Debug("span ended", "id", id, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
	}
}

// handlePost proxies a JSON-RPC POST, starting a span if the method should be traced.
func (p *Proxy) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body failed", http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	var spanStarted bool
	rpcReq, err := ParseRequest(body)
	if err == nil && p.filter.ShouldTrace(rpcReq.Method) {
		id := IDString(rpcReq.ID)
		if id != "" {
			toolName := ""
			if rpcReq.Method == "tools/call" {
				toolName = ToolCallParams(rpcReq.Params)
			}
			ctx, span := telemetry.StartSpan(context.Background(), p.tracer, telemetry.SpanAttrs{
				Method:    rpcReq.Method,
				ToolName:  toolName,
				RequestID: id,
				Target:    p.target.String(),
			})
			_ = ctx
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

	// Determine upstream POST URL.
	p.postEndpointMu.RLock()
	postEndpoint := p.postEndpoint
	p.postEndpointMu.RUnlock()

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
