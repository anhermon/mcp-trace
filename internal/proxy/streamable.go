// Package proxy implements Streamable HTTP transport for MCP.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/anhermon/mcp-trace/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// StreamableProxy handles Streamable HTTP transport, where both request and
// response happen on a single POST endpoint.
type StreamableProxy struct {
	target       string
	filter       *Filter
	tracer       trace.Tracer
	reqMap       *RequestMap
	logger       *slog.Logger
	CaptureArgs  bool
	clientName   string
	clientVer    string
}

// NewStreamableProxy creates a proxy for Streamable HTTP transport.
func NewStreamableProxy(ctx context.Context, targetURL string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*StreamableProxy, error) {
	return &StreamableProxy{
		target: targetURL,
		filter: filter,
		tracer: tracer,
		reqMap: NewRequestMap(),
		logger: logger,
	}, nil
}

func (p *StreamableProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST is supported for streamable transport", http.StatusMethodNotAllowed)
		return
	}

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

	if err == nil {
		// Remember client info from initialize
		if rpcReq.Method == "initialize" {
			info := ParseClientInfo(rpcReq.Params)
			p.clientName, p.clientVer = info.Name, info.Version
		}
	}

	if err == nil && p.filter.ShouldTrace(rpcReq.Method) {
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

	// Forward response headers
	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// Stream the response and parse JSON-RPC responses
	if spanStarted && resp.StatusCode < 400 {
		p.streamAndParseResponse(w, resp.Body, inflight)
	} else {
		// Just forward without parsing
		_, _ = io.Copy(w, resp.Body)
	}
}

// streamAndParseResponse streams the response body to the client while parsing
// JSON-RPC responses to close spans.
func (p *StreamableProxy) streamAndParseResponse(w http.ResponseWriter, r io.Reader, inflight *InFlightRequest) {
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)

	// Stream to client
	written, _ := io.Copy(w, tee)

	if written == 0 {
		return
	}

	// Parse the complete response
	data := buf.Bytes()
	resp, err := ParseResponse(data)
	if err != nil {
		p.logger.Debug("could not parse response as JSON-RPC", "err", err)
		return
	}

	if resp.ID == nil {
		return
	}

	id := IDString(resp.ID)
	if id == "" || id != inflight.RequestID {
		return
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
	p.logger.Debug("span ended", "id", id, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
}

// streamChunkedResponse handles streaming responses that may arrive in chunks.
// For streamable HTTP, some servers may send multiple newline-delimited JSON-RPC messages.
func (p *StreamableProxy) streamChunkedResponse(w http.ResponseWriter, r io.Reader, inflight *InFlightRequest) {
	flusher, canFlush := w.(http.Flusher)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)

	var foundResponse bool
	for scanner.Scan() {
		line := scanner.Bytes()

		// Forward to client
		w.Write(line)
		w.Write([]byte("\n"))
		if canFlush {
			flusher.Flush()
		}

		// Try to parse as JSON-RPC response
		if !foundResponse {
			resp, err := ParseResponse(line)
			if err == nil && resp.ID != nil {
				id := IDString(resp.ID)
				if id == inflight.RequestID {
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
						ResponseSize: len(line),
					})
					p.logger.Debug("span ended", "id", id, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
					foundResponse = true
				}
			}
		}
	}

	if err := scanner.Err(); err != nil && err != io.EOF {
		p.logger.Error("streaming response error", "err", err)
	}

	// If we never found a matching response, end the span as abandoned
	if !foundResponse {
		p.endInFlight(inflight, telemetry.StatusAbandoned, "response stream ended without matching JSON-RPC response")
	}
}

func (p *StreamableProxy) endInFlight(e *InFlightRequest, status, errMsg string) {
	telemetry.EndSpan(e.Span, telemetry.EndAttrs{
		DurationMS: float64(time.Since(e.StartTime).Microseconds()) / 1000.0,
		Status:     status,
		ErrMsg:     errMsg,
		ToolName:   e.ToolName,
	})
}
