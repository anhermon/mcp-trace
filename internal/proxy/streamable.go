// Package proxy implements Streamable HTTP transport for MCP.
package proxy

import (
	"bufio"
	"bytes"
	"context"
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

	// Stream the response and parse JSON-RPC responses.
	// application/json is one document. The Python SDK's default Streamable
	// HTTP mode (json_response=False) answers the same POST with
	// text/event-stream instead, and that body is not a JSON document.
	if spanStarted && resp.StatusCode < 400 {
		p.streamAndParseResponse(w, resp.Body, resp.Header.Get("Content-Type"), inflight)
	} else {
		// Just forward without parsing
		_, _ = io.Copy(w, resp.Body)
	}
}

// streamAndParseResponse streams the response body to the client while parsing
// JSON-RPC responses to close spans.
func (p *StreamableProxy) streamAndParseResponse(w http.ResponseWriter, r io.Reader, contentType string, inflight *InFlightRequest) {
	var buf bytes.Buffer
	tee := io.TeeReader(r, &buf)

	// Stream to client. Bytes are copied unchanged; parsing uses the copy.
	written, _ := io.Copy(w, tee)

	if written == 0 {
		return
	}

	data := buf.Bytes()
	if isEventStream(contentType) {
		// Default Streamable HTTP (Python SDK json_response=False, and the
		// same shape from sse-starlette): the POST body is an SSE stream.
		// Each JSON-RPC message is one event, typically
		//
		//	event: message
		//	data: {"jsonrpc":"2.0","id":1,"result":...}
		//
		// A priming event (empty data) and notifications can precede the
		// response. Parsing the whole body as one JSON document fails, and
		// returning here used to leave the span open so it was never exported.
		p.endSpanFromEventStream(data, inflight)
		return
	}

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

// endSpanFromEventStream closes inflight from the first SSE event whose data
// is the JSON-RPC response for that request. Other events (priming, progress
// notifications, server requests with a different id) are ignored. If the
// stream ends with no matching response, the span is still ended so it is
// exported instead of leaking.
func (p *StreamableProxy) endSpanFromEventStream(data []byte, inflight *InFlightRequest) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)

	var dataLines []string
	ended := false
	finishEvent := func() {
		payload := strings.Join(dataLines, "\n")
		dataLines = nil
		if ended || strings.TrimSpace(payload) == "" {
			return
		}
		resp, err := ParseResponse([]byte(payload))
		if err != nil || resp.ID == nil {
			return
		}
		if p.finishIfMatch(inflight, resp, len(payload)) {
			ended = true
		}
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			finishEvent()
			continue
		}
		if strings.HasPrefix(line, ":") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:") {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, sseFieldValue(line, "data:"))
		}
	}
	if len(dataLines) > 0 {
		finishEvent()
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		p.logger.Debug("reading streamable event stream", "err", err)
	}
	if !ended {
		p.endInFlight(inflight, telemetry.StatusAbandoned,
			"streamable HTTP event stream closed before a JSON-RPC response arrived")
	}
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
