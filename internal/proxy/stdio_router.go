package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anhermon/mcp-trace/v2/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// stdioResponseTimeout is how long a request waits with nothing from the
// subprocess before it fails with 504. It does not run while the subprocess
// is waiting on the client (an unanswered sampling or elicitation request).
var stdioResponseTimeout = 30 * time.Second

// stdioMsg is one subprocess message bound for a client stream. final marks
// the JSON-RPC response that ends a POST.
type stdioMsg struct {
	data  []byte
	final bool
}

// stdioStream is one open client connection that subprocess messages can be
// delivered on: a POST waiting for its response, or the standalone GET stream.
type stdioStream struct {
	msgs          chan stdioMsg
	done          chan struct{} // closed by unregister; senders stop waiting
	sse           bool          // client accepts text/event-stream
	get           bool          // the standalone GET stream
	ctx           context.Context
	sessionID     string          // Mcp-Session-Id the client sent (or was just given)
	clientID      json.RawMessage // the client's own request id (POST only)
	childID       string          // proxy-assigned id the subprocess sees (POST only)
	progressToken string          // progress token the subprocess sees (proxy-assigned)
	clientToken   json.RawMessage // the client's own params._meta.progressToken
	awaiting      atomic.Int32    // server-to-client requests not yet answered
	answered      chan struct{}   // signalled when the client answers one (POST only)
}

// noteAnswered restarts the response timeout of a request whose server
// request was just answered: the subprocess now has the full timeout again.
func (s *stdioStream) noteAnswered() {
	s.awaiting.Add(-1)
	if s.answered == nil {
		return
	}
	select {
	case s.answered <- struct{}{}:
	default:
	}
}

func (s *stdioStream) awaitingClient() bool { return s.awaiting.Load() > 0 }

// stdioServerRequest is a server-to-client request relayed to a client and
// waiting for the client's answer.
type stdioServerRequest struct {
	span   trace.Span // nil when the method is not traced
	method string
	start  time.Time
	stream *stdioStream
}

// stdioRouter tracks open client streams and correlates subprocess messages
// with them. A stdio subprocess has no notion of HTTP requests, so:
//
//   - a response goes to the POST whose (rewritten) id it carries;
//   - notifications/progress goes to the request that set its progressToken;
//   - any other notification or server-to-client request goes to the most
//     recently opened POST event stream, or else the newest GET stream. This
//     matches the usual shape, where a tool asks the client for something
//     while its own tools/call is in flight.
type stdioRouter struct {
	mu         sync.Mutex
	nextID     uint64
	pending    map[string]*stdioStream // child id -> POST waiting for the response
	streams    []*stdioStream          // SSE-capable streams, oldest first
	byClientID map[string]string       // session + client request id -> child id (for cancellation)
	serverReqs map[string]*stdioServerRequest
}

func newStdioRouter() stdioRouter {
	return stdioRouter{
		pending:    map[string]*stdioStream{},
		byClientID: map[string]string{},
		serverReqs: map[string]*stdioServerRequest{},
	}
}

// register adds st and, for a POST, assigns the id the subprocess will see.
func (p *StdioProxy) register(st *stdioStream, clientKey string) uint64 {
	rt := &p.router
	rt.mu.Lock()
	defer rt.mu.Unlock()
	st.done = make(chan struct{})
	var id uint64
	if !st.get {
		rt.nextID++
		id = rt.nextID
		st.childID = strconv.FormatUint(id, 10)
		rt.pending[st.childID] = st
		rt.byClientID[clientKey] = st.childID
		if len(st.clientToken) > 0 {
			// Progress tokens are chosen by each client (SDKs reuse the request
			// id), so they collide across clients just like ids do.
			st.progressToken = "mcp-trace-" + st.childID
		}
	}
	if st.sse {
		rt.streams = append(rt.streams, st)
	}
	return id
}

// unregister removes st. Server-to-client requests it carried that were never
// answered end as abandoned.
func (p *StdioProxy) unregister(st *stdioStream) {
	rt := &p.router
	rt.mu.Lock()
	close(st.done)
	if st.childID != "" {
		delete(rt.pending, st.childID)
		for k, v := range rt.byClientID {
			if v == st.childID {
				delete(rt.byClientID, k)
			}
		}
	}
	for i, s := range rt.streams {
		if s == st {
			rt.streams = append(rt.streams[:i], rt.streams[i+1:]...)
			break
		}
	}
	var orphaned []*stdioServerRequest
	for id, sr := range rt.serverReqs {
		if sr.stream == st {
			orphaned = append(orphaned, sr)
			delete(rt.serverReqs, id)
		}
	}
	rt.mu.Unlock()

	for _, sr := range orphaned {
		endServerRequestSpan(sr, telemetry.StatusAbandoned,
			"client stream closed before the client answered the server request", 0, 0)
	}
}

// pickStream returns the stream for a message not tied to a request id.
// Caller holds rt.mu.
func (rt *stdioRouter) pickStream(progressToken string) *stdioStream {
	if progressToken != "" {
		for i := len(rt.streams) - 1; i >= 0; i-- {
			if rt.streams[i].progressToken == progressToken {
				return rt.streams[i]
			}
		}
	}
	for i := len(rt.streams) - 1; i >= 0; i-- {
		if !rt.streams[i].get {
			return rt.streams[i]
		}
	}
	if n := len(rt.streams); n > 0 {
		return rt.streams[n-1]
	}
	return nil
}

func deliver(st *stdioStream, m stdioMsg) bool {
	select {
	case st.msgs <- m:
		return true
	case <-st.done:
		return false
	}
}

// dispatch routes one message read from the subprocess's stdout.
func (p *StdioProxy) dispatch(data []byte) {
	var msg struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		p.logger.Debug("non-JSON-RPC line from subprocess", "err", err)
		return
	}
	id := IDString(msg.ID)
	if id == "null" {
		id = ""
	}
	rt := &p.router

	switch {
	case msg.Method == "" && id != "": // response to a client request
		rt.mu.Lock()
		st := rt.pending[id]
		delete(rt.pending, id)
		rt.mu.Unlock()
		if st == nil {
			p.logger.Debug("response for no open request", "id", id)
			return
		}
		deliver(st, stdioMsg{data: data, final: true})

	case msg.Method != "" && id != "": // server-to-client request
		rt.mu.Lock()
		st := rt.pickStream("")
		var sr *stdioServerRequest
		if st != nil {
			sr = &stdioServerRequest{method: msg.Method, start: time.Now(), stream: st}
			rt.serverReqs[id] = sr
			st.awaiting.Add(1)
		}
		rt.mu.Unlock()
		if st == nil {
			p.logger.Warn("server-to-client request with no open client stream; answering with an error",
				"method", msg.Method, "id", id)
			p.rejectServerRequest(msg.ID, "no client stream is open to deliver the request")
			return
		}
		if p.filter.ShouldTraceServerRequest(msg.Method) {
			p.mu.Lock()
			name, ver := p.clientName, p.clientVer
			p.mu.Unlock()
			_, sr.span = telemetry.StartSpan(st.ctx, p.tracer, telemetry.SpanAttrs{
				Method:     msg.Method,
				RequestID:  id,
				SessionID:  st.sessionID,
				Target:     "stdio",
				Direction:  telemetry.DirectionServerToClient,
				ClientName: name,
				ClientVer:  ver,
			})
		}
		p.logger.Debug("server request relayed to client", "id", id, "method", msg.Method, "get_stream", st.get)
		if !deliver(st, stdioMsg{data: data}) {
			rt.mu.Lock()
			if rt.serverReqs[id] == sr {
				delete(rt.serverReqs, id)
			}
			rt.mu.Unlock()
			st.awaiting.Add(-1)
			endServerRequestSpan(sr, telemetry.StatusAbandoned, "client stream closed before delivery", 0, 0)
			p.rejectServerRequest(msg.ID, "client stream closed before the request was delivered")
		}

	case msg.Method != "": // notification
		tok := progressTokenOfNotification(msg.Method, msg.Params)
		rt.mu.Lock()
		st := rt.pickStream(tok)
		rt.mu.Unlock()
		if st == nil {
			p.logger.Debug("dropping notification: no open client stream", "method", msg.Method)
			return
		}
		if tok != "" && tok == st.progressToken && len(st.clientToken) > 0 {
			if out, err := setProgressToken(data, false, st.clientToken); err == nil {
				data = out
			}
		}
		deliver(st, stdioMsg{data: data})
	}
}

// rejectServerRequest answers the subprocess so it does not wait forever on a
// request no client can receive.
func (p *StdioProxy) rejectServerRequest(id json.RawMessage, reason string) {
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32603, "message": "mcp-trace: " + reason},
	})
	if err := p.writeChild(body); err != nil {
		p.logger.Debug("could not reject server request", "err", err)
	}
}

// finishServerRequest ends the span of the server-to-client request the
// client is answering with body.
func (p *StdioProxy) finishServerRequest(id string, body []byte) {
	rt := &p.router
	rt.mu.Lock()
	sr := rt.serverReqs[id]
	delete(rt.serverReqs, id)
	rt.mu.Unlock()
	if sr == nil {
		return
	}
	sr.stream.noteAnswered()
	status, errMsg, errCode := telemetry.StatusOK, "", 0
	if resp, err := ParseResponse(body); err == nil {
		var isErr bool
		isErr, errMsg, errCode = IsError(resp)
		if isErr {
			status = telemetry.StatusError
		}
	}
	endServerRequestSpan(sr, status, errMsg, errCode, len(body))
	p.logger.Debug("client answered server request", "id", id, "method", sr.method)
}

func endServerRequestSpan(sr *stdioServerRequest, status, errMsg string, errCode, size int) {
	if sr == nil || sr.span == nil {
		return
	}
	telemetry.EndSpan(sr.span, telemetry.EndAttrs{
		DurationMS:   float64(time.Since(sr.start).Microseconds()) / 1000.0,
		Status:       status,
		ErrMsg:       errMsg,
		ErrCode:      errCode,
		ResponseSize: size,
	})
}

// translateCancel rewrites params.requestId of a client's
// notifications/cancelled to the id the subprocess knows the request by.
func (p *StdioProxy) translateCancel(body []byte, sessionID string) []byte {
	var n struct {
		Params map[string]json.RawMessage `json:"params"`
	}
	if json.Unmarshal(body, &n) != nil || n.Params == nil {
		return body
	}
	p.router.mu.Lock()
	childID, ok := p.router.byClientID[clientKey(sessionID, IDString(n.Params["requestId"]))]
	p.router.mu.Unlock()
	if !ok {
		return body
	}
	n.Params["requestId"] = json.RawMessage(childID)
	params, err := marshalNoEscape(n.Params)
	if err != nil {
		return body
	}
	out, err := setJSONRPCField(body, "params", params)
	if err != nil {
		return body
	}
	return out
}

// setJSONRPCID returns msg with its top-level "id" replaced by id.
func setJSONRPCID(msg []byte, id json.RawMessage) ([]byte, error) {
	return setJSONRPCField(msg, "id", id)
}

func setJSONRPCField(msg []byte, key string, value json.RawMessage) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(msg, &obj); err != nil {
		return nil, err
	}
	obj[key] = value
	return marshalNoEscape(obj)
}

// marshalNoEscape marshals without HTML-escaping <, > and &, so payloads
// pass through as close to byte-identical as re-encoding allows.
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func progressTokenOfNotification(method string, params json.RawMessage) string {
	if method != "notifications/progress" {
		return ""
	}
	var p struct {
		ProgressToken json.RawMessage `json:"progressToken"`
	}
	if len(params) == 0 || json.Unmarshal(params, &p) != nil {
		return ""
	}
	return IDString(p.ProgressToken)
}

// clientKey scopes a client request id to its session: ids are only unique
// per client, so a bare id would let one client cancel another's request.
func clientKey(sessionID, id string) string { return sessionID + "\x00" + id }

// setProgressToken replaces the progress token of a request
// (params._meta.progressToken, inRequest) or of a notifications/progress
// (params.progressToken).
func setProgressToken(msg []byte, inRequest bool, tok json.RawMessage) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(msg, &obj); err != nil {
		return nil, err
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(obj["params"], &params); err != nil {
		return nil, err
	}
	if inRequest {
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(params["_meta"], &meta); err != nil {
			return nil, err
		}
		meta["progressToken"] = tok
		b, err := marshalNoEscape(meta)
		if err != nil {
			return nil, err
		}
		params["_meta"] = b
	} else {
		params["progressToken"] = tok
	}
	b, err := marshalNoEscape(params)
	if err != nil {
		return nil, err
	}
	obj["params"] = b
	return marshalNoEscape(obj)
}

// progressTokenRaw returns params._meta.progressToken of a request as sent.
func progressTokenRaw(params json.RawMessage) json.RawMessage {
	var p struct {
		Meta struct {
			ProgressToken json.RawMessage `json:"progressToken"`
		} `json:"_meta"`
	}
	if len(params) == 0 || json.Unmarshal(params, &p) != nil || string(p.Meta.ProgressToken) == "null" {
		return nil
	}
	return p.Meta.ProgressToken
}
