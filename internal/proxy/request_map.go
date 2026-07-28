package proxy

import (
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// InFlightRequest tracks an in-progress JSON-RPC call.
type InFlightRequest struct {
	Span      trace.Span
	Method    string
	ToolName  string // non-empty only for tools/call
	RequestID string // raw JSON-RPC id, for logging
	SessionID string // MCP session this request belongs to ("" if the server advertises none)
	StartTime time.Time
}

// correlationKey scopes a JSON-RPC id to its MCP session.
//
// JSON-RPC ids are only unique within one client connection: every SDK starts
// counting at 1, so two clients talking to the same proxy both send id 1. Keyed
// on the id alone, the second client's request overwrites the first's map entry
// and the first span is never ended, never evicted, and never exported — while
// the surviving span reports a duration belonging to neither call.
//
// Servers that advertise no session id fall back to id-only keying, which is
// exactly as correlated as it was before — there is nothing else to key on.
func correlationKey(sessionID, id string) string {
	if sessionID == "" {
		return id
	}
	return sessionID + "\x00" + id
}

// RequestMap is a concurrency-safe map of in-flight JSON-RPC requests keyed by
// (session id, JSON-RPC id).
type RequestMap struct {
	mu   sync.Mutex
	reqs map[string]*InFlightRequest
}

// NewRequestMap returns an initialised RequestMap.
func NewRequestMap() *RequestMap {
	return &RequestMap{reqs: make(map[string]*InFlightRequest)}
}

// Store saves an in-flight request under its session-scoped key.
func (m *RequestMap) Store(sessionID, id string, r *InFlightRequest) {
	m.mu.Lock()
	m.reqs[correlationKey(sessionID, id)] = r
	m.mu.Unlock()
}

// Take atomically retrieves and removes an in-flight request.
// Returns nil if not found.
func (m *RequestMap) Take(sessionID, id string) *InFlightRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := correlationKey(sessionID, id)
	r, ok := m.reqs[key]
	if !ok {
		return nil
	}
	delete(m.reqs, key)
	return r
}

// TakeSession removes and returns every in-flight request belonging to one
// session. Used when that session's SSE stream ends: a request still open at
// that point can never be answered, and waiting out the eviction deadline would
// report the deadline as the call's duration.
func (m *RequestMap) TakeSession(sessionID string) []*InFlightRequest {
	if sessionID == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Matched on the key rather than the SessionID field so this can never
	// disagree with how the entry was stored.
	prefix := sessionID + "\x00"
	var out []*InFlightRequest
	for key, r := range m.reqs {
		if strings.HasPrefix(key, prefix) {
			out = append(out, r)
			delete(m.reqs, key)
		}
	}
	return out
}

// EvictStale removes all requests older than timeout and returns them for cleanup.
// Callers are responsible for ending the spans on returned entries.
func (m *RequestMap) EvictStale(timeout time.Duration) []*InFlightRequest {
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	defer m.mu.Unlock()

	var entries []*InFlightRequest
	for key, r := range m.reqs {
		if r.StartTime.Before(cutoff) {
			entries = append(entries, r)
			delete(m.reqs, key)
		}
	}
	return entries
}
