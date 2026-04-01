package proxy

import (
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// InFlightRequest tracks an in-progress JSON-RPC call.
type InFlightRequest struct {
	Span      trace.Span
	Method    string
	ToolName  string // non-empty only for tools/call
	StartTime time.Time
}

// RequestMap is a concurrency-safe map of in-flight JSON-RPC requests keyed by id.
type RequestMap struct {
	mu   sync.Mutex
	reqs map[string]*InFlightRequest
}

// NewRequestMap returns an initialised RequestMap.
func NewRequestMap() *RequestMap {
	return &RequestMap{reqs: make(map[string]*InFlightRequest)}
}

// Store saves an in-flight request.
func (m *RequestMap) Store(id string, r *InFlightRequest) {
	m.mu.Lock()
	m.reqs[id] = r
	m.mu.Unlock()
}

// Take atomically retrieves and removes an in-flight request.
// Returns nil if not found.
func (m *RequestMap) Take(id string) *InFlightRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.reqs[id]
	if !ok {
		return nil
	}
	delete(m.reqs, id)
	return r
}

// StaleEntry is a request that exceeded the span timeout and has been evicted.
type StaleEntry struct {
	ID  string
	Req *InFlightRequest
}

// EvictStale removes all requests older than timeout and returns them for cleanup.
// Callers are responsible for ending the spans on returned entries.
func (m *RequestMap) EvictStale(timeout time.Duration) []*StaleEntry {
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	defer m.mu.Unlock()

	var entries []*StaleEntry
	for id, r := range m.reqs {
		if r.StartTime.Before(cutoff) {
			entries = append(entries, &StaleEntry{ID: id, Req: r})
			delete(m.reqs, id)
		}
	}
	return entries
}
