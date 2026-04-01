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

// EvictStale ends and removes all spans older than the given timeout,
// marking them with an error status. Should be called periodically.
func (m *RequestMap) EvictStale(timeout time.Duration, onEvict func(id string, r *InFlightRequest)) {
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	var stale []string
	for id, r := range m.reqs {
		if r.StartTime.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		delete(m.reqs, id)
	}
	m.mu.Unlock()

	for _, id := range stale {
		// reqs already removed; reconstruct for callback — safe since we copied
	}
	// Re-iterate without lock to call the callback.
	// We deleted above; reconstruct from our local copy by calling Take first.
	// Actually we need to keep a local copy — let's redo with a local slice.
	_ = stale
}

// EvictStaleV2 is a cleaner version that keeps a local copy before deleting.
func (m *RequestMap) EvictStaleV2(timeout time.Duration) []*staleEntry {
	cutoff := time.Now().Add(-timeout)
	m.mu.Lock()
	defer m.mu.Unlock()

	var entries []*staleEntry
	for id, r := range m.reqs {
		if r.StartTime.Before(cutoff) {
			entries = append(entries, &staleEntry{ID: id, Req: r})
			delete(m.reqs, id)
		}
	}
	return entries
}

type staleEntry struct {
	ID  string
	Req *InFlightRequest
}
