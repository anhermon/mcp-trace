package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
)

func noopSpan() *InFlightRequest {
	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "test")
	return &InFlightRequest{Span: span, StartTime: time.Now()}
}

func TestRequestMap_StoreAndTake(t *testing.T) {
	m := NewRequestMap()
	r := noopSpan()
	m.Store("sess", "1", r)
	got := m.Take("sess", "1")
	if got != r {
		t.Error("Take did not return stored request")
	}
	// Second take should return nil.
	if m.Take("sess", "1") != nil {
		t.Error("expected nil on second Take")
	}
}

func TestRequestMap_MissingKey(t *testing.T) {
	m := NewRequestMap()
	if m.Take("sess", "nope") != nil {
		t.Error("expected nil for missing key")
	}
}

func TestRequestMap_Concurrent(t *testing.T) {
	m := NewRequestMap()
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := string(rune('A' + id%26))
			m.Store(key, "1", noopSpan())
			m.Take(key, "1")
		}(i)
	}
	wg.Wait()
}

func TestRequestMap_EvictStale(t *testing.T) {
	m := NewRequestMap()
	old := &InFlightRequest{
		Span:      noopSpan().Span,
		RequestID: "old",
		StartTime: time.Now().Add(-60 * time.Second),
	}
	m.Store("sess", "old", old)
	m.Store("sess", "fresh", noopSpan())

	stale := m.EvictStale(30 * time.Second)
	if len(stale) != 1 || stale[0].RequestID != "old" {
		t.Errorf("expected 1 stale entry with id 'old', got %v", stale)
	}
	// Fresh entry should still be there.
	if m.Take("sess", "fresh") == nil {
		t.Error("fresh entry should still be in map")
	}
}

// TestRequestMap_SessionsDoNotCollide is the regression test for the bug this
// keying exists to prevent: every MCP SDK numbers its JSON-RPC ids from 1, so
// two concurrent clients both send id "1". Keyed on the id alone, the second
// client's request evicted the first's from the map, and the first span was
// never ended, never evicted and never exported — the call vanished from the
// trace while the surviving span reported a duration belonging to neither call.
func TestRequestMap_SessionsDoNotCollide(t *testing.T) {
	m := NewRequestMap()
	a := &InFlightRequest{Span: noopSpan().Span, ToolName: "slow_tool", SessionID: "sess-a", StartTime: time.Now()}
	b := &InFlightRequest{Span: noopSpan().Span, ToolName: "fast_tool", SessionID: "sess-b", StartTime: time.Now()}

	m.Store("sess-a", "1", a)
	m.Store("sess-b", "1", b)

	if got := m.Take("sess-b", "1"); got != b {
		t.Fatalf("session b took %v, want its own request", got)
	}
	got := m.Take("sess-a", "1")
	if got != a {
		t.Fatalf("session a's request was lost when session b stored the same id (got %v)", got)
	}
	if got.ToolName != "slow_tool" {
		t.Errorf("tool name misattributed across sessions: %q", got.ToolName)
	}
}

// TestRequestMap_TakeSession drains one session without touching another,
// which is what lets a dropped stream close out only its own calls.
func TestRequestMap_TakeSession(t *testing.T) {
	m := NewRequestMap()
	m.Store("sess-a", "1", noopSpan())
	m.Store("sess-a", "2", noopSpan())
	m.Store("sess-b", "1", noopSpan())

	if got := m.TakeSession("sess-a"); len(got) != 2 {
		t.Fatalf("TakeSession returned %d requests, want 2", len(got))
	}
	if m.Take("sess-b", "1") == nil {
		t.Error("TakeSession drained an unrelated session")
	}
	if got := m.TakeSession(""); got != nil {
		t.Error("TakeSession(\"\") must not drain the id-only fallback bucket")
	}
}
