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
	m.Store("1", r)
	got := m.Take("1")
	if got != r {
		t.Error("Take did not return stored request")
	}
	// Second take should return nil.
	if m.Take("1") != nil {
		t.Error("expected nil on second Take")
	}
}

func TestRequestMap_MissingKey(t *testing.T) {
	m := NewRequestMap()
	if m.Take("nope") != nil {
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
			m.Store(key, noopSpan())
			m.Take(key)
		}(i)
	}
	wg.Wait()
}

func TestRequestMap_EvictStale(t *testing.T) {
	m := NewRequestMap()
	old := &InFlightRequest{
		Span:      noopSpan().Span,
		StartTime: time.Now().Add(-60 * time.Second),
	}
	m.Store("old", old)
	m.Store("fresh", noopSpan())

	stale := m.EvictStale(30 * time.Second)
	if len(stale) != 1 || stale[0].ID != "old" {
		t.Errorf("expected 1 stale entry with id 'old', got %v", stale)
	}
	// Fresh entry should still be there.
	if m.Take("fresh") == nil {
		t.Error("fresh entry should still be in map")
	}
}
