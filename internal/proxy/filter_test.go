package proxy

import "testing"

func TestFilter_DefaultTracesOnlyToolsCall(t *testing.T) {
	f := &Filter{}
	cases := map[string]bool{
		"tools/call":          true,
		"tools/list":          false,
		"resources/read":      false,
		"initialize":          false,
		"ping":                false,
		"notifications/hello": false,
	}
	for method, want := range cases {
		got := f.ShouldTrace(method)
		if got != want {
			t.Errorf("Filter{}.ShouldTrace(%q) = %v, want %v", method, got, want)
		}
	}
}

func TestFilter_TraceAll(t *testing.T) {
	f := &Filter{TraceAll: true}
	// Non-lifecycle methods should be traced.
	if !f.ShouldTrace("tools/list") {
		t.Error("expected tools/list to be traced with TraceAll")
	}
	if !f.ShouldTrace("resources/read") {
		t.Error("expected resources/read to be traced with TraceAll")
	}
	// Lifecycle methods still suppressed.
	if f.ShouldTrace("initialize") {
		t.Error("initialize should be suppressed even with TraceAll")
	}
	if f.ShouldTrace("ping") {
		t.Error("ping should be suppressed even with TraceAll")
	}
	if f.ShouldTrace("notifications/progress") {
		t.Error("notifications/progress should be suppressed even with TraceAll")
	}
}

func TestFilter_IncludeLifecycle(t *testing.T) {
	f := &Filter{TraceAll: true, IncludeLifecycle: true}
	if !f.ShouldTrace("initialize") {
		t.Error("initialize should be traced with IncludeLifecycle")
	}
	if !f.ShouldTrace("ping") {
		t.Error("ping should be traced with IncludeLifecycle")
	}
	if !f.ShouldTrace("notifications/progress") {
		t.Error("notifications/progress should be traced with IncludeLifecycle")
	}
}

func TestFilter_IncludeLifecycleSansTraceAll(t *testing.T) {
	// IncludeLifecycle alone doesn't enable non-tools/call tracing.
	f := &Filter{IncludeLifecycle: true}
	if !f.ShouldTrace("tools/call") {
		t.Error("tools/call should always be traced")
	}
	if f.ShouldTrace("tools/list") {
		t.Error("tools/list should not be traced without TraceAll")
	}
	// But lifecycle methods are now included.
	if !f.ShouldTrace("initialize") {
		t.Error("initialize should be traced with IncludeLifecycle")
	}
}
