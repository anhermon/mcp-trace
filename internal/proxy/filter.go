package proxy

import "strings"

// Filter decides which JSON-RPC methods should be traced.
type Filter struct {
	// TraceAll enables tracing for all methods, not just tools/call.
	TraceAll bool
	// IncludeLifecycle enables tracing for initialize/ping/notifications/*.
	IncludeLifecycle bool
}

// ShouldTrace returns true if the given JSON-RPC method should produce a span.
func (f *Filter) ShouldTrace(method string) bool {
	// Lifecycle methods are suppressed unless explicitly enabled.
	if isLifecycle(method) && !f.IncludeLifecycle {
		return false
	}
	// Default: only tools/call.
	if !f.TraceAll && method != "tools/call" {
		return false
	}
	return true
}

// isLifecycle returns true for MCP protocol lifecycle methods that are
// suppressed by default because they add noise without operational value.
func isLifecycle(method string) bool {
	switch method {
	case "initialize", "initialized", "ping":
		return true
	}
	return strings.HasPrefix(method, "notifications/")
}
