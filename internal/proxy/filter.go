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
	// Lifecycle methods are controlled solely by IncludeLifecycle.
	if isLifecycle(method) {
		return f.IncludeLifecycle
	}
	// Default: only tools/call; TraceAll opens up everything else.
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

// ShouldTraceServerRequest reports whether a request the server sends to the
// client (sampling/createMessage, elicitation/create, roots/list, ...) should
// produce a span. These round-trips happen inside a traced call and often
// dominate its duration (a human answering an elicitation, a model sampling),
// so they are traced by default. Server pings follow IncludeLifecycle.
func (f *Filter) ShouldTraceServerRequest(method string) bool {
	if isLifecycle(method) {
		return f.IncludeLifecycle
	}
	return true
}
