package main

import (
	"sort"
	"time"
)

// ---------------------------------------------------------------------------
// the join under test
// ---------------------------------------------------------------------------

// joinKey is everything joinBySpanID is allowed to know about a wire span.
// The type exists to make the claim structural rather than a comment: there is
// no tool name and no timestamp in here, so the join cannot consult them even
// by accident.
type joinKey struct {
	spanID       string
	parentSpanID string
}

// joinBySpanID attaches wire spans to intent nodes by span identity alone.
//
// intentBySpanID maps an intent node's own trace.spanId to its eventId. A wire
// span emitted by the proxy is a *child* of the client span whose traceparent
// the client injected, so the edge is wireSpan.parentSpanId -> node.trace.spanId.
//
// The result maps wire span id -> intent event id. Spans with no match are
// simply absent; that is the whole negative control.
func joinBySpanID(intentBySpanID map[string]string, wire []joinKey) map[string]string {
	out := make(map[string]string, len(wire))
	for _, k := range wire {
		if ev, ok := intentBySpanID[k.parentSpanID]; ok {
			out[k.spanID] = ev
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// the heuristic it replaces
// ---------------------------------------------------------------------------

// nameTimeWindow is what a correlator without propagation has to work with:
// a tool name and a wall-clock window per intent node.
type nameTimeWindow struct {
	eventID    string
	toolName   string
	start, end time.Time
}

// nameTimeCandidates returns every intent node that could plausibly be the
// origin of a wire span, matching on tool name and containment of the span's
// window, widened by skew in both directions.
//
// skew is not a detail: two independently instrumented systems do not share a
// clock, and the correlator has to guess how far apart they can be. The size of
// the returned set as skew grows is the point — see the sweep in report().
func nameTimeCandidates(nodes []nameTimeWindow, toolName string, start, end time.Time, skew time.Duration) []string {
	var out []string
	for _, n := range nodes {
		if n.toolName != toolName {
			continue
		}
		if !start.Before(n.start.Add(-skew)) && !end.After(n.end.Add(skew)) {
			out = append(out, n.eventID)
		}
	}
	sort.Strings(out)
	return out
}
