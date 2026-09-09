package main

import (
	"reflect"
	"testing"
	"time"
)

// intent side of a three-attempt retry: one logical tool node, three attempts.
var testIntent = map[string]string{
	"1111111111111111": "evt_tool",
	"aaaaaaaaaaaaaaaa": "evt_attempt_1",
	"bbbbbbbbbbbbbbbb": "evt_attempt_2",
	"cccccccccccccccc": "evt_attempt_3",
}

// TestJoinBySpanIDOrderIndependent feeds the wire spans out of chronological
// order. Order independence is exactly the property the name+time heuristic
// lacks, so a join that quietly relied on arrival order would pass a
// chronological test and fail here.
func TestJoinBySpanIDOrderIndependent(t *testing.T) {
	shuffled := []joinKey{
		{spanID: "dd03", parentSpanID: "cccccccccccccccc"}, // attempt 3
		{spanID: "dd01", parentSpanID: "aaaaaaaaaaaaaaaa"}, // attempt 1
		{spanID: "dd02", parentSpanID: "bbbbbbbbbbbbbbbb"}, // attempt 2
	}
	want := map[string]string{
		"dd01": "evt_attempt_1",
		"dd02": "evt_attempt_2",
		"dd03": "evt_attempt_3",
	}
	if got := joinBySpanID(testIntent, shuffled); !reflect.DeepEqual(got, want) {
		t.Fatalf("join attached spans to the wrong attempts:\n got %v\nwant %v", got, want)
	}
}

// TestJoinBySpanIDWithoutPropagation is the negative control: with no
// traceparent the proxy's spans are roots, so nothing attaches.
func TestJoinBySpanIDWithoutPropagation(t *testing.T) {
	orphans := []joinKey{{spanID: "dd01"}, {spanID: "dd02"}, {spanID: "dd03"}}
	if got := joinBySpanID(testIntent, orphans); len(got) != 0 {
		t.Fatalf("orphan spans should not attach, got %v", got)
	}
}

// TestNameTimeCandidatesAreSkewDependent pins the claim the demo prints, on
// windows chosen to be the heuristic's best case: even when containment holds
// exactly, the logical TOOL node is a second candidate at zero skew, and the
// answer changes as the assumed skew grows. (The live run is worse still — see
// heuristicSweep: real span ends miss containment by microseconds.)
func TestNameTimeCandidatesAreSkewDependent(t *testing.T) {
	base := time.Unix(1700000000, 0)
	ms := func(n int) time.Time { return base.Add(time.Duration(n) * time.Millisecond) }
	nodes := []nameTimeWindow{
		{eventID: "evt_tool", toolName: toolName, start: ms(0), end: ms(450)},
		{eventID: "evt_attempt_1", toolName: toolName, start: ms(0), end: ms(42)},
		{eventID: "evt_attempt_2", toolName: toolName, start: ms(142), end: ms(189)},
		{eventID: "evt_attempt_3", toolName: toolName, start: ms(389), end: ms(452)},
	}
	// The wire span for attempt 2, as the proxy saw it.
	start, end := ms(143), ms(188)

	if got := nameTimeCandidates(nodes, toolName, start, end, 0); len(got) != 2 {
		t.Fatalf("at zero skew want the attempt plus the containing logical node, got %v", got)
	}
	if got := nameTimeCandidates(nodes, toolName, start, end, 150*time.Millisecond); len(got) < 3 {
		t.Fatalf("at 150ms skew the attempts should stop being distinguishable, got %v", got)
	}
}
