package main

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// agent-inspect v1.0 intent JSONL
// ---------------------------------------------------------------------------

// intentEvent is one line of an agent-inspect v1.0 JSONL export: the agent's
// own record of what it meant to do. parentId is the event-level edge; trace is
// the span-level edge, and it is the only thing the join in join.go looks at.
type intentEvent struct {
	SchemaVersion string         `json:"schemaVersion"`
	EventID       string         `json:"eventId"`
	RunID         string         `json:"runId"`
	ParentID      string         `json:"parentId,omitempty"`
	Kind          string         `json:"kind"`
	Name          string         `json:"name"`
	Status        string         `json:"status"`
	Timestamp     string         `json:"timestamp"`
	StartedAt     string         `json:"startedAt"`
	EndedAt       string         `json:"endedAt"`
	DurationMs    int64          `json:"durationMs"`
	Confidence    string         `json:"confidence"`
	Source        intentSource   `json:"source"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Trace         *intentTrace   `json:"trace,omitempty"`

	// start/end are in-memory only, not serialised. The sweep in report()
	// deliberately hands the heuristic these full-precision times rather than
	// the truncated ISO millis a real consumer would parse — that is the
	// heuristic's best case, and it is still ambiguous.
	start, end time.Time
}

type intentSource struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

type intentTrace struct {
	TraceID      string `json:"traceId"`
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
}

// isoMillis formats a timestamp the way agent-inspect fixtures do.
func isoMillis(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// argSummary is the payload summary agent-inspect emits instead of the raw
// value. The published shape carries only type and length; sha256 is added here
// because identical hashes across the three attempts are what proves the retry
// replayed byte-identical arguments rather than quietly mutating them.
func argSummary(args string) map[string]any {
	sum := sha256.Sum256([]byte(args))
	return map[string]any{
		"type":   "string",
		"length": len(args),
		"sha256": hex.EncodeToString(sum[:]),
	}
}

func writeJSONL(path string, events []intentEvent) error {
	f, err := os.Create(path) //nolint:gosec // demo output path
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for i := range events {
		if err := enc.Encode(events[i]); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// tiny in-process OTLP/HTTP receiver
// ---------------------------------------------------------------------------

// wireSpan is one span as it arrived over OTLP, flattened to what the demo
// needs. Attribute values are stringified on the way in so the JSON dump has no
// number-typing surprises when it is read back.
type wireSpan struct {
	Name         string            `json:"name"`
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId"`
	StartNanos   uint64            `json:"startTimeUnixNano"`
	EndNanos     uint64            `json:"endTimeUnixNano"`
	Attributes   map[string]string `json:"attributes"`
}

// Unix nanos as uint64 is the OTLP wire type; the cast is safe until 2262.
func (s wireSpan) start() time.Time { return time.Unix(0, int64(s.StartNanos)) } //nolint:gosec // see above
func (s wireSpan) end() time.Time   { return time.Unix(0, int64(s.EndNanos)) }   //nolint:gosec // see above

// spanSink accepts OTLP/HTTP exports and dumps them to a JSON file. It stands
// in for a collector so the demo runs with `go run` and no Docker.
type spanSink struct {
	path string

	mu    sync.Mutex
	spans []wireSpan
}

func (s *spanSink) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", s.export)
	// Anything else means the exporter and this receiver disagree on the path,
	// which would otherwise look like "the proxy emitted no spans".
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(os.Stderr, "otlp receiver: unexpected request %s %s\n", r.Method, r.URL.Path)
		http.NotFound(w, r)
	})
	return mux
}

func (s *spanSink) export(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var req coltracepb.ExportTraceServiceRequest
	if err := proto.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				s.spans = append(s.spans, wireSpan{
					Name:         sp.GetName(),
					TraceID:      hex.EncodeToString(sp.GetTraceId()),
					SpanID:       hex.EncodeToString(sp.GetSpanId()),
					ParentSpanID: hex.EncodeToString(sp.GetParentSpanId()),
					StartNanos:   sp.GetStartTimeUnixNano(),
					EndNanos:     sp.GetEndTimeUnixNano(),
					Attributes:   flattenAttrs(sp.GetAttributes()),
				})
			}
		}
	}
	dump, err := json.MarshalIndent(s.spans, "", "  ")
	s.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Write the file BEFORE answering. provider.Shutdown() returns once the
	// exporter's POST returns, so this ordering is what makes Shutdown a real
	// barrier for "spans.json is complete".
	if err := os.WriteFile(s.path, dump, 0o600); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	}
	return io.ReadAll(r.Body)
}

func flattenAttrs(kvs []*commonpb.KeyValue) map[string]string {
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			out[kv.GetKey()] = v.StringValue
		case *commonpb.AnyValue_BoolValue:
			out[kv.GetKey()] = fmt.Sprintf("%t", v.BoolValue)
		case *commonpb.AnyValue_IntValue:
			out[kv.GetKey()] = fmt.Sprintf("%d", v.IntValue)
		case *commonpb.AnyValue_DoubleValue:
			out[kv.GetKey()] = fmt.Sprintf("%.2f", v.DoubleValue)
		default:
			out[kv.GetKey()] = kv.GetValue().String()
		}
	}
	return out
}

func readSpans(path string) ([]wireSpan, error) {
	data, err := os.ReadFile(path) //nolint:gosec // demo output path
	if err != nil {
		return nil, err
	}
	var spans []wireSpan
	return spans, json.Unmarshal(data, &spans)
}
