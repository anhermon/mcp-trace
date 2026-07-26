package telemetry

import (
	"context"
	"net"
	"testing"
	"time"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

// stubCollector is a plaintext OTLP/gRPC trace collector that reports what it received.
type stubCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	got chan *coltracepb.ExportTraceServiceRequest
}

func (c *stubCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	select {
	case c.got <- req:
	default:
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// TestNew_GRPCExportReachesPlaintextCollector is the regression test for the
// insecure-gRPC bug: the exporter used to send a TLS ClientHello to a plaintext
// collector, so no span ever arrived and the failure was silent.
func TestNew_GRPCExportReachesPlaintextCollector(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubCollector{got: make(chan *coltracepb.ExportTraceServiceRequest, 1)}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, stub)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	ctx := context.Background()
	p, err := New(ctx, Config{
		GRPCEndpoint: lis.Addr().String(),
		Insecure:     true,
		ServiceName:  "otel-test",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, span := p.Tracer.Start(ctx, "mcp tools/call probe")
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown (span never flushed to collector): %v", err)
	}

	select {
	case req := <-stub.got:
		spans := req.GetResourceSpans()
		if len(spans) == 0 || len(spans[0].GetScopeSpans()) == 0 || len(spans[0].GetScopeSpans()[0].GetSpans()) == 0 {
			t.Fatalf("collector received an export with no spans: %v", req)
		}
		if got := spans[0].GetScopeSpans()[0].GetSpans()[0].GetName(); got != "mcp tools/call probe" {
			t.Errorf("span name = %q, want %q", got, "mcp tools/call probe")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no span reached the plaintext collector")
	}
}
