package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// A server that asks the client for sampling mid tools/call keeps the POST's
// event stream open until the client answers on a separate POST. The proxy
// must deliver that server request immediately; buffering it deadlocks the
// call (found by the MCP conformance suite: tools-call-sampling and the
// elicitation scenarios timed out only through mcp-trace).
func TestStreamable_ServerRequestIsFlushedBeforeStreamEnds(t *testing.T) {
	for _, traced := range []bool{true, false} {
		t.Run(fmt.Sprintf("traced=%v", traced), func(t *testing.T) {
			release := make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"sampling/createMessage\",\"params\":{}}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				fmt.Fprint(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[]}}\n\n")
			}))
			defer up.Close()

			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
			defer tp.Shutdown(context.Background())
			method := "tools/call"
			if !traced {
				method = "resources/read" // not traced by default: plain forwarding path
			}
			p, err := NewStreamableProxy(context.Background(), up.URL, &Filter{}, tp.Tracer("t"), slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatal(err)
			}
			ps := httptest.NewServer(p)
			defer ps.Close()

			// The POST runs in a goroutine: without flushing, even the response
			// headers stay buffered and http.Post itself never returns.
			got := make(chan struct{}, 1)
			go func() {
				resp, err := http.Post(ps.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":{"name":"test_sampling"}}`))
				if err != nil {
					return
				}
				defer resp.Body.Close()
				sc := bufio.NewScanner(resp.Body)
				for sc.Scan() {
					if strings.Contains(sc.Text(), "sampling/createMessage") {
						got <- struct{}{}
						return
					}
				}
			}()
			select {
			case <-got:
				close(release)
			case <-time.After(2 * time.Second):
				close(release)
				t.Fatal("server-to-client request was buffered by the proxy; the client never saw it while the stream was open")
			}
		})
	}
}
