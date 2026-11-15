package proxy

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Regression test for issue #24 (MCP conformance dns-rebinding-protection):
// the proxy answered 200 to requests with Host/Origin: evil.example.com and
// relayed them upstream with Host rewritten to --target, so the upstream's own
// check could not catch them either.
func TestHostGuard(t *testing.T) {
	cases := []struct {
		name    string
		allowed []string
		host    string
		origin  string
		want    int
	}{
		{"localhost", nil, "localhost:8001", "", 200},
		{"localhost no port", nil, "localhost", "", 200},
		{"127.0.0.1", nil, "127.0.0.1:8001", "", 200},
		{"other loopback", nil, "127.0.0.2:8001", "", 200},
		{"ipv6 loopback", nil, "[::1]:8001", "", 200},
		{"sub.localhost", nil, "app.localhost:8001", "", 200},
		{"local origin", nil, "localhost:8001", "http://localhost:3000", 200},
		{"local origin ip", nil, "127.0.0.1:8001", "http://127.0.0.1:8001", 200},
		{"rebinding host and origin", nil, "evil.example.com", "http://evil.example.com", 403},
		{"rebinding host only", nil, "evil.example.com:8001", "", 403},
		{"cross-site origin on localhost", nil, "localhost:8001", "https://evil.example.com", 403},
		{"null origin", nil, "localhost:8001", "null", 403},
		{"lan ip not allowed by default", nil, "192.168.1.5:8001", "", 403},
		{"empty host", nil, "", "", 403},
		{"localhost lookalike", nil, "localhost.evil.com", "", 403},
		{"allowlisted host", []string{"mcp-trace"}, "mcp-trace:8001", "", 200},
		{"allowlist ignores port and case", []string{"MCP-Trace:9999"}, "mcp-trace:8001", "http://mcp-trace:8001", 200},
		{"allowlisted ip", []string{"192.168.1.5"}, "192.168.1.5:8001", "", 200},
		{"allowlist does not open others", []string{"mcp-trace"}, "evil.example.com", "", 403},
		{"wildcard disables check", []string{"*"}, "evil.example.com", "http://evil.example.com", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reached := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				w.WriteHeader(http.StatusOK)
			})
			g := NewHostGuard(next, tc.allowed, slog.New(slog.NewTextHandler(io.Discard, nil)))
			req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`))
			req.Host = tc.host
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
			if tc.want == 403 {
				if reached {
					t.Error("rejected request still reached the upstream handler")
				}
				if !strings.Contains(rec.Body.String(), `"code":-32000`) {
					t.Errorf("403 body is not a JSON-RPC error: %s", rec.Body.String())
				}
			}
		})
	}
}

// End to end over a real listener, the way the conformance suite probes it.
func TestHostGuard_StreamableProxyRejectsRebinding(t *testing.T) {
	h := newStreamableTestHarness(t, &Filter{})
	guarded := httptest.NewServer(NewHostGuard(h.proxySrv.Config.Handler, nil, nil))
	defer guarded.Close()

	req, _ := http.NewRequest(http.MethodPost, guarded.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	req.Host = "evil.example.com"
	req.Header.Set("Origin", "http://evil.example.com")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("rebinding request status = %d, want 403", resp.StatusCode)
	}
	if h.up.lastHeader != nil {
		t.Error("rebinding request was forwarded upstream")
	}

	// The same request with a local Host goes through.
	ok, err := http.Post(guarded.URL, "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	ok.Body.Close()
	if ok.StatusCode != http.StatusOK {
		t.Errorf("local request status = %d, want 200", ok.StatusCode)
	}
}
