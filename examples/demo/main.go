// Command demo is the traffic generator for the mcp-trace demo bundle
// (docker-compose.yml). It is not part of the mcp-trace product.
//
//	demo -mode server -addr :8000        a minimal MCP HTTP+SSE server
//	demo -mode client -proxy http://...  fires tools/call requests at the proxy
//
// The server speaks just enough of the MCP HTTP+SSE transport for the proxy to
// trace it: GET /sse advertises a per-session POST endpoint via event:endpoint,
// and each POST's JSON-RPC response is pushed back on that session's SSE stream
// (which is what lets mcp-trace close the span with a real duration).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func main() {
	mode := flag.String("mode", "server", "server | client")
	addr := flag.String("addr", ":8000", "server listen address")
	proxy := flag.String("proxy", "http://localhost:8001", "client: proxy base URL")
	every := flag.Duration("every", 3*time.Second, "client: delay between tool calls")
	flag.Parse()

	var err error
	switch *mode {
	case "server":
		err = serve(*addr)
	case "client":
		err = callLoop(*proxy, *every)
	default:
		err = fmt.Errorf("unknown -mode %q", *mode)
	}
	if err != nil {
		log.Fatalf("demo: %v", err)
	}
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

type server struct {
	mu       sync.Mutex
	sessions map[string]chan string
}

func serve(addr string) error {
	s := &server{sessions: make(map[string]chan string)}
	mux := http.NewServeMux()
	mux.HandleFunc("/sse", s.sse)
	mux.HandleFunc("/messages", s.messages)
	log.Printf("demo MCP server listening on %s (GET /sse, POST /messages)", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	return srv.ListenAndServe()
}

func (s *server) sse(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	sid := fmt.Sprintf("%08x", rand.Uint32()) //nolint:gosec // demo session id, not a secret
	ch := make(chan string, 16)
	s.mu.Lock()
	s.sessions[sid] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, sid)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "event: endpoint\ndata: /messages?session_id=%s\n\n", sid)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (s *server) messages(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session_id")
	s.mu.Lock()
	ch, ok := s.sessions[sid]
	s.mu.Unlock()
	if !ok {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}

	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)

	// Answer asynchronously on the SSE stream, like a real MCP server.
	go func() {
		time.Sleep(work(req.Params.Name))
		var result any
		if req.Params.Name == "flaky_tool" {
			result = map[string]any{"isError": true, "content": []map[string]string{{"text": "upstream rate limit exceeded"}}}
		} else {
			result = map[string]any{"content": []map[string]string{{"text": "result of " + req.Params.Name}}}
		}
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
		select {
		case ch <- string(body):
			log.Printf("answered %s %s", req.Method, req.Params.Name)
		case <-time.After(time.Second):
			log.Printf("dropped response for %s (slow consumer)", req.Params.Name)
		}
	}()
}

// work fakes per-tool latency so the traces are not all identical.
func work(tool string) time.Duration {
	base := map[string]int{"search_docs": 120, "fetch_url": 340, "summarize": 700}[tool]
	if base == 0 {
		base = 200
	}
	return time.Duration(base+rand.Intn(base/2)) * time.Millisecond //nolint:gosec // demo jitter
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

var tools = []string{"search_docs", "fetch_url", "summarize", "search_docs", "flaky_tool"}

func callLoop(proxy string, every time.Duration) error {
	base, err := url.Parse(proxy)
	if err != nil {
		return err
	}

	endpoint, body, err := subscribe(base)
	if err != nil {
		return err
	}
	log.Printf("connected to %s, POST endpoint %s", proxy, endpoint)
	defer body.Close()

	// Print responses as they stream back.
	go func() {
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			if line := sc.Text(); strings.HasPrefix(line, "data: {") {
				log.Printf("← %s", strings.TrimPrefix(line, "data: "))
			}
		}
	}()

	for i := 1; ; i++ {
		tool := tools[i%len(tools)]
		req := fmt.Sprintf(`{"jsonrpc":"2.0","id":"%d","method":"tools/call","params":{"name":%q,"arguments":{"query":"opentelemetry"}}}`, i, tool)
		log.Printf("→ tools/call %s", tool)
		resp, err := http.Post(endpoint, "application/json", strings.NewReader(req)) //nolint:gosec,noctx // demo
		if err != nil {
			return err
		}
		resp.Body.Close()
		time.Sleep(every)
	}
}

// subscribe opens the SSE stream on the proxy and returns the absolute POST
// endpoint advertised by the upstream server plus the still-open stream body.
func subscribe(base *url.URL) (string, io.ReadCloser, error) {
	sseURL := base.JoinPath("/sse").String()

	var resp *http.Response
	var err error
	for i := 0; i < 30; i++ { // the proxy and server may still be starting
		resp, err = http.Get(sseURL) //nolint:gosec,noctx // demo
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return "", nil, fmt.Errorf("connecting to %s: %w", sseURL, err)
	}

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: /") {
			continue
		}
		rel, err := url.Parse(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		if err != nil {
			return "", nil, err
		}
		return base.ResolveReference(rel).String(), resp.Body, nil
	}
	resp.Body.Close()
	return "", nil, fmt.Errorf("no endpoint event from %s", sseURL)
}
