// Package proxy implements stdio transport for MCP.
package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/anhermon/mcp-trace/v2/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// StdioProxy wraps a local MCP server subprocess and proxies JSON-RPC over stdio.
type StdioProxy struct {
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	stdout      io.ReadCloser
	stderr      io.ReadCloser
	filter      *Filter
	tracer      trace.Tracer
	reqMap      *RequestMap
	logger      *slog.Logger
	CaptureArgs bool
	clientName  string
	clientVer   string
	responseCh  chan *rpcResponse
	mu          sync.Mutex
	running     bool
	stopped     bool // tracks if Stop() was called
	ctx         context.Context
	cancel      context.CancelFunc
	waitDone    chan struct{} // closed when cmd.Wait() completes
	exitErr     chan error    // receives subprocess exit errors
}

type rpcResponse struct {
	data []byte
	err  error
}

// NewStdioProxy creates a proxy that wraps a local MCP server subprocess.
func NewStdioProxy(ctx context.Context, command string, args []string, filter *Filter, tracer trace.Tracer, logger *slog.Logger) (*StdioProxy, error) {
	cmd := exec.CommandContext(ctx, command, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("creating stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, fmt.Errorf("creating stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		stdin.Close()
		stdout.Close()
		return nil, fmt.Errorf("creating stderr pipe: %w", err)
	}

	proxyCtx, cancel := context.WithCancel(ctx)

	p := &StdioProxy{
		cmd:        cmd,
		stdin:      stdin,
		stdout:     stdout,
		stderr:     stderr,
		filter:     filter,
		tracer:     tracer,
		reqMap:     NewRequestMap(),
		logger:     logger,
		responseCh: make(chan *rpcResponse, 16),
		ctx:        proxyCtx,
		cancel:     cancel,
		waitDone:   make(chan struct{}),
		exitErr:    make(chan error, 1),
	}

	return p, nil
}

// Start begins the subprocess and response reader.
func (p *StdioProxy) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("proxy already running")
	}

	if err := p.cmd.Start(); err != nil {
		return fmt.Errorf("starting subprocess: %w", err)
	}

	p.running = true

	// Read stdout in a goroutine
	go p.readResponses()

	// Forward stderr to logger
	go p.forwardStderr()

	// Monitor subprocess
	go p.monitorProcess()

	p.logger.Info("stdio subprocess started", "pid", p.cmd.Process.Pid)
	return nil
}

// Stop terminates the subprocess.
func (p *StdioProxy) Stop() error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()

	p.cancel()

	// Close stdin to signal subprocess to exit
	p.stdin.Close()

	// Wait for subprocess to exit (with timeout)
	select {
	case <-p.waitDone:
		// Process exited cleanly
	case <-time.After(5 * time.Second):
		p.logger.Warn("subprocess did not exit cleanly, killing")
		if err := p.cmd.Process.Kill(); err != nil {
			p.logger.Error("failed to kill subprocess", "err", err)
		}
		<-p.waitDone // wait for the wait to complete after kill
	}

	return nil
}

func (p *StdioProxy) readResponses() {
	scanner := bufio.NewScanner(p.stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSELine)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Make a copy since scanner reuses the buffer
		data := make([]byte, len(line))
		copy(data, line)

		select {
		case p.responseCh <- &rpcResponse{data: data}:
		case <-p.ctx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			p.logger.Error("reading subprocess stdout", "err", err)
		}
		select {
		case p.responseCh <- &rpcResponse{err: err}:
		case <-p.ctx.Done():
		}
	}

	close(p.responseCh)
}

func (p *StdioProxy) forwardStderr() {
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.logger.Info("subprocess stderr", "line", scanner.Text())
	}
}

func (p *StdioProxy) monitorProcess() {
	err := p.cmd.Wait()
	close(p.waitDone)

	p.mu.Lock()
	wasStopped := p.stopped
	p.running = false
	p.mu.Unlock()

	if !wasStopped {
		if err != nil {
			p.logger.Error("subprocess exited unexpectedly", "err", err)
			select {
			case p.exitErr <- err:
			default:
			}
		} else {
			p.logger.Warn("subprocess exited")
			select {
			case p.exitErr <- fmt.Errorf("subprocess exited"):
			default:
			}
		}
	}
}

// ExitErr returns a channel that receives an error when the subprocess exits unexpectedly.
// This allows the caller to shut down the proxy when the subprocess dies.
func (p *StdioProxy) ExitErr() <-chan error {
	return p.exitErr
}

func (p *StdioProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST is supported for stdio transport", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "reading body failed", http.StatusBadRequest)
		return
	}

	// Continue the caller's trace if it sent one
	spanCtx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	var spanStarted bool
	var inflight *InFlightRequest
	var requestID string
	var isNotification bool

	rpcReq, err := ParseRequest(body)
	if err != nil {
		p.logger.Warn("unparseable JSON-RPC request, forwarding untraced",
			"bytes", len(body), "err", err)
	}

	if err == nil {
		requestID = IDString(rpcReq.ID)
		isNotification = requestID == ""

		// Remember client info from initialize
		if rpcReq.Method == "initialize" {
			info := ParseClientInfo(rpcReq.Params)
			p.clientName, p.clientVer = info.Name, info.Version
		}

		if p.filter.ShouldTrace(rpcReq.Method) && requestID != "" {
			attrs := telemetry.SpanAttrs{
				Method:     rpcReq.Method,
				RequestID:  requestID,
				Target:     "stdio",
				ClientName: p.clientName,
				ClientVer:  p.clientVer,
			}
			if rpcReq.Method == "tools/call" {
				attrs.ToolName = ToolCallParams(rpcReq.Params)
				attrs.ArgKeys = ToolCallArgKeys(rpcReq.Params)
				if p.CaptureArgs {
					attrs.ArgsJSON = ToolCallArgsJSON(rpcReq.Params, maxArgsLen)
				}
			}

			ctx, span := telemetry.StartSpan(spanCtx, p.tracer, attrs)
			_ = ctx // spanCtx not needed for stdio since we don't forward headers
			inflight = &InFlightRequest{
				Span:      span,
				Method:    rpcReq.Method,
				ToolName:  attrs.ToolName,
				RequestID: requestID,
				StartTime: time.Now(),
			}
			p.reqMap.Store("", requestID, inflight)
			spanStarted = true
			p.logger.Debug("span started", "id", requestID, "method", rpcReq.Method, "tool", attrs.ToolName)
		}
	}

	// Write request to subprocess stdin
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		if spanStarted {
			p.cleanupSpan(requestID, "subprocess not running")
		}
		http.Error(w, "subprocess not running", http.StatusServiceUnavailable)
		return
	}
	p.mu.Unlock()

	// Write the JSON-RPC request as a single line
	if _, err := p.stdin.Write(body); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			p.logger.Error("writing to subprocess stdin", "err", err)
		}
		if spanStarted {
			p.cleanupSpan(requestID, "failed to write to subprocess")
		}
		http.Error(w, "subprocess write failed", http.StatusBadGateway)
		return
	}
	if _, err := p.stdin.Write([]byte("\n")); err != nil {
		if !errors.Is(err, os.ErrClosed) {
			p.logger.Error("writing newline to subprocess stdin", "err", err)
		}
		if spanStarted {
			p.cleanupSpan(requestID, "failed to write to subprocess")
		}
		http.Error(w, "subprocess write failed", http.StatusBadGateway)
		return
	}

	// For notifications, return immediately without waiting for a response
	if isNotification {
		p.logger.Debug("forwarded notification", "method", rpcReq.Method)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Wait for response (with timeout)
	timeout := time.After(30 * time.Second)
	for {
		select {
		case <-r.Context().Done():
			if spanStarted {
				p.cleanupSpan(requestID, "client disconnected")
			}
			return
		case <-timeout:
			if spanStarted {
				p.cleanupSpan(requestID, "response timeout")
			}
			http.Error(w, "request timeout", http.StatusGatewayTimeout)
			return
		case respData, ok := <-p.responseCh:
			if !ok {
				if spanStarted {
					p.cleanupSpan(requestID, "subprocess stdout closed")
				}
				http.Error(w, "subprocess closed", http.StatusBadGateway)
				return
			}
			if respData.err != nil {
				if spanStarted {
					p.cleanupSpan(requestID, respData.err.Error())
				}
				http.Error(w, "subprocess error", http.StatusBadGateway)
				return
			}

			// Parse the response
			resp, err := ParseResponse(respData.data)
			if err != nil {
				// Not a JSON-RPC response we understand - might be a notification
				p.logger.Debug("non-JSON-RPC response from subprocess", "err", err)
				continue
			}

			if resp.ID == nil {
				// Notification, not a response
				p.logger.Debug("received notification from subprocess")
				continue
			}

			respID := IDString(resp.ID)
			if respID != requestID {
				// Response for a different request - this shouldn't happen in serial stdio
				p.logger.Warn("received response for different request", "got", respID, "want", requestID)
				continue
			}

			// Found our response
			if spanStarted {
				inflight := p.reqMap.Take("", requestID)
				if inflight != nil {
					durationMS := float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0
					isErr, errMsg, errCode := IsError(resp)
					status := telemetry.StatusOK
					if isErr {
						status = telemetry.StatusError
					}

					telemetry.EndSpan(inflight.Span, telemetry.EndAttrs{
						DurationMS:   durationMS,
						Status:       status,
						ErrMsg:       errMsg,
						ErrCode:      errCode,
						ToolName:     inflight.ToolName,
						ResponseSize: len(respData.data),
					})
					p.logger.Debug("span ended", "id", requestID, "method", inflight.Method, "duration_ms", durationMS, "error", isErr)
				}
			}

			// Write response to client
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(respData.data); err != nil {
				p.logger.Debug("failed to write response to client", "err", err)
			}
			return
		}
	}
}

func (p *StdioProxy) cleanupSpan(requestID, errMsg string) {
	inflight := p.reqMap.Take("", requestID)
	if inflight == nil {
		return
	}
	telemetry.EndSpan(inflight.Span, telemetry.EndAttrs{
		DurationMS: float64(time.Since(inflight.StartTime).Microseconds()) / 1000.0,
		Status:     telemetry.StatusError,
		ErrMsg:     errMsg,
		ToolName:   inflight.ToolName,
	})
}
