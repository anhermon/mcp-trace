package proxy

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// HostGuard protects a local listener against DNS rebinding: a web page on
// evil.example.com that re-resolves its own name to 127.0.0.1 can make the
// browser send requests to mcp-trace, which would relay them to the MCP server.
// The browser still sends Host: evil.example.com (and an Origin for that
// site), so requests are only served when the Host header, and the Origin
// header if present, name localhost, a loopback address, or a host on the
// operator's allowlist. Everything else gets 403 before it reaches upstream.
//
// mcp-trace rewrites Host to the --target when it forwards, so the upstream
// server's own DNS-rebinding check can never see the original value: the
// check has to happen here.
type HostGuard struct {
	next     http.Handler
	allowAll bool
	allowed  map[string]bool
	logger   *slog.Logger
}

// AllowAnyHost is the allowlist entry that disables the check.
const AllowAnyHost = "*"

// NewHostGuard wraps next. extraHosts are hostnames (or IP literals) allowed
// in addition to localhost and loopback addresses; ports are ignored, and
// AllowAnyHost turns the check off.
func NewHostGuard(next http.Handler, extraHosts []string, logger *slog.Logger) *HostGuard {
	g := &HostGuard{next: next, allowed: map[string]bool{}, logger: logger}
	for _, h := range extraHosts {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if h == AllowAnyHost {
			g.allowAll = true
			continue
		}
		g.allowed[hostnameOf(h)] = true
	}
	return g
}

func (g *HostGuard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.allowAll {
		if !g.hostAllowed(hostnameOf(r.Host)) {
			g.reject(w, r, "Invalid Host header: "+r.Host)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host == "" || !g.hostAllowed(normalizeHost(u.Hostname())) {
				g.reject(w, r, "Invalid Origin header: "+origin)
				return
			}
		}
	}
	g.next.ServeHTTP(w, r)
}

func (g *HostGuard) hostAllowed(host string) bool {
	if host == "" {
		return false
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return g.allowed[host]
}

// reject answers 403 with a JSON-RPC error body, the same shape the MCP SDKs
// use for this check, so clients surface a readable message.
func (g *HostGuard) reject(w http.ResponseWriter, r *http.Request, msg string) {
	if g.logger != nil {
		g.logger.Warn("rejected request: possible DNS rebinding (allow the host with --allowed-hosts)",
			"host", r.Host, "origin", r.Header.Get("Origin"), "remote", r.RemoteAddr)
	}
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      nil,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write(body)
}

// hostnameOf returns the lower-cased hostname of a Host header value, without
// port or IPv6 brackets.
func hostnameOf(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return normalizeHost(h)
	}
	return normalizeHost(hostport)
}

func normalizeHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	h = strings.TrimSuffix(h, ".")
	return strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")
}
