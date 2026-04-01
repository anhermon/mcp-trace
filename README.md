# mcp-trace

Transparent Go proxy for MCP servers that emits [OpenTelemetry](https://opentelemetry.io/) spans for every JSON-RPC tool call.

```
MCP Client  →  mcp-trace :8001  →  MCP Server :8000
                     ↓
              OTEL Collector :4317
              (Jaeger / Tempo / Honeycomb)
```

## Quick start

```bash
# Run against a local MCP server, export spans to Jaeger
mcp-trace --target http://localhost:8000/sse --port 8001 --otel-endpoint localhost:4317
```

Point your MCP client at `:8001` instead of `:8000`. Zero client changes required.

### With Docker Compose (Jaeger all-in-one)

```bash
docker compose -f docker/compose.jaeger.yml up -d
mcp-trace --target http://localhost:8000/sse
# Open http://localhost:16686 — traces appear after the first tool call.
```

## Installation

### Pre-built binary

Download from [GitHub Releases](https://github.com/anhermon/mcp-trace/releases).

### Homebrew (coming soon)

```bash
brew install paperclipai/tap/mcp-trace
```

### Docker

```bash
docker run --rm ghcr.io/paperclipai/mcp-trace \
  --target http://host.docker.internal:8000/sse \
  --otel-endpoint host.docker.internal:4317
```

### Build from source

```bash
go install github.com/paperclipai/mcp-trace/cmd/mcp-trace@latest
```

## Configuration

All flags can be set via a `.mcp-trace.yaml` file (see `.mcp-trace.yaml.example`).

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | *(required)* | Upstream MCP server SSE URL |
| `--port` | `8001` | Local port to listen on |
| `--otel-endpoint` | `localhost:4317` | OTLP gRPC endpoint |
| `--otel-http` | `false` | Use HTTP OTLP exporter instead of gRPC |
| `--otel-http-endpoint` | `http://localhost:4318` | OTLP HTTP endpoint |
| `--otel-insecure` | `true` | Disable TLS for OTLP |
| `--service-name` | `mcp-trace` | OTel `service.name` resource attribute |
| `--trace-all` | `false` | Trace all JSON-RPC methods (not just `tools/call`) |
| `--include-lifecycle` | `false` | Include `initialize`/`ping`/`notifications/*` |
| `--log-level` | `info` | `debug` \| `info` \| `warn` \| `error` |
| `--config` | | Path to config file |

## Span schema

Every traced call produces a span with these attributes:

| Attribute | Description |
|-----------|-------------|
| `mcp.tool.name` | Tool name (tools/call only) |
| `mcp.tool.duration_ms` | Wall-clock duration in ms |
| `mcp.tool.status` | `ok` or `error` |
| `mcp.server.target` | Upstream URL |
| `mcp.request.id` | JSON-RPC request id |
| `mcp.method` | JSON-RPC method |
| `error` | `true` if errored |
| `error.message` | Error message |

Span names follow the pattern:
- `mcp tools/call read_file` (tool calls)
- `mcp tools/list` (other methods, with `--trace-all`)

## As a Claude Code plugin

```json
{
  "mcpServers": {
    "my-server-traced": {
      "command": "mcp-trace",
      "args": [
        "--target", "http://localhost:8000/sse",
        "--port", "8001",
        "--otel-endpoint", "localhost:4317"
      ]
    }
  }
}
```

## Development

```bash
task build    # build binary to bin/mcp-trace
task test     # run all tests
task ci       # full pipeline: vet + test + lint + build
task release  # cross-compile all platforms to dist/
```

### Pre-commit hooks

Install hooks that run `task ci` before every commit and `task check` before every push:

```bash
task hooks:install
```

Remove hooks:

```bash
task hooks:uninstall
```

Hooks are stored in `scripts/hooks/` and symlinked into `.git/hooks/`. Bypass in emergencies with `--no-verify` (use sparingly).

## Roadmap

- **v1.0** — SSE proxy with OTLP spans (this release)
- **v2.0** — stdio transport support (`mcp-trace --stdio -- <command>`)
- **v2.x** — Metrics (counters, histograms), sampling

## License

MIT
