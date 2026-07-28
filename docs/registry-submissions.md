# Registry submission drafts

Prepared, **not submitted**. Each needs a published `v*` tag (and, for Smithery
and Homebrew, the release artifacts that tag produces) before it can go out.

## Blurbs

- **One line:** Transparent proxy that turns MCP tool calls into OpenTelemetry spans — no client changes.
- **Two lines:** mcp-trace sits between an MCP client and an HTTP+SSE MCP server and emits one OTel span per `tools/call`: tool name, wall-clock duration, ok/error, W3C trace context propagated upstream. Ships a `docker compose up` demo bundle with an OTel Collector and Jaeger.

## awesome-mcp-servers (PR to the list)

Section: *Tools / Observability* (add the section if it does not exist yet).

```markdown
- [mcp-trace](https://github.com/anhermon/mcp-trace) 🏎️ ☁️ - Transparent proxy that emits an OpenTelemetry span for every MCP tool call; works with any OTLP backend (Jaeger, Tempo, Honeycomb). No client changes.
```

Checklist before opening the PR: read that repo's CONTRIBUTING for the current
emoji legend and alphabetical ordering rule, one entry per PR, no self-promotion
in the PR body beyond the entry itself.

## Smithery

Smithery indexes MCP *servers*; mcp-trace is a proxy in front of one, so it fits
only if listed as infrastructure. Check the current server.json/`smithery.yaml`
schema at the time of submission — the fields below are a draft, not a verified
schema.

```yaml
name: mcp-trace
displayName: mcp-trace
description: Transparent MCP proxy that emits OpenTelemetry spans for every tool call.
homepage: https://github.com/anhermon/mcp-trace
license: MIT
transport: sse
tags: [observability, opentelemetry, tracing, proxy, jaeger]
```

## Homebrew

Not a tap-worthy candidate yet — Homebrew core requires notability
(`homebrew/core` acceptable-formulae rules) that this project does not meet.
The realistic path is a personal tap (`anhermon/homebrew-tap`), which needs the
release tarball SHA256 from a published tag:

```ruby
class McpTrace < Formula
  desc "Transparent MCP proxy that emits OpenTelemetry spans for every tool call"
  homepage "https://github.com/anhermon/mcp-trace"
  url "https://github.com/anhermon/mcp-trace/archive/refs/tags/vX.Y.Z.tar.gz"
  sha256 "<fill from the released tarball>"
  license "MIT"

  depends_on "go" => :build

  def install
    system "go", "build", *std_go_args(ldflags: "-X main.Version=#{version}"), "./cmd/mcp-trace"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/mcp-trace --version")
  end
end
```

Note: `--version` prints `dev` unless the binary is built with
`-ldflags -X main.Version=…`, which the formula above does.
