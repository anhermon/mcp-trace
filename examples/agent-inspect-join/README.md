# agent-inspect join demo

Joining MCP **wire** spans (what mcp-trace's proxy sees on the JSON-RPC POST) to
an agent's **intent** records (agent-inspect v1.0 JSONL) **by span id alone** —
no tool name, no timestamps, no ordering.

The scenario is a three-attempt retry of the same tool with byte-identical
arguments, because that is the case where name-and-time correlation has the
least to work with.

## Run it

```
go run ./examples/agent-inspect-join                # W3C traceparent propagated
go run ./examples/agent-inspect-join -no-propagate  # negative control
go test ./examples/agent-inspect-join               # self-check
```

One process, ephemeral loopback ports, no Docker and no collector. It starts a
fake MCP HTTP+SSE server (fails `fetch-inventory` twice with a JSON-RPC error,
succeeds on the third attempt), the real `internal/proxy`, an OTLP/HTTP receiver
that dumps spans to `spans.json`, and a client that retries with 100ms/200ms
backoff. Add `-out DIR` to keep `spans.json` and `intent.jsonl`.

The interesting part of the output (span ids and durations vary per run; the
`agent` column is truncated to whole milliseconds, the `proxy` column is not):

```
merged tree — wire spans attached by span id only
────────────────────────────────────────────────────────────────────────
RUN mcp-retry-run                evt_run        span=b26b065908ba26d2 parent=-                agent  451ms  ok
  TOOL fetch-inventory (logical)   evt_tool       span=00a03d590907e886 parent=b26b065908ba26d2 agent  451ms  ok
    attempt 1                        evt_attempt_1  span=a41d8315635007d4 parent=00a03d590907e886 agent   41ms  error
      └─ wire mcp tools/call fetch-inventory span=baa8a0adac0977e6 parent=a41d8315635007d4  proxy 41.30ms  mcp.status=error mcp.rpc.error_code=-32003
    attempt 2                        evt_attempt_2  span=dbad6b3b1dcd8d21 parent=00a03d590907e886 agent   46ms  error
      └─ wire mcp tools/call fetch-inventory span=c9e2a8267498bdde parent=dbad6b3b1dcd8d21  proxy 46.36ms  mcp.status=error mcp.rpc.error_code=-32003
    attempt 3                        evt_attempt_3  span=f2cc072b4784d732 parent=00a03d590907e886 agent   62ms  ok
      └─ wire mcp tools/call fetch-inventory span=90a192d88b3051af parent=f2cc072b4784d732  proxy 61.77ms  mcp.status=ok

attached 3/3 wire spans by span id (tool name and timestamps were not consulted: see joinKey in join.go)

SELF-CHECK: PASSED — all 3 wire spans attached to the attempt whose
            mcp.request.id matches, using span identity alone.
```

With `-no-propagate` the same run prints `attached 0/3` and the negative control
block instead.

## What to look at

1. **The merged tree.** Each wire span sits under the attempt that caused it,
   and the two duration columns (`agent Nms` vs `proxy N.NNms`) come from two
   independently instrumented systems that never compared clocks.
2. **`joinKey` in `join.go`.** It has exactly two fields, `spanID` and
   `parentSpanID`. `joinBySpanID` takes nothing else, so the claim "the join
   never consults tool name or timestamps" is enforced by the type, not by a
   comment.
3. **`SELF-CHECK`.** `mcp.request.id` is the attempt number the upstream server
   actually saw. The join never reads it; the check does, and fails the process
   if any span landed on the wrong attempt — or if nothing attached at all
   while propagation was on.
4. **`input.value.summary`** in `intent.jsonl`. Same `sha256` on all three
   attempts — the retries replayed identical bytes, so arguments cannot
   disambiguate them either. (The field is the point; the hash is trivially
   equal here because the demo hashes one string literal three times.)
5. **`-no-propagate`.** Zero attachments, and the three wire spans carry no
   parent and three distinct trace ids.

## What this proves

- With `traceparent` on the client's JSON-RPC POST, the proxy's tool-call span
  is a child of the client's attempt span in the client's trace. `mcp-trace`
  needed **no changes** for this: `internal/proxy/sse.go` already extracts the
  inbound context and `internal/telemetry/otel.go` already installs the W3C
  propagator.
- Span identity alone is sufficient to attach wire spans to agent intent nodes
  across a retry, with no tolerance parameter and no clock assumption.
- Without propagation the same join yields nothing, and the wire data contains
  no evidence that the three calls were even one logical tool call.

## What this does not prove

- **It does not prove name+time correlation always fails.** With this timing
  (40/45/60ms of work, 100ms/200ms backoff) the attempt windows are disjoint, so
  a containment heuristic can pick the right attempt. The honest finding is
  weaker and printed as a sweep: the heuristic's answer is a function of a skew
  tolerance nobody can derive (at 150ms two attempts become indistinguishable, at
  250ms all three), and even at zero skew it is wrong in both directions — a wire
  span normally matches two nodes, its attempt and the logical `TOOL` node
  containing it, so it needs a "narrowest containing node" tiebreak; while the
  last attempt often matches *nothing*, because the proxy closes its span
  microseconds after the agent stamped that attempt's end. Both are visible in
  the sweep, and neither knob nor tiebreak exists in the span-id join. The sweep
  also flatters the heuristic on purpose: it is fed full-precision in-memory
  timestamps, not the ISO-millis a consumer would actually parse out of the
  JSONL.
- Single process, single host, loopback, one session, no concurrency. Real clock
  skew, batching delay, sampling and span loss are not modelled.
- The intent JSONL is emitted by this demo's client, not by a real agent-inspect
  instrumentation. It matches the published v1.0 shape (plus `sha256` in the
  payload summary, which the published fixtures do not carry yet); it is not a
  conformance test of that schema.
- The proxy is exercised over HTTP+SSE only. stdio and streamable HTTP
  transports are untouched here.
