#!/bin/sh
# Records the terminal demo. From the repo root:
#   docker compose down
#   asciinema rec --overwrite --window-size 132x28 -c "sh scripts/record-demo.sh" docs/img/demo.cast
#   agg --font-size 15 docs/img/demo.cast docs/img/demo.gif
set -e

p() { printf '\033[32m$\033[0m %s\n' "$1"; sleep 1; }

p "docker compose up -d"
docker compose up -d

p "docker compose logs -f mcp-trace   # every tool call becomes a span"
docker compose logs -f --tail 0 mcp-trace &
logs_pid=$!
sleep 11
kill "$logs_pid" 2>/dev/null || true
sleep 1

p "curl localhost:16686/api/traces?service=mcp-trace   # it is in Jaeger"
curl -s 'http://localhost:16686/api/traces?service=mcp-trace&lookback=1m&limit=3' |
	python3 -c 'import sys,json
for t in json.load(sys.stdin)["data"]:
    for s in t["spans"]:
        a={x["key"]:x["value"] for x in s["tags"]}
        print(f'"'"'{s["operationName"]:<32} {s["duration"]/1000:7.1f}ms  {a.get("mcp.tool.status")}'"'"')'
sleep 4
