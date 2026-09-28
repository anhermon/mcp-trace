#!/usr/bin/env python3
"""
Minimal MCP server for testing stdio transport.
"""
import sys
import json

def main():
    for line in sys.stdin:
        try:
            req = json.loads(line.strip())
            req_id = req.get("id")
            method = req.get("method", "")
            
            # Notifications have no id - don't send a response
            if req_id is None:
                # Just log to stderr and continue
                print(f"Received notification: {method}", file=sys.stderr, flush=True)
                continue
            
            # Regular request - send response
            resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"status": "ok", "method": method}
            }
            print(json.dumps(resp), flush=True)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr, flush=True)

if __name__ == "__main__":
    main()
