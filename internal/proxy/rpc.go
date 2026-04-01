package proxy

import (
	"encoding/json"
	"fmt"
)

// RPCRequest represents an incoming JSON-RPC 2.0 request.
type RPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"` // string, number, or null
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

// RPCResponse represents an outgoing JSON-RPC 2.0 response.
type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error,omitempty"`
}

// RPCError is the error field in a JSON-RPC response.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ToolCallParams extracts the tool name from a tools/call params payload.
// Returns empty string if not parseable.
func ToolCallParams(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var p struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return ""
	}
	return p.Name
}

// IDString converts a JSON-RPC id (which may be number, string, or null) to a string key.
func IDString(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	s := string(raw)
	// strip surrounding quotes if it's a JSON string
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var unquoted string
		if err := json.Unmarshal(raw, &unquoted); err == nil {
			return unquoted
		}
	}
	return s
}

// ParseRequest parses a JSON body as an RPCRequest.
func ParseRequest(data []byte) (*RPCRequest, error) {
	var req RPCRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("parsing JSON-RPC request: %w", err)
	}
	return &req, nil
}

// ParseResponse parses a JSON body as an RPCResponse.
func ParseResponse(data []byte) (*RPCResponse, error) {
	var resp RPCResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("parsing JSON-RPC response: %w", err)
	}
	return &resp, nil
}

// IsError returns true if the response represents an error condition.
// Covers both JSON-RPC protocol errors and MCP tool-level errors (result.isError).
func IsError(resp *RPCResponse) (bool, string) {
	if resp.Error != nil {
		return true, resp.Error.Message
	}
	if resp.Result != nil {
		var r struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		}
		if err := json.Unmarshal(resp.Result, &r); err == nil && r.IsError {
			msg := "tool error"
			if len(r.Content) > 0 {
				msg = r.Content[0].Text
			}
			return true, msg
		}
	}
	return false, ""
}
