package proxy

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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
	name, _ := toolCall(raw)
	return name
}

// ToolCallArgKeys returns the tool's argument names, sorted and comma-joined.
// Names alone are safe to record by default: they tell you which call shape
// failed without putting file paths, queries, or credentials in a trace backend.
func ToolCallArgKeys(raw json.RawMessage) string {
	_, args := toolCall(raw)
	if len(args) == 0 {
		return ""
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// ToolCallArgsJSON returns the raw tool arguments as JSON, truncated to
// maxLen bytes. Only for use when the operator has opted in: arguments are
// user data and routinely carry secrets.
func ToolCallArgsJSON(raw json.RawMessage, maxLen int) string {
	var p struct {
		Arguments json.RawMessage `json:"arguments"`
	}
	if raw == nil || json.Unmarshal(raw, &p) != nil || len(p.Arguments) == 0 {
		return ""
	}
	s := string(p.Arguments)
	if len(s) > maxLen {
		return s[:maxLen] + "…(truncated)"
	}
	return s
}

func toolCall(raw json.RawMessage) (string, map[string]json.RawMessage) {
	if raw == nil {
		return "", nil
	}
	var p struct {
		Name      string                     `json:"name"`
		Arguments map[string]json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", nil
	}
	return p.Name, p.Arguments
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

// IsError reports whether the response represents an error condition, along
// with the error message and the JSON-RPC error code (0 when there is none).
// Covers both JSON-RPC protocol errors and MCP tool-level errors (result.isError).
func IsError(resp *RPCResponse) (bool, string, int) {
	if resp.Error != nil {
		return true, resp.Error.Message, resp.Error.Code
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
			return true, msg, 0
		}
	}
	return false, "", 0
}

// ClientInfo is the client identity announced in an initialize request.
type ClientInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// ParseClientInfo extracts clientInfo from an initialize params payload.
// This is the only place an MCP client names itself, so it is the only way a
// span can answer "which client made this call".
func ParseClientInfo(raw json.RawMessage) ClientInfo {
	var p struct {
		ClientInfo ClientInfo `json:"clientInfo"`
	}
	if raw == nil {
		return ClientInfo{}
	}
	_ = json.Unmarshal(raw, &p)
	return p.ClientInfo
}
