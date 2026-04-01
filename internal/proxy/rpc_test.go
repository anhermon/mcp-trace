package proxy

import (
	"encoding/json"
	"testing"
)

func TestIDString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`"abc"`, "abc"},
		{`42`, "42"},
		{`null`, "null"},
		{`"hello world"`, "hello world"},
	}
	for _, tt := range tests {
		got := IDString(json.RawMessage(tt.input))
		if got != tt.want {
			t.Errorf("IDString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestToolCallParams(t *testing.T) {
	raw := json.RawMessage(`{"name":"read_file","arguments":{"path":"/tmp/x"}}`)
	got := ToolCallParams(raw)
	if got != "read_file" {
		t.Errorf("got %q, want %q", got, "read_file")
	}
}

func TestToolCallParams_Empty(t *testing.T) {
	got := ToolCallParams(nil)
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestIsError_RPCError(t *testing.T) {
	resp := &RPCResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Error:   &RPCError{Code: -32600, Message: "Invalid Request"},
	}
	isErr, msg := IsError(resp)
	if !isErr {
		t.Error("expected error")
	}
	if msg != "Invalid Request" {
		t.Errorf("got msg %q", msg)
	}
}

func TestIsError_MCPToolError(t *testing.T) {
	resp := &RPCResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  json.RawMessage(`{"isError":true,"content":[{"type":"text","text":"file not found"}]}`),
	}
	isErr, msg := IsError(resp)
	if !isErr {
		t.Error("expected error")
	}
	if msg != "file not found" {
		t.Errorf("got msg %q", msg)
	}
}

func TestIsError_OK(t *testing.T) {
	resp := &RPCResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  json.RawMessage(`{"content":[{"type":"text","text":"hello"}]}`),
	}
	isErr, _ := IsError(resp)
	if isErr {
		t.Error("expected no error")
	}
}

func TestParseRequest(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"echo"}}`)
	req, err := ParseRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if req.Method != "tools/call" {
		t.Errorf("got method %q", req.Method)
	}
	if IDString(req.ID) != "1" {
		t.Errorf("got id %q", IDString(req.ID))
	}
}
