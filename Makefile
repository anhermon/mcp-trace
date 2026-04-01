BINARY  := mcp-trace
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.Version=$(VERSION)"

.PHONY: build test lint release clean

build:
	go build $(LDFLAGS) -o bin/$(BINARY) ./cmd/mcp-trace

test:
	go test ./...

lint:
	golangci-lint run ./...

# Cross-compile for all supported platforms.
release:
	GOOS=linux   GOARCH=amd64  go build $(LDFLAGS) -o dist/$(BINARY)_linux_amd64   ./cmd/mcp-trace
	GOOS=linux   GOARCH=arm64  go build $(LDFLAGS) -o dist/$(BINARY)_linux_arm64   ./cmd/mcp-trace
	GOOS=darwin  GOARCH=amd64  go build $(LDFLAGS) -o dist/$(BINARY)_darwin_amd64  ./cmd/mcp-trace
	GOOS=darwin  GOARCH=arm64  go build $(LDFLAGS) -o dist/$(BINARY)_darwin_arm64  ./cmd/mcp-trace
	GOOS=windows GOARCH=amd64  go build $(LDFLAGS) -o dist/$(BINARY)_windows_amd64.exe ./cmd/mcp-trace

clean:
	rm -rf bin/ dist/
