# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-X main.Version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /mcp-trace ./cmd/mcp-trace

# Runtime stage — scratch for minimal image size
FROM scratch
COPY --from=builder /mcp-trace /mcp-trace
ENTRYPOINT ["/mcp-trace"]
