# Build stage
FROM golang:1.22-alpine AS builder

# ca-certificates so the scratch image can make TLS OTLP connections.
RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.Version=${VERSION}" -o /mcp-trace ./cmd/mcp-trace

# Runtime stage — scratch for minimal image size
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /mcp-trace /mcp-trace
ENTRYPOINT ["/mcp-trace"]
