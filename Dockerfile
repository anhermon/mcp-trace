# Build stage. --platform=$BUILDPLATFORM keeps the compiler running natively
# under buildx multi-arch builds; Go cross-compiles instead of being emulated.
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder

# ca-certificates so the scratch image can make TLS OTLP connections.
RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags "-X main.Version=${VERSION}" -o /mcp-trace ./cmd/mcp-trace
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /demo ./examples/demo

# Demo traffic generator, used only by docker-compose.yml (build target: demo).
FROM scratch AS demo
COPY --from=builder /demo /demo
ENTRYPOINT ["/demo"]

# Runtime stage — scratch for minimal image size. Kept last on purpose: a plain
# `docker build .` (and the ghcr publish job) must produce mcp-trace, not demo.
FROM scratch AS runtime
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /mcp-trace /mcp-trace
ENTRYPOINT ["/mcp-trace"]
