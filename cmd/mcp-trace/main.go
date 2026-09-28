package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/anhermon/mcp-trace/v2/internal/config"
	"github.com/anhermon/mcp-trace/v2/internal/proxy"
	"github.com/anhermon/mcp-trace/v2/internal/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is set at build time via -ldflags. Binaries built by `go install`
// get no ldflags, so version() falls back to the module metadata the toolchain
// stamps into every binary.
var Version = "dev"

// version reports the build version, preferring the ldflags value and falling
// back to Go module build info.
//
// `go install module/cmd/x@v1.0.1` records the resolved version in
// Main.Version; `go install ./...` from a source tree records "(devel)" and the
// VCS revision instead, so each case is handled separately rather than printing
// a bare "dev" for both.
func version() string {
	info, _ := debug.ReadBuildInfo()
	return versionFrom(Version, info)
}

// versionFrom is version() with its inputs injected, so both fallback paths are
// testable — the real build info of a `go test` binary is neither.
func versionFrom(ldflags string, info *debug.BuildInfo) string {
	if ldflags != "dev" || info == nil {
		return ldflags
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var rev, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if rev == "" {
		return ldflags
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if modified == "true" {
		rev += "-dirty"
	}
	return "devel-" + rev
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mcp-trace: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	v := viper.New()

	root := &cobra.Command{
		Use:     "mcp-trace",
		Short:   "Transparent MCP proxy with OpenTelemetry span emission",
		Version: version(),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfgFile, _ := cmd.Flags().GetString("config")

			// Handle --stdio flag with remaining args
			useStdio, _ := cmd.Flags().GetBool("stdio")
			if useStdio {
				if len(args) == 0 {
					return fmt.Errorf("--stdio requires a command: mcp-trace --stdio -- <command> [args...]")
				}
				v.Set("transport", "stdio")
				v.Set("stdio.command", args[0])
				if len(args) > 1 {
					v.Set("stdio.args", args[1:])
				}
			}

			cfg, err := config.Load(v, cfgFile)
			if err != nil {
				return err
			}
			return serve(cfg)
		},
	}

	config.BindFlags(root, v)

	return root.Execute()
}

func serve(cfg config.Config) error {
	logger := newLogger(cfg.LogLevel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialise OTel provider.
	provider, err := telemetry.New(ctx, telemetry.Config{
		UseHTTP:      cfg.OTel.HTTP,
		GRPCEndpoint: cfg.OTel.Endpoint,
		HTTPEndpoint: cfg.OTel.HTTPEndpoint,
		Insecure:     cfg.OTel.Insecure,
		ServiceName:  cfg.OTel.ServiceName,
		Logger:       logger,
	})
	if err != nil {
		return fmt.Errorf("initialising OTel: %w", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := provider.Shutdown(shutdownCtx); err != nil {
			logger.Error("OTel shutdown error", "err", err)
		}
	}()

	filter := &proxy.Filter{
		TraceAll:         cfg.TraceAll,
		IncludeLifecycle: cfg.IncludeLifecycle,
	}

	var handler http.Handler
	var subprocessErrMu sync.Mutex
	var subprocessErr error

	// Create appropriate proxy based on transport mode
	switch cfg.Transport {
	case config.TransportSSE:
		p, err := proxy.New(ctx, cfg.Target, filter, provider.Tracer, logger)
		if err != nil {
			return fmt.Errorf("creating SSE proxy: %w", err)
		}
		p.CaptureArgs = cfg.CaptureToolArgs
		handler = p

	case config.TransportStreamable:
		p, err := proxy.NewStreamableProxy(ctx, cfg.Target, filter, provider.Tracer, logger)
		if err != nil {
			return fmt.Errorf("creating Streamable proxy: %w", err)
		}
		p.CaptureArgs = cfg.CaptureToolArgs
		handler = p

	case config.TransportStdio:
		p, err := proxy.NewStdioProxy(ctx, cfg.Stdio.Command, cfg.Stdio.Args, filter, provider.Tracer, logger)
		if err != nil {
			return fmt.Errorf("creating stdio proxy: %w", err)
		}
		p.CaptureArgs = cfg.CaptureToolArgs
		if err := p.Start(); err != nil {
			return fmt.Errorf("starting stdio subprocess: %w", err)
		}
		defer func() {
			if err := p.Stop(); err != nil {
				logger.Error("stdio proxy shutdown error", "err", err)
			}
		}()
		
		// Monitor subprocess and shut down if it exits unexpectedly
		go func() {
			err := <-p.ExitErr()
			if err != nil {
				logger.Error("subprocess died, shutting down proxy", "err", err)
				subprocessErrMu.Lock()
				if subprocessErr == nil {
					subprocessErr = err
				}
				subprocessErrMu.Unlock()
				cancel()
			}
		}()
		
		handler = p

	default:
		return fmt.Errorf("unsupported transport mode: %s", cfg.Transport)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-quit:
			logger.Info("shutting down")
			cancel()
		case <-ctx.Done():
			// Context cancelled (e.g. subprocess died)
		}
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("mcp-trace listening",
		"port", cfg.Port,
		"transport", cfg.Transport,
		"target", cfg.Target,
		"trace_all", cfg.TraceAll,
		"include_lifecycle", cfg.IncludeLifecycle,
		"otel_http", cfg.OTel.HTTP,
		"capture_tool_args", cfg.CaptureToolArgs,
	)

	if cfg.Transport == config.TransportStdio {
		logger.Info("stdio subprocess",
			"command", cfg.Stdio.Command,
			"args", cfg.Stdio.Args,
		)
	}

	serveErr := srv.ListenAndServe()
	
	// Check if subprocess died (for stdio transport)
	subprocessErrMu.Lock()
	savedSubprocessErr := subprocessErr
	subprocessErrMu.Unlock()
	
	if savedSubprocessErr != nil {
		return fmt.Errorf("subprocess died: %w", savedSubprocessErr)
	}
	
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return fmt.Errorf("server: %w", serveErr)
	}
	return nil
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
