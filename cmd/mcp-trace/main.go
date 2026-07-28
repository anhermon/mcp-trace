package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/anhermon/mcp-trace/internal/config"
	"github.com/anhermon/mcp-trace/internal/proxy"
	"github.com/anhermon/mcp-trace/internal/telemetry"
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
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgFile, _ := cmd.Flags().GetString("config")
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

	p, err := proxy.New(ctx, cfg.Target, filter, provider.Tracer, logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}
	p.CaptureArgs = cfg.CaptureToolArgs

	addr := fmt.Sprintf(":%d", cfg.Port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           p,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		logger.Info("shutting down")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("mcp-trace listening",
		"port", cfg.Port,
		"target", cfg.Target,
		"trace_all", cfg.TraceAll,
		"include_lifecycle", cfg.IncludeLifecycle,
		"otel_http", cfg.OTel.HTTP,
		"capture_tool_args", cfg.CaptureToolArgs,
	)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server: %w", err)
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
