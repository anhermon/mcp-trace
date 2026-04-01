package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paperclipai/mcp-trace/internal/config"
	"github.com/paperclipai/mcp-trace/internal/proxy"
	"github.com/paperclipai/mcp-trace/internal/telemetry"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Version is set at build time via -ldflags.
var Version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mcp-trace: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	v := viper.New()
	var cfgFile string

	root := &cobra.Command{
		Use:     "mcp-trace",
		Short:   "Transparent MCP proxy with OpenTelemetry span emission",
		Version: Version,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgFile, _ = cmd.Flags().GetString("config")
			cfg, err := config.Load(v, cfgFile)
			if err != nil {
				return err
			}
			return serve(cfg)
		},
	}

	config.BindFlags(root, v)
	_ = cfgFile

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

	p, err := proxy.New(cfg.Target, filter, provider.Tracer, logger)
	if err != nil {
		return fmt.Errorf("creating proxy: %w", err)
	}

	addr := fmt.Sprintf(":%d", cfg.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: p,
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
