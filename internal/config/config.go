package config

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// TransportMode defines how mcp-trace connects to the MCP server.
type TransportMode string

const (
	TransportSSE        TransportMode = "sse"
	TransportStreamable TransportMode = "streamable"
	TransportStdio      TransportMode = "stdio"
)

// Config holds all mcp-trace configuration.
type Config struct {
	Target           string        `mapstructure:"target"`
	Transport        TransportMode `mapstructure:"transport"`
	Port             int           `mapstructure:"port"`
	LogLevel         string        `mapstructure:"log_level"`
	TraceAll         bool          `mapstructure:"trace_all"`
	IncludeLifecycle bool          `mapstructure:"include_lifecycle"`
	CaptureToolArgs  bool          `mapstructure:"capture_tool_args"`
	ConfigFile       string        `mapstructure:"-"`

	// Stdio holds the command and arguments for stdio transport mode.
	Stdio StdioConfig `mapstructure:"stdio"`

	OTel OTelConfig `mapstructure:"otel"`
}

// StdioConfig holds configuration for stdio transport mode.
type StdioConfig struct {
	Command string   `mapstructure:"command"`
	Args    []string `mapstructure:"args"`
}

// OTelConfig holds OpenTelemetry exporter settings.
type OTelConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	HTTP         bool   `mapstructure:"http"`
	HTTPEndpoint string `mapstructure:"http_endpoint"`
	Insecure     bool   `mapstructure:"insecure"`
	ServiceName  string `mapstructure:"service_name"`
}

// Defaults returns a Config with sensible defaults.
func Defaults() Config {
	return Config{
		Port:      8001,
		Transport: TransportSSE, // default to SSE for backward compatibility
		LogLevel:  "info",
		OTel: OTelConfig{
			Endpoint:     "localhost:4317",
			HTTPEndpoint: "http://localhost:4318",
			Insecure:     true,
			ServiceName:  "mcp-trace",
		},
	}
}

// BindFlags registers all CLI flags onto cmd and binds them to viper.
func BindFlags(cmd *cobra.Command, v *viper.Viper) {
	defaults := Defaults()

	cmd.Flags().String("target", "", "Upstream MCP server URL (required for sse/streamable), e.g. http://localhost:8000/sse")
	cmd.Flags().String("transport", string(defaults.Transport), "Transport mode: sse|streamable|stdio")
	cmd.Flags().Bool("stdio", false, "Use stdio transport (shorthand for --transport stdio); remaining args become the command")
	cmd.Flags().Int("port", defaults.Port, "Local port to listen on")
	cmd.Flags().String("otel-endpoint", defaults.OTel.Endpoint, "OTLP gRPC endpoint")
	cmd.Flags().Bool("otel-http", false, "Use HTTP OTLP exporter instead of gRPC")
	cmd.Flags().String("otel-http-endpoint", defaults.OTel.HTTPEndpoint, "OTLP HTTP endpoint")
	cmd.Flags().Bool("otel-insecure", defaults.OTel.Insecure, "Disable TLS for OTLP connection")
	cmd.Flags().String("service-name", defaults.OTel.ServiceName, "OTel service.name attribute")
	cmd.Flags().Bool("trace-all", false, "Trace all JSON-RPC methods, not just tools/call")
	cmd.Flags().Bool("include-lifecycle", false, "Include initialize/ping/notifications in traces")
	cmd.Flags().Bool("capture-tool-args", false, "Record full tool arguments on spans (off by default: arguments are user data and may contain secrets)")
	cmd.Flags().String("log-level", defaults.LogLevel, "Log level: debug|info|warn|error")
	cmd.Flags().String("config", "", "Path to .mcp-trace.yaml config file")

	_ = v.BindPFlag("target", cmd.Flags().Lookup("target"))
	_ = v.BindPFlag("transport", cmd.Flags().Lookup("transport"))
	_ = v.BindPFlag("port", cmd.Flags().Lookup("port"))
	_ = v.BindPFlag("otel.endpoint", cmd.Flags().Lookup("otel-endpoint"))
	_ = v.BindPFlag("otel.http", cmd.Flags().Lookup("otel-http"))
	_ = v.BindPFlag("otel.http_endpoint", cmd.Flags().Lookup("otel-http-endpoint"))
	_ = v.BindPFlag("otel.insecure", cmd.Flags().Lookup("otel-insecure"))
	_ = v.BindPFlag("otel.service_name", cmd.Flags().Lookup("service-name"))
	_ = v.BindPFlag("trace_all", cmd.Flags().Lookup("trace-all"))
	_ = v.BindPFlag("include_lifecycle", cmd.Flags().Lookup("include-lifecycle"))
	_ = v.BindPFlag("capture_tool_args", cmd.Flags().Lookup("capture-tool-args"))
	_ = v.BindPFlag("log_level", cmd.Flags().Lookup("log-level"))
}

// Load reads the config file (if any) and unmarshals into Config.
// Flag values (already bound via BindFlags) take precedence over file values.
func Load(v *viper.Viper, cfgFile string) (Config, error) {
	cfg := Defaults()

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.SetConfigName(".mcp-trace")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME")
	}

	v.SetEnvPrefix("MCP_TRACE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return cfg, fmt.Errorf("reading config file: %w", err)
		}
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return cfg, fmt.Errorf("unmarshalling config: %w", err)
	}

	// Handle --stdio shorthand flag
	if v.GetBool("stdio") {
		cfg.Transport = TransportStdio
	}

	// Validate configuration based on transport mode
	switch cfg.Transport {
	case TransportSSE, TransportStreamable:
		if cfg.Target == "" {
			return cfg, fmt.Errorf("--target is required for %s transport", cfg.Transport)
		}
	case TransportStdio:
		if cfg.Stdio.Command == "" {
			return cfg, fmt.Errorf("stdio.command is required for stdio transport (use --stdio -- <command> [args...])")
		}
	default:
		return cfg, fmt.Errorf("invalid transport mode: %s (must be sse, streamable, or stdio)", cfg.Transport)
	}

	return cfg, nil
}
