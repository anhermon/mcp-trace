package config

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config holds all mcp-trace configuration.
type Config struct {
	Target           string `mapstructure:"target"`
	Port             int    `mapstructure:"port"`
	LogLevel         string `mapstructure:"log_level"`
	TraceAll         bool   `mapstructure:"trace_all"`
	IncludeLifecycle bool   `mapstructure:"include_lifecycle"`
	CaptureToolArgs  bool   `mapstructure:"capture_tool_args"`
	ConfigFile       string `mapstructure:"-"`

	OTel OTelConfig `mapstructure:"otel"`
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
		Port:     8001,
		LogLevel: "info",
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

	cmd.Flags().String("target", "", "Upstream MCP server URL (required), e.g. http://localhost:8000/sse")
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

	if cfg.Target == "" {
		return cfg, fmt.Errorf("--target is required")
	}

	return cfg, nil
}
