package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func TestDefaults(t *testing.T) {
	cfg := Defaults()

	if cfg.Port != 8001 {
		t.Errorf("expected Port=8001, got %d", cfg.Port)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected LogLevel=info, got %s", cfg.LogLevel)
	}
	if cfg.OTel.Endpoint != "localhost:4317" {
		t.Errorf("expected OTel.Endpoint=localhost:4317, got %s", cfg.OTel.Endpoint)
	}
	if cfg.OTel.HTTPEndpoint != "http://localhost:4318" {
		t.Errorf("expected OTel.HTTPEndpoint=http://localhost:4318, got %s", cfg.OTel.HTTPEndpoint)
	}
	if !cfg.OTel.Insecure {
		t.Error("expected OTel.Insecure=true")
	}
	if cfg.OTel.ServiceName != "mcp-trace" {
		t.Errorf("expected OTel.ServiceName=mcp-trace, got %s", cfg.OTel.ServiceName)
	}
	if cfg.Target != "" {
		t.Errorf("expected Target to be empty, got %s", cfg.Target)
	}
}

func TestLoad_MissingTarget(t *testing.T) {
	v := viper.New()
	_, err := Load(v, "")
	if err == nil {
		t.Fatal("expected error for missing --target, got nil")
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("MCP_TRACE_TARGET", "http://test-server")

	// BindFlags registers the "target" key with viper. The real binary always
	// calls it before Load; without it viper has no key for AutomaticEnv to fill.
	v := viper.New()
	BindFlags(&cobra.Command{}, v)

	cfg, err := Load(v, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Target != "http://test-server" {
		t.Errorf("expected Target=http://test-server, got %s", cfg.Target)
	}
}

func TestLoad_ConfigFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, ".mcp-trace.yaml")
	content := `
target: "http://config-server:9000"
port: 9001
log_level: debug
otel:
  endpoint: "otel-collector:4317"
  service_name: "my-service"
`
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	v := viper.New()
	cfg, err := Load(v, cfgPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Target != "http://config-server:9000" {
		t.Errorf("expected Target=http://config-server:9000, got %s", cfg.Target)
	}
	if cfg.Port != 9001 {
		t.Errorf("expected Port=9001, got %d", cfg.Port)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel=debug, got %s", cfg.LogLevel)
	}
	if cfg.OTel.Endpoint != "otel-collector:4317" {
		t.Errorf("expected OTel.Endpoint=otel-collector:4317, got %s", cfg.OTel.Endpoint)
	}
	if cfg.OTel.ServiceName != "my-service" {
		t.Errorf("expected OTel.ServiceName=my-service, got %s", cfg.OTel.ServiceName)
	}
}
