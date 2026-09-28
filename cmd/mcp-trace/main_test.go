package main

import (
	"runtime/debug"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// TestVersionFrom covers the reason `mcp-trace --version` used to print "dev"
// for every `go install`ed binary: that path gets no -ldflags, so the version
// has to come from the module metadata the toolchain stamps in instead.
func TestVersionFrom(t *testing.T) {
	devel := func(rev, modified string) *debug.BuildInfo {
		return &debug.BuildInfo{
			Main: debug.Module{Version: "(devel)"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: rev},
				{Key: "vcs.modified", Value: modified},
			},
		}
	}

	for _, tc := range []struct {
		name    string
		ldflags string
		info    *debug.BuildInfo
		want    string
	}{
		{"ldflags wins", "v1.0.1", devel("abc", "false"), "v1.0.1"},
		{"go install @latest", "dev",
			&debug.BuildInfo{Main: debug.Module{Version: "v1.0.1"}}, "v1.0.1"},
		{"go install from source tree", "dev",
			devel("0123456789abcdef0123", "false"), "devel-0123456789ab"},
		{"dirty source tree", "dev", devel("0123456789abcdef0123", "true"), "devel-0123456789ab-dirty"},
		{"no build info at all", "dev", nil, "dev"},
		{"no vcs stamp", "dev", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, "dev"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := versionFrom(tc.ldflags, tc.info); got != tc.want {
				t.Errorf("versionFrom() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRootCommandSilenceFlags verifies that operational failures (like subprocess
// death) print the error without dumping usage text.
func TestRootCommandSilenceFlags(t *testing.T) {
	v := viper.New()
	root := &cobra.Command{
		Use:           "mcp-trace",
		Short:         "test",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	if !root.SilenceUsage {
		t.Error("SilenceUsage should be true to prevent usage dump on operational errors")
	}
	if !root.SilenceErrors {
		t.Error("SilenceErrors should be true to let main() handle error formatting")
	}

	_ = v
}
