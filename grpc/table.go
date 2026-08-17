package grpc

import (
	"runtime/debug"
	"strings"

	"github.com/fatih/color"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
)

// section color palette — each protocol layer gets its own tint.
var (
	colorGRPC = color.New(color.FgCyan, color.Bold).SprintFunc()
	colorMCP  = color.New(color.FgYellow, color.Bold).SprintFunc()
	colorA2A  = color.New(color.FgMagenta, color.Bold).SprintFunc()
	colorHTTP = color.New(color.FgGreen, color.Bold).SprintFunc()
)

// sectionColor returns the color function for a given section label.
func sectionColor(section string) func(...interface{}) string {
	switch {
	case strings.HasPrefix(section, "gRPC"):
		return colorGRPC
	case strings.HasPrefix(section, "MCP"):
		return colorMCP
	case strings.HasPrefix(section, "A2A"):
		return colorA2A
	default:
		return colorHTTP
	}
}

// buildHash reads the VCS revision baked in by `go build` (the same hash
// shown by `git rev-parse --short HEAD`). Falls back to "dev" when building
// with `go run` or without VCS information.
func buildHash() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			if len(s.Value) >= 7 {
				return s.Value[:7]
			}
			return s.Value
		}
	}
	return "dev"
}

// envColor returns a colored environment label.
func envColor(env options.ServerEnvironment) string {
	switch env {
	case options.Production:
		return color.New(color.FgRed, color.Bold).Sprint(strings.ToUpper(string(env)))
	case options.Staging:
		return color.New(color.FgYellow, color.Bold).Sprint(strings.ToUpper(string(env)))
	case options.Debug, options.Development:
		return color.New(color.FgGreen, color.Bold).Sprint(strings.ToUpper(string(env)))
	default:
		return color.New(color.FgWhite).Sprint(strings.ToUpper(string(env)))
	}
}
