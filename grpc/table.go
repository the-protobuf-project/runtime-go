package grpc

import (
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"

	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// section color palette — each protocol layer gets its own tint.
var (
	colorGRPC = color.New(color.FgCyan, color.Bold).SprintFunc()
	colorMCP  = color.New(color.FgYellow, color.Bold).SprintFunc()
	colorHTTP = color.New(color.FgGreen, color.Bold).SprintFunc()
)

// sectionColor returns the color function for a given section label.
func sectionColor(section string) func(...interface{}) string {
	switch {
	case strings.HasPrefix(section, "gRPC"):
		return colorGRPC
	case strings.HasPrefix(section, "MCP"):
		return colorMCP
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

// printStartupBanner prints the ASCII-art service name, a build info line,
// and a color-coded startup summary table. Warnings follow the table so they
// always appear below the visual summary.
func (s *HybridServer) printStartupBanner(mcpEndpoints []mcpEndpointInfo) {
	magenta := color.New(color.FgMagenta, color.Bold).SprintFunc()
	lines := figure.NewFigure(s.opts.ServiceName, "speed", true).Slicify()
	first, last := 0, len(lines)
	for first < last && strings.TrimSpace(lines[first]) == "" {
		first++
	}
	for last > first && strings.TrimSpace(lines[last-1]) == "" {
		last--
	}
	for _, l := range lines[first:last] {
		fmt.Println(magenta(l))
	}

	dim := color.New(color.Faint).SprintFunc()
	sep := dim(" * ")
	fmt.Printf("\n%s%s%s%s%s\n\n",
		color.New(color.FgWhite, color.Bold).Sprintf("Version: %s", s.opts.Version),
		sep,
		fmt.Sprintf("Environment: %s", envColor(s.opts.Environment)),
		sep,
		dim("Build: #"+buildHash()),
	)

	var data [][]string

	grpcAddr := fmt.Sprintf("%s:%d", s.opts.GRPC.Host, s.opts.GRPC.Port)
	otlpEndpoint := getFromEnvOrDefault("PULSE_TELEMETRY_OTLP_ENDPOINT", "localhost:12005")
	isReflection := s.opts.Environment == options.Debug || s.opts.Environment == options.Development

	addRow := func(section, component, addr, detail string) {
		col := sectionColor(section)
		data = append(data, []string{col(section), component, addr, detail})
	}

	// gRPC user services
	if s.grpcServer != nil {
		for _, name := range grpcServiceNamesFromMap(s.grpcServer.GetServiceInfo()) {
			addRow("gRPC", name, grpcAddr, tlsLabel(s.cert != nil))
		}
	}
	if s.opts.EnableHealth {
		addRow("gRPC", "Health Check", grpcAddr, "grpc.health.v1")
	}
	if isReflection {
		addRow("gRPC", "Reflection", grpcAddr, "grpcurl-compatible")
	}
	addRow("gRPC", "OpenTelemetry", grpcAddr, "OTLP → "+otlpEndpoint)

	// MCP
	for i, ep := range mcpEndpoints {
		path := ep.url
		if u, err := url.Parse(ep.url); err == nil && u.Path != "" {
			path = u.Path
		}
		addRow("MCP",
			fmt.Sprintf("Server %d  (%s)", i+1, ep.transport),
			fmt.Sprintf("%s:%d", s.opts.MCP.Host, ep.port),
			path,
		)
	}

	// HTTP/1.1
	if s.opts.EnableHTTP {
		httpAddr := fmt.Sprintf("%s:%d", s.opts.HTTP.Host, s.opts.HTTP.Port)
		scheme := "http"
		if s.cert != nil {
			scheme = "https"
		}
		addRow("HTTP/1.1", pluralise(len(s.httpServiceFuncs), "REST Route")+"  +  /health", httpAddr, scheme)
	}

	// HTTP/3
	if s.opts.ExperimentalHttp3 {
		h3Addr := fmt.Sprintf("%s:%d", s.opts.HTTP.Host, s.opts.HTTP.Port+1)
		addRow("HTTP/3 ⚗", "REST Gateway (QUIC)", h3Addr, "TLS 1.3")
	}

	t := tablewriter.NewTable(os.Stdout,
		tablewriter.WithRenderer(renderer.NewBlueprint(tw.Rendition{
			Settings: tw.Settings{
				Separators: tw.Separators{BetweenRows: tw.On},
			},
		})),
		tablewriter.WithConfig(tablewriter.Config{
			Header: tw.CellConfig{
				Alignment: tw.CellAlignment{Global: tw.AlignCenter},
			},
			Row: tw.CellConfig{
				Merging:   tw.CellMerging{Mode: tw.MergeHierarchical},
				Alignment: tw.CellAlignment{Global: tw.AlignLeft},
			},
		}),
	)
	t.Header([]string{"Section", "Component", "Address", "Detail"})
	_ = t.Bulk(data)
	_ = t.Render()
	fmt.Println()

	if s.cert == nil {
		shared.Pulse.Logger.Warn("gRPC: running without TLS (plaintext)")
	}
	if s.opts.EnableHealth {
		shared.Pulse.Logger.Warn("Health check service enabled")
	}
	if isReflection {
		shared.Pulse.Logger.Warn("Reflection service enabled (dev/debug only)")
	}
}

// grpcServiceNamesFromMap extracts and sorts gRPC service names from GetServiceInfo(),
// filtering out well-known built-in services that have their own table rows.
func grpcServiceNamesFromMap[V any](info map[string]V) []string {
	skip := map[string]bool{
		"grpc.health.v1.Health":                    true,
		"grpc.reflection.v1alpha.ServerReflection": true,
		"grpc.reflection.v1.ServerReflection":      true,
	}
	var names []string
	for name := range info {
		if !skip[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func tlsLabel(hasTLS bool) string {
	if hasTLS {
		return "TLS ✓"
	}
	return "plaintext"
}

func pluralise(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}
