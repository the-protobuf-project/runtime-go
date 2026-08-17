package grpc

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/common-nighthawk/go-figure"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/the-protobuf-project/runtime-go/agents"
	"github.com/the-protobuf-project/runtime-go/agents/a2a"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
)

// printStartupBanner prints the ASCII-art service name, a build info line,
// and a color-coded startup summary table. Warnings follow the table so they
// always appear below the visual summary.
func (s *HybridServer) printStartupBanner(endpoints []agents.Endpoint) {
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

	// Agent protocols — MCP and A2A, in the order the runtime brought them up.
	counts := map[agents.Protocol]int{}
	for _, ep := range endpoints {
		counts[ep.Protocol]++

		section, label := "MCP", "Server"
		host, port := s.opts.MCP.Host, s.opts.MCP.Port
		if ep.Protocol == agents.A2A {
			section, label = "A2A", "Agent"
			host, port = s.opts.A2A.Host, s.opts.A2A.Port
		}

		addr := fmt.Sprintf("%s:%d", host, port)
		detail := ep.URL
		if u, err := url.Parse(ep.URL); err == nil && u.Path != "" {
			detail = u.Path
		}
		if ep.Protocol == agents.A2A && ep.Detail != "" {
			// The card is what a client fetches first, so the summary says
			// where it is rather than making someone guess the well-known path.
			detail += "  card " + a2a.AgentCardPath
		}
		if ep.Protocol == agents.MCP && ep.Detail != "" {
			detail = ep.Detail
		}

		addRow(section,
			fmt.Sprintf("%s %d  (%s)", label, counts[ep.Protocol], ep.Transport),
			addr,
			detail,
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
		shared.Telemetry().Logger.Warn("gRPC: running without TLS (plaintext)")
	}
	if s.opts.EnableHealth {
		shared.Telemetry().Logger.Warn("Health check service enabled")
	}
	if isReflection {
		shared.Telemetry().Logger.Warn("Reflection service enabled (dev/debug only)")
	}
}
