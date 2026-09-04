module github.com/the-protobuf-project/runtime-go/grpc

go 1.26.4

require (
	buf.build/go/protovalidate v1.2.0
	github.com/common-nighthawk/go-figure v0.0.0-20210622060536-734e95fb86be
	github.com/fatih/color v1.19.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.30.0
	github.com/joho/godotenv v1.5.1
	github.com/modelcontextprotocol/go-sdk v1.7.0
	github.com/olekukonko/tablewriter v1.1.4
	github.com/quic-go/quic-go v0.61.0
	github.com/the-protobuf-project/opentelementry/opentelementry-go v0.0.0-20260722091843-d33763c88e10
	github.com/the-protobuf-project/runtime-go/agents v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/observability v0.0.0-00010101000000-000000000000
	github.com/the-protobuf-project/runtime-go/telemetry v0.0.0-20260722084318-b90e81eeadb7
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.70.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.45.0
	go.opentelemetry.io/otel/sdk/metric v1.45.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260825221802-da73d73af1c5
	google.golang.org/grpc v1.83.1
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2 // indirect
	cel.dev/expr v0.25.2 // indirect
	github.com/a2aproject/a2a-go/v2 v2.4.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/charmbracelet/colorprofile v0.4.3 // indirect
	github.com/charmbracelet/lipgloss v1.1.0 // indirect
	github.com/charmbracelet/log v1.0.0 // indirect
	github.com/charmbracelet/x/ansi v0.11.7 // indirect
	github.com/charmbracelet/x/cellbuf v0.0.15 // indirect
	github.com/charmbracelet/x/term v0.2.2 // indirect
	github.com/clipperhouse/displaywidth v0.11.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/foxglove/mcap/go/mcap v1.9.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-logfmt/logfmt v0.6.1 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/cel-go v0.29.2 // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/pyroscope-go v1.4.1 // indirect
	github.com/grafana/pyroscope-go/godeltaprof v0.1.12 // indirect
	github.com/klauspost/compress v1.19.0 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/parsers/json v1.0.0 // indirect
	github.com/knadh/koanf/parsers/toml v0.1.0 // indirect
	github.com/knadh/koanf/parsers/yaml v1.1.0 // indirect
	github.com/knadh/koanf/providers/env v1.1.0 // indirect
	github.com/knadh/koanf/providers/file v1.2.1 // indirect
	github.com/knadh/koanf/v2 v2.3.5 // indirect
	github.com/lucasb-eyer/go-colorful v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mattn/go-runewidth v0.0.24 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/muesli/termenv v0.16.0 // indirect
	github.com/olekukonko/cat v0.0.0-20250911104152-50322a0618f6 // indirect
	github.com/olekukonko/errors v1.3.0 // indirect
	github.com/olekukonko/ll v0.1.8 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/log v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20260709172345-9ea1abe57597 // indirect
	golang.org/x/mod v0.40.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260831171406-18b4a7587f8a // indirect
)

// agents, observability, and telemetry are versioned alongside this module
// in runtime-go and have no tagged release carrying the contracts used here
// yet. Drop these once they are published.
replace github.com/the-protobuf-project/runtime-go/agents => ../agents

replace github.com/the-protobuf-project/runtime-go/observability => ../observability

replace github.com/the-protobuf-project/runtime-go/telemetry => ../telemetry
