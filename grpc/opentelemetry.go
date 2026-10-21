package grpc

import (
	_ "github.com/joho/godotenv/autoload"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

// setupOtelExporter instruments the server with OTel: a span per call
// (otelgrpc) and gRPC's own metrics. Both go through OTel's global tracer and
// meter providers, which the application installs when it builds its telemetry
// (and shares with this package via observability.Adopt), so every span and
// series lands in the application's one pipeline, under its service name.
//
// This used to stand up a metric exporter of its own, at
// PULSE_TELEMETRY_OTLP_ENDPOINT or localhost:12005: a second pipeline that
// nothing listened to by default, so gRPC metrics were silently dropped. The
// global meter provider is a delegate until the application sets it, so
// building the option before then is fine.
func setupOtelExporter() []grpc.ServerOption {
	so := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{
			MeterProvider: otel.GetMeterProvider(),
			// These are example experimental gRPC metrics, which are disabled
			// by default and must be explicitly enabled. For the full,
			// up-to-date list of metrics, see:
			// https://grpc.io/docs/guides/opentelemetry-metrics/#instruments
			Metrics: opentelemetry.DefaultMetrics().Add(
				// Server side metrics
				string(options.AvailableOpenTelemetryMetricTypes.ServerMetricType.ServerCallSentTotalCompressedMessageSize),
				string(options.AvailableOpenTelemetryMetricTypes.ServerMetricType.ServerCallRcvdTotalCompressedMessageSize),
				string(options.AvailableOpenTelemetryMetricTypes.ServerMetricType.ServerCallDuration),
				string(options.AvailableOpenTelemetryMetricTypes.ServerMetricType.ServerCallStarted),

				// Client side metrics
				string(options.AvailableOpenTelemetryMetricTypes.ClientMetricType.ClientAttemptDuration),
				string(options.AvailableOpenTelemetryMetricTypes.ClientMetricType.ClientAttemptRcvdTotalCompressedMessageSize),
				string(options.AvailableOpenTelemetryMetricTypes.ClientMetricType.ClientAttemptSentTotalCompressedMessageSize),
				string(options.AvailableOpenTelemetryMetricTypes.ClientMetricType.ClientAttemptStarted),
				string(options.AvailableOpenTelemetryMetricTypes.ClientMetricType.ClientCallDuration),

				// Load balancer metrics (pick_first)
				string(options.AvailableOpenTelemetryMetricTypes.LoadBalancerMetricType.LBPickFirstConnectionAttemptsFailed),
				string(options.AvailableOpenTelemetryMetricTypes.LoadBalancerMetricType.LBPickFirstConnectionAttemptsSucceeded),
				string(options.AvailableOpenTelemetryMetricTypes.LoadBalancerMetricType.LBPickFirstDisconnections),

				// Load balancer metrics (weighted round robin)
				string(options.AvailableOpenTelemetryMetricTypes.WRRLoadBalancerMetricType.WRRFallback),
				string(options.AvailableOpenTelemetryMetricTypes.WRRLoadBalancerMetricType.WRREndpointWeightNotYetUsable),
				string(options.AvailableOpenTelemetryMetricTypes.WRRLoadBalancerMetricType.WRREndpointWeightStale),
				string(options.AvailableOpenTelemetryMetricTypes.WRRLoadBalancerMetricType.WRREndpointWeights),

				// xDS client metrics
				string(options.AvailableOpenTelemetryMetricTypes.XDSClientMetricType.XDSClientConnected),
				string(options.AvailableOpenTelemetryMetricTypes.XDSClientMetricType.XDSClientServerFailure),
				string(options.AvailableOpenTelemetryMetricTypes.XDSClientMetricType.XDSClientResourceUpdatesValid),
				string(options.AvailableOpenTelemetryMetricTypes.XDSClientMetricType.XDSClientResourceUpdatesInvalid),
				string(options.AvailableOpenTelemetryMetricTypes.XDSClientMetricType.XDSClientResources),
			),
		},
	})

	return []grpc.ServerOption{so, grpc.StatsHandler(otelgrpc.NewServerHandler())}
}
