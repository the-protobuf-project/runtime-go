package grpc

import (
	"context"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/the-protobuf-project/runtime-go/grpc/options"
	"github.com/the-protobuf-project/runtime-go/grpc/shared"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats/opentelemetry"
)

func setupOtelExporter() []grpc.ServerOption {
	ctx := context.Background()
	serverOpts := make([]grpc.ServerOption, 0, 2)

	endpoint := getFromEnvOrDefault("PULSE_TELEMETRY_OTLP_ENDPOINT", "localhost:12005")
	shared.Pulse.Logger.Debugf("setupOtelExporter: creating OTLP metric exporter → %s (insecure)", endpoint)

	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		shared.Pulse.Logger.Errorf("failed to create OTLP metric exporter: %s", err.Error())
		return serverOpts
	}
	shared.Pulse.Logger.Debugf("setupOtelExporter: exporter created, configuring MeterProvider (interval=10s)")
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exporter,
			metric.WithInterval(10*time.Second),
		)),
	)
	so := opentelemetry.ServerOption(opentelemetry.Options{
		MetricsOptions: opentelemetry.MetricsOptions{
			MeterProvider: meterProvider,
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

	serverOpts = append(serverOpts, so, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	shared.Pulse.Logger.Debugf("setupOtelExporter: OTel server options ready (%d option(s))", len(serverOpts))
	return serverOpts
}
