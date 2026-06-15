package options

// OpenTelemetryMetricType is the base type for Metric identifiers.
type OpenTelemetryMetricType string

// Metric Category Types
type ClientMetricType OpenTelemetryMetricType
type ServerMetricType OpenTelemetryMetricType
type LoadBalancerMetricType OpenTelemetryMetricType
type WRRLoadBalancerMetricType OpenTelemetryMetricType
type XDSClientMetricType OpenTelemetryMetricType

// AvailableOpenTelemetryMetricTypes contains all supported OpenTelemetry
// metric names used for gRPC client, server, and load balancer instrumentation.
var AvailableOpenTelemetryMetricTypes = struct {

	// ClientMetricType contains metrics emitted by a gRPC client during
	// request attempts and call lifecycle events.
	ClientMetricType struct {

		// ClientAttemptStarted records the number of client call attempts started.
		ClientAttemptStarted ClientMetricType

		// ClientAttemptDuration records the duration of a single client call attempt.
		ClientAttemptDuration ClientMetricType

		// ClientAttemptSentTotalCompressedMessageSize records the total compressed
		// size of messages sent by the client in a single attempt.
		ClientAttemptSentTotalCompressedMessageSize ClientMetricType

		// ClientAttemptRcvdTotalCompressedMessageSize records the total compressed
		// size of messages received by the client in a single attempt.
		ClientAttemptRcvdTotalCompressedMessageSize ClientMetricType

		// ClientCallDuration records the total duration of the entire client call,
		// including retries and multiple attempts.
		ClientCallDuration ClientMetricType
	}

	// ServerMetricType contains metrics emitted by a gRPC server while
	// handling incoming RPC calls.
	ServerMetricType struct {

		// ServerCallStarted records the number of RPC calls started by the server.
		ServerCallStarted ServerMetricType

		// ServerCallDuration records the duration of an RPC handled by the server.
		ServerCallDuration ServerMetricType

		// ServerCallSentTotalCompressedMessageSize records the total compressed
		// size of messages sent by the server during the RPC lifecycle.
		ServerCallSentTotalCompressedMessageSize ServerMetricType

		// ServerCallRcvdTotalCompressedMessageSize records the total compressed
		// size of messages received by the server during the RPC lifecycle.
		ServerCallRcvdTotalCompressedMessageSize ServerMetricType
	}

	// LoadBalancerMetricType contains metrics related to gRPC load balancer
	// behavior and connection state.
	LoadBalancerMetricType struct {

		// LBPickFirstConnectionAttemptsSucceeded records the number of successful
		// connection attempts made by the pick_first load balancer policy.
		LBPickFirstConnectionAttemptsSucceeded LoadBalancerMetricType

		// LBPickFirstConnectionAttemptsFailed records the number of failed
		// connection attempts made by the pick_first load balancer policy.
		LBPickFirstConnectionAttemptsFailed LoadBalancerMetricType

		// LBPickFirstDisconnections records the number of disconnections observed
		// by the pick_first load balancer policy.
		LBPickFirstDisconnections LoadBalancerMetricType
	}

	// WRRLoadBalancerMetricType contains metrics emitted by the weighted round-robin load balancer.
	WRRLoadBalancerMetricType struct {

		// WRRFallback records the number of times the WRR scheduler falls back.
		WRRFallback WRRLoadBalancerMetricType

		// WRREndpointWeightNotYetUsable records endpoints whose weights are not yet usable.
		WRREndpointWeightNotYetUsable WRRLoadBalancerMetricType

		// WRREndpointWeightStale records endpoints whose weights have become stale.
		WRREndpointWeightStale WRRLoadBalancerMetricType

		// WRREndpointWeights records the weights assigned to endpoints.
		WRREndpointWeights WRRLoadBalancerMetricType
	}

	// XDSClientMetricType contains metrics emitted by the xDS client.
	XDSClientMetricType struct {

		// XDSClientConnected records whether the xDS client is connected.
		XDSClientConnected XDSClientMetricType

		// XDSClientServerFailure records failures communicating with the xDS server.
		XDSClientServerFailure XDSClientMetricType

		// XDSClientResourceUpdatesValid records valid resource updates received.
		XDSClientResourceUpdatesValid XDSClientMetricType

		// XDSClientResourceUpdatesInvalid records invalid resource updates received.
		XDSClientResourceUpdatesInvalid XDSClientMetricType

		// XDSClientResources records the number of active resources managed by the xDS client.
		XDSClientResources XDSClientMetricType
	}
}{
	// Initialize the values for that struct
	ClientMetricType: struct {
		ClientAttemptStarted                        ClientMetricType
		ClientAttemptDuration                       ClientMetricType
		ClientAttemptSentTotalCompressedMessageSize ClientMetricType
		ClientAttemptRcvdTotalCompressedMessageSize ClientMetricType
		ClientCallDuration                          ClientMetricType
	}{
		ClientAttemptStarted:                        "grpc.client.attempt.started",
		ClientAttemptDuration:                       "grpc.client.attempt.duration",
		ClientAttemptSentTotalCompressedMessageSize: "grpc.client.attempt.sent_total_compressed_message_size",
		ClientAttemptRcvdTotalCompressedMessageSize: "grpc.client.attempt.rcvd_total_compressed_message_size",
		ClientCallDuration:                          "grpc.client.call.duration",
	},
	ServerMetricType: struct {
		ServerCallStarted                        ServerMetricType
		ServerCallDuration                       ServerMetricType
		ServerCallSentTotalCompressedMessageSize ServerMetricType
		ServerCallRcvdTotalCompressedMessageSize ServerMetricType
	}{
		ServerCallStarted:                        "grpc.server.call.started",
		ServerCallDuration:                       "grpc.server.call.duration",
		ServerCallSentTotalCompressedMessageSize: "grpc.server.call.sent_total_compressed_message_size",
		ServerCallRcvdTotalCompressedMessageSize: "grpc.server.call.rcvd_total_compressed_message_size",
	},
	LoadBalancerMetricType: struct {
		LBPickFirstConnectionAttemptsSucceeded LoadBalancerMetricType
		LBPickFirstConnectionAttemptsFailed    LoadBalancerMetricType
		LBPickFirstDisconnections              LoadBalancerMetricType
	}{
		LBPickFirstConnectionAttemptsSucceeded: "grpc.lb.pick_first.connection_attempts_succeeded",
		LBPickFirstConnectionAttemptsFailed:    "grpc.lb.pick_first.connection_attempts_failed",
		LBPickFirstDisconnections:              "grpc.lb.pick_first.disconnections",
	},
	WRRLoadBalancerMetricType: struct {
		WRRFallback                   WRRLoadBalancerMetricType
		WRREndpointWeightNotYetUsable WRRLoadBalancerMetricType
		WRREndpointWeightStale        WRRLoadBalancerMetricType
		WRREndpointWeights            WRRLoadBalancerMetricType
	}{
		WRRFallback:                   "grpc.lb.wrr.rr_fallback",
		WRREndpointWeightNotYetUsable: "grpc.lb.wrr.endpoint_weight_not_yet_usable",
		WRREndpointWeightStale:        "grpc.lb.wrr.endpoint_weight_stale",
		WRREndpointWeights:            "grpc.lb.wrr.endpoint_weights",
	},
	XDSClientMetricType: struct {
		XDSClientConnected              XDSClientMetricType
		XDSClientServerFailure          XDSClientMetricType
		XDSClientResourceUpdatesValid   XDSClientMetricType
		XDSClientResourceUpdatesInvalid XDSClientMetricType
		XDSClientResources              XDSClientMetricType
	}{
		XDSClientConnected:              "grpc.xds_client.connected",
		XDSClientServerFailure:          "grpc.xds_client.server_failure",
		XDSClientResourceUpdatesValid:   "grpc.xds_client.resource_updates_valid",
		XDSClientResourceUpdatesInvalid: "grpc.xds_client.resource_updates_invalid",
		XDSClientResources:              "grpc.xds_client.resources",
	},
}
