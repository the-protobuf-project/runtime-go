// Package options holds the configuration for the grpc HybridServer: service
// identity (name, description, version), the gRPC/HTTP/MCP endpoints, the
// environment mode, the health/HTTP-3 toggles, and the OpenTelemetry metric
// selection. Build an [Options] value and hand it to grpc.NewHybridServer.
//
// # Example
//
//	opts := options.Options{
//	    ServiceName:  "bookstore",
//	    Environment:  options.Production,
//	    GRPC:         options.GRPCOptions{Host: "0.0.0.0", Port: 50051},
//	    HTTP:         options.HTTPOptions{Host: "0.0.0.0", Port: 8080},
//	    EnableHTTP:   true,
//	    EnableHealth: true,
//	}
//	srv := grpc.NewHybridServer(opts)
package options
