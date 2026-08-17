package grpc

import (
	"fmt"
	"sort"
)

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
