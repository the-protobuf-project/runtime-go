// GraphQL raw-string operations: ExecuteRawQuery and ExecRawMutation.
package network

import (
	"context"
	"encoding/json"
	"fmt"
)

// ExecuteRawQuery sends a raw GraphQL query (or mutation) string with optional
// variables and returns the response as a map[string]interface{}. ctx carries
// cancellation/deadline and tracing through to the transport (bounded by g.Timeout). The
// returned channel receives one GraphQLResult whose Response is the parsed JSON map.
func (g *GraphQLClient) ExecuteRawQuery(ctx context.Context, query string, variables map[string]interface{}) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		ctx, cancel := context.WithTimeout(ctx, g.Timeout)
		defer cancel()
		raw, err := g.client.ExecRaw(ctx, query, variables)
		if err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute raw query: %w", err)}
			return
		}
		var response map[string]interface{}
		if err := json.Unmarshal(raw, &response); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to unmarshal raw response: %w", err)}
			return
		}
		resultChan <- GraphQLResult{Response: response}
	}()
	return resultChan
}

// ExecRawMutation sends a raw GraphQL mutation string (mutation must be a string at
// runtime) with optional variables and returns the response as a map. ctx carries
// cancellation/deadline and tracing through to the transport (bounded by g.Timeout). The
// channel receives one GraphQLResult whose Response is the parsed JSON map.
func (g *GraphQLClient) ExecRawMutation(ctx context.Context, mutation any, variables map[string]interface{}) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		mutationStr, ok := mutation.(string)
		if !ok {
			resultChan <- GraphQLResult{Error: fmt.Errorf("mutation must be a string")}
			return
		}
		ctx, cancel := context.WithTimeout(ctx, g.Timeout)
		defer cancel()
		raw, err := g.client.MutateRaw(ctx, mutationStr, variables)
		if err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute raw mutation: %w", err)}
			return
		}
		var response map[string]interface{}
		if err := json.Unmarshal(raw, &response); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to unmarshal raw mutation response: %w", err)}
			return
		}
		resultChan <- GraphQLResult{Response: response}
	}()
	return resultChan
}
