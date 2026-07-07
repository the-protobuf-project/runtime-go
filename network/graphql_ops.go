// GraphQL typed operations: Query, Mutation, and MutationWithInput.
package network

import (
	"context"
	"fmt"
)

// GraphQLResult is the result of an asynchronous GraphQL operation. Response holds
// the decoded result (the filled query/mutation struct or a raw response map);
// Error is non-nil if the operation failed.
type GraphQLResult struct {
	Response interface{}
	Error    error
}

// Query runs a GraphQL query asynchronously. query is a struct whose graphql struct
// tags define the query shape; it is filled with the response on success. Variables
// may be nil. The returned channel receives exactly one GraphQLResult and is closed.
func (g *GraphQLClient) Query(query interface{}, variables map[string]interface{}) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), g.Timeout)
		defer cancel()
		if err := g.client.Query(ctx, query, variables); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute query: %w", err)}
			return
		}
		resultChan <- GraphQLResult{Response: query}
	}()
	return resultChan
}

// Mutation runs a GraphQL mutation asynchronously. mutation is a struct with graphql
// tags; it is filled with the response on success. Variables may be nil. The returned
// channel receives one GraphQLResult and is closed.
func (g *GraphQLClient) Mutation(mutation any, variables map[string]interface{}) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), g.Timeout)
		defer cancel()
		if err := g.client.Mutate(ctx, mutation, variables); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute mutation: %w", err)}
			return
		}
		resultChan <- GraphQLResult{Response: mutation}
	}()
	return resultChan
}

// MutationWithInput runs a mutation by building arguments from an input struct with
// json tags. mutationName is the mutation field (e.g. "createUser"); input is the
// argument struct; response is a struct pointer that receives the result. The
// returned channel receives one GraphQLResult.
func (g *GraphQLClient) MutationWithInput(mutationName string, input interface{}, response interface{}) <-chan GraphQLResult {
	resultChan := make(chan GraphQLResult, 1)
	go func() {
		defer close(resultChan)
		if g.client == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("GraphQL client is not initialized")}
			return
		}
		variables, err := StructToMap(input)
		if err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to convert input: %w", err)}
			return
		}
		args := BuildGraphQLArgs(variables)
		mutationStruct := buildDynamicMutation(mutationName, args, response)
		if mutationStruct == nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("response must be a pointer to a struct")}
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), g.Timeout)
		defer cancel()
		if err := g.client.Mutate(ctx, mutationStruct, nil); err != nil {
			resultChan <- GraphQLResult{Error: fmt.Errorf("failed to execute mutation: %w", err)}
			return
		}
		resultChan <- GraphQLResult{Response: mutationStruct}
	}()
	return resultChan
}
