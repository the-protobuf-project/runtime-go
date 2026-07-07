// GraphQL client: connection lifecycle and scalar type aliases.
//
// This file provides the GraphQLClient used when ClientType is GraphQLConnClient.
// Operation methods (Query, Mutation, raw exec, Subscribe) live in the
// graphql_ops.go, graphql_raw.go, and graphql_subscription.go files.
package network

import (
	"context"
	"fmt"
	"net/http"

	graphql "github.com/hasura/go-graphql-client"
)

// DefaultGraphQLConnectivityQuery is sent to verify the GraphQL server is reachable
// during Connect/Reconnect. Override with ConnectionOptions.GraphQLConnectivityQuery
// for strict servers that limit introspection.
const DefaultGraphQLConnectivityQuery = `query { __typename }`

// GraphQL scalar type aliases: use network.Boolean, network.String, network.ID,
// etc. in variables and struct tags without importing the underlying graphql
// package.
type (
	Boolean = graphql.Boolean // true or false
	Float   = graphql.Float   // IEEE 754 double-precision
	Int     = graphql.Int     // signed 32-bit integer
	String  = graphql.String  // UTF-8 text
	ID      = graphql.ID      // unique identifier
)

// GraphQLClient is a GraphQL API client. Create via NewConnection(GraphQLConnClient)
// and AsGraphQLConnectionType. It embeds ConnectionOptions (URL, Timeout, Headers,
// SkipConnectivityCheck, GraphQLConnectivityQuery).
type GraphQLClient struct {
	client *graphql.Client
	ConnectionOptions
}

// newGraphQLClient builds an underlying graphql.Client for fullURL and attaches a
// request modifier that applies ConnectionOptions.Headers (e.g. auth tokens) to
// every request. Without this, headers set in opts would be dropped for GraphQL.
func newGraphQLClient(fullURL string, opts ConnectionOptions) *graphql.Client {
	client := graphql.NewClient(fullURL, &http.Client{Timeout: opts.Timeout})
	if len(opts.Headers) > 0 {
		headers := opts.Headers
		client = client.WithRequestModifier(func(r *http.Request) {
			for k, v := range headers {
				r.Header.Set(k, v)
			}
		})
	}
	return client
}

// Connect configures the GraphQL client and optionally verifies server reachability.
// If opts.Timeout <= 0, DefaultTimeout is used. If SkipConnectivityCheck is true, no
// connectivity query is sent; otherwise the connectivity query runs and Connect
// returns an error on failure.
func (g *GraphQLClient) Connect(opts ConnectionOptions) error {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	g.ConnectionOptions = opts

	fullURL, err := buildFullURL(opts.URL, 0)
	if err != nil {
		return fmt.Errorf("failed to build full URL: %w", err)
	}
	g.client = newGraphQLClient(fullURL, opts)

	return g.verifyConnectivity(opts.URL.Host)
}

// Reconnect tears down the current client and re-establishes it with the same
// options. If SkipConnectivityCheck is false, the connectivity query runs again.
func (g *GraphQLClient) Reconnect() error {
	if g.client == nil {
		return fmt.Errorf("GraphQL client is not initialized")
	}
	fullURL, err := buildFullURL(g.URL, 0)
	if err != nil {
		return fmt.Errorf("failed to build full URL: %w", err)
	}
	if g.Timeout <= 0 {
		g.Timeout = DefaultTimeout
	}
	g.client = newGraphQLClient(fullURL, g.ConnectionOptions)
	return g.verifyConnectivity(g.URL.Host)
}

// verifyConnectivity runs the connectivity query unless SkipConnectivityCheck is set.
func (g *GraphQLClient) verifyConnectivity(host string) error {
	if g.SkipConnectivityCheck {
		return nil
	}
	query := DefaultGraphQLConnectivityQuery
	if g.GraphQLConnectivityQuery != "" {
		query = g.GraphQLConnectivityQuery
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.Timeout)
	defer cancel()
	if _, err := g.client.ExecRaw(ctx, query, nil); err != nil {
		return fmt.Errorf("failed to connect to GraphQL server at %s: %w", host, err)
	}
	return nil
}

// Close clears the GraphQL client. It is not usable until Connect is called again.
func (g *GraphQLClient) Close() error {
	g.client = nil
	return nil
}
