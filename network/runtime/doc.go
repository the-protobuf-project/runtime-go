// Package runtime is the stable, single-import facade that generated GraphQL clients
// depend on. It re-exports the essentials of the underlying network package so
// generated code references one branded package (this one) instead of reaching into
// transport internals.
//
// Generated code typically does:
//
//	conn, _ := runtime.NewConnection(runtime.GraphQLConnClient)
//	conn.WithOpts(runtime.ConnectionOptions{URL: runtime.URLOptions{
//	    Scheme: runtime.HTTP, Host: "localhost:3280", Paths: []string{"/graphql"},
//	}})
//	gql, _ := conn.AsGraphQLConnectionType()
package runtime
