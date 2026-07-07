// GraphQL subscription support over WebSocket (graphql-ws protocol).
package network

import (
	"context"
	"fmt"
	"net/http"
	"reflect"

	graphql "github.com/hasura/go-graphql-client"
)

// Subscription is a live GraphQL subscription. Updates streams one GraphQLResult per
// server message (Response holds a freshly decoded copy of the subscription struct,
// or Error is set). Stop ends the subscription and closes Updates.
type Subscription struct {
	updates chan GraphQLResult
	client  *graphql.SubscriptionClient
}

// Updates returns the channel of subscription results. It is closed when the
// subscription stops (via Stop, a fatal error, or server completion).
func (s *Subscription) Updates() <-chan GraphQLResult { return s.updates }

// Stop ends the subscription and releases the underlying WebSocket connection.
func (s *Subscription) Stop() error { return s.client.Close() }

// SubscribeFields opens a subscription selecting result under field with the given
// arguments, declaring ONLY the arguments present (so nil optionals are omitted, not
// sent as null). result must be a pointer to the typed selection; each server message
// is decoded into a fresh value of that type and delivered (as Response) on the
// returned Subscription's Updates channel. Headers are sent on the WebSocket handshake.
// Cancelling ctx stops the subscription (equivalent to calling Stop); Stop still works
// independently.
func (g *GraphQLClient) SubscribeFields(ctx context.Context, field string, result any, args map[string]interface{}) (*Subscription, error) {
	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("result must be a pointer")
	}
	resultType := rv.Type().Elem()

	fullURL, err := buildFullURL(websocketURL(g.URL), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to build subscription URL: %w", err)
	}

	// Use the modern graphql-transport-ws protocol (go-graphql-client's GraphQLWS),
	// which most current engines speak; the library otherwise defaults to the legacy
	// subscriptions-transport-ws protocol.
	subClient := graphql.NewSubscriptionClient(fullURL).WithProtocol(graphql.GraphQLWS)
	if len(g.Headers) > 0 {
		header := http.Header{}
		for k, v := range g.Headers {
			header.Set(k, v)
		}
		subClient = subClient.WithWebSocketOptions(graphql.WebsocketOptions{HTTPHeader: header})
	}

	sub := &Subscription{updates: make(chan GraphQLResult, 1), client: subClient}
	wrapper := newOpStruct(field, resultType, args)

	if _, err := subClient.Subscribe(wrapper.Interface(), args, sub.handler(wrapper.Elem().Type())); err != nil {
		_ = subClient.Close()
		return nil, fmt.Errorf("failed to start subscription: %w", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(sub.updates)
		defer close(done)
		if runErr := subClient.Run(); runErr != nil {
			sub.updates <- GraphQLResult{Error: fmt.Errorf("subscription stopped: %w", runErr)}
		}
	}()

	// Stop the subscription when ctx is cancelled. The watcher also exits when the
	// subscription ends on its own (done closed), so it never outlives the stream.
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = subClient.Close()
			case <-done:
			}
		}()
	}

	return sub, nil
}

// handler decodes each payload into a fresh wrapper of wrapperType and forwards the
// selected field value (or the error) onto the updates channel.
func (s *Subscription) handler(wrapperType reflect.Type) func([]byte, error) error {
	return func(message []byte, err error) error {
		if err != nil {
			s.updates <- GraphQLResult{Error: err}
			return nil
		}
		out := reflect.New(wrapperType)
		if decodeErr := graphql.UnmarshalGraphQL(message, out.Interface()); decodeErr != nil {
			s.updates <- GraphQLResult{Error: fmt.Errorf("failed to decode subscription message: %w", decodeErr)}
			return nil
		}
		// Deliver a pointer to the inner "Result" field (the typed selection).
		s.updates <- GraphQLResult{Response: out.Elem().Field(0).Addr().Interface()}
		return nil
	}
}
