package core

import "github.com/the-protobuf-project/runtime-go/streams"

// Outgoing returns the headers a publish carries: the stream's defaults with
// the publish's own laid on top, validated, so an invalid header fails at the
// call that set it. Nil when there are none.
func Outgoing(s streams.Stream, o streams.Options) (streams.Headers, error) {
	headers := s.Headers.Merged(o.Headers)
	if err := headers.Validate(); err != nil {
		return nil, err
	}
	return headers, nil
}
