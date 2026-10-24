// Package hints turns an API's generic errors into ones a developer can act
// on, after the handler has run.
//
// A generated repository reports a missing, duplicate or concurrently changed
// record with a bare sentinel -- "not found", "already exists", "conflict" --
// because it cannot know what the caller was doing. The request can: it names
// the resource. So a bare error is rewritten to say which resource, of what
// kind, and what to do next -- the List call that shows what does exist, the
// Get that reads what is in the way -- with the gRPC method and its REST path,
// both read from the protos' google.api.resource and google.api.http
// annotations, so the hint is right on whichever surface the caller used.
//
// An Internal or Unknown error is a fault on the server's side, and its text
// -- a SQL error, a stack of wrapping -- helps nobody calling the API and says
// more about the database than it should. It is replaced by an apology that
// carries the trace id, which a developer can report and an operator can look
// up; the original is in that trace and the log.
//
// An error a service described itself, with an ErrorInfo reason not among
// [Options.GenericReasons], is left as it is.
package hints
