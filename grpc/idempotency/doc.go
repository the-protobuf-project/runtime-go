// Package idempotency makes an API's request_id field mean what AIP-155
// promises: reusing an id returns what the first call returned, instead of
// attempting the write a second time.
//
// It is one unary interceptor rather than per-handler logic, because the rule
// is identical for every RPC. The interceptor finds the field by reflection,
// so an RPC opts in simply by declaring `string request_id` -- there is
// nothing to register, and RPCs without the field are untouched. A request
// with validate_only set is a dry run that writes nothing and is not recorded:
// recording it would burn the caller's request_id on a request that never
// happened.
//
// The key is claimed before the handler runs and settled after it returns:
//
//	claim (create IN_FLIGHT)  ->  handler  ->  settle (DONE + response)
//
// The claim is what makes this safe under concurrency. A key's name is a
// digest of (method, request_id), so two callers replaying the same id race
// on the store's unique name and exactly one wins the create. The loser reads
// the winner's key: DONE is a replay and gets the first response verbatim;
// IN_FLIGHT means the original call is still running, a concurrent duplicate
// that cannot be answered yet (ABORTED, reason REQUEST_IN_FLIGHT). A claim
// older than [Options.StaleAfter] was abandoned by a process that died before
// settling; it is cleared (ABORTED, reason REQUEST_ABANDONED) so the retry
// runs once. A handler that fails deletes its key, so a retry after a real
// failure is free to proceed.
package idempotency
