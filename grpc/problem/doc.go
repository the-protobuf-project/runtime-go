// Package problem turns a service's typed errors into google.rpc.Status
// errors a developer can act on (AIP-193): a status code, a message that says
// what is wrong and what to do, an ErrorInfo with a stable reason, and the
// detail the error calls for -- BadRequest for a field, PreconditionFailure for
// a blocked call, ResourceInfo for a missing resource.
//
// The vocabulary is the service's: it registers its sentinel errors, each
// with a status code and a default reason, on a [Translator], declares the
// reasons it may return, and returns [Problem] values (or errors wrapping its
// sentinels) from its handlers. Nothing here names a reason of its own.
//
//	var errs = problem.New("shop.example.com",
//	    problem.Kind{Err: ErrNotFound, Code: codes.NotFound, Reason: "NOT_FOUND"},
//	    problem.Kind{Err: ErrSoldOut, Code: codes.FailedPrecondition, Reason: "SOLD_OUT"},
//	)
//
//	return nil, errs.ToStatus(&problem.Problem{Kind: ErrSoldOut, Reason: "SHELF_EMPTY",
//	    ResourceName: shelf, What: "shelf " + shelf + " is empty", Fix: "Restock it first"})
package problem
