package telemetry

// Outcome classifies how an operation ended. Its labels carry no caller text,
// so they are safe as metric dimensions. It matches runtime-rs's
// telemetry::Outcome value for value.
type Outcome int

const (
	// OutcomeOK completed successfully. It is the zero value.
	OutcomeOK Outcome = iota
	// OutcomeNotFound found no such record.
	OutcomeNotFound
	// OutcomeConflict hit a uniqueness or precondition conflict.
	OutcomeConflict
	// OutcomeUnsupported asked for a capability the backend lacks.
	OutcomeUnsupported
	// OutcomeInvalid was given invalid input.
	OutcomeInvalid
	// OutcomeTimeout exceeded its deadline.
	OutcomeTimeout
	// OutcomeCancelled was abandoned before it completed.
	OutcomeCancelled
	// OutcomePanic unwound through a panic.
	OutcomePanic
	// OutcomeClosed was refused by a closed component.
	OutcomeClosed
	// OutcomeError failed in the backend, the transport or decoding.
	OutcomeError
)

// String is the stable label logs, metrics and spans all use.
func (o Outcome) String() string {
	switch o {
	case OutcomeOK:
		return "ok"
	case OutcomeNotFound:
		return "not_found"
	case OutcomeConflict:
		return "conflict"
	case OutcomeUnsupported:
		return "unsupported"
	case OutcomeInvalid:
		return "invalid"
	case OutcomeTimeout:
		return "timeout"
	case OutcomeCancelled:
		return "cancelled"
	case OutcomePanic:
		return "panic"
	case OutcomeClosed:
		return "closed"
	default:
		return "error"
	}
}

// IsError reports whether the outcome marks a span failed. An expected absence,
// a conflict or a cancellation does not.
func (o Outcome) IsError() bool {
	switch o {
	case OutcomeInvalid, OutcomeTimeout, OutcomePanic, OutcomeClosed, OutcomeError, OutcomeUnsupported:
		return true
	default:
		return false
	}
}
