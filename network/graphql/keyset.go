package graphql

// Keyset (cursor) pagination. Offset/limit paging is unstable under concurrent inserts: a row
// added before the cursor shifts every later row by one, so the next page repeats or skips
// rows. Keyset paging instead orders by a column and asks for rows strictly after the last one
// seen, which is stable because the cursor is a value in the data, not a position.

// After restricts a list to rows that sort strictly after last for the order term: `col _gt
// last` when term is ascending, `col _lt last` when descending. last is the order column's
// value from the last row of the previous page (the cursor). The order column should be unique
// (e.g. an id or a strictly-increasing timestamp); a non-unique column can skip or repeat rows
// at page boundaries.
func After(term OrderTerm, last any) Predicate {
	op := "_gt"
	if term.dir == Desc {
		op = "_lt"
	}
	return pred(term.col, op, last)
}

// KeysetAfter turns a ListRequest into the next keyset page: it orders by term and keeps only
// rows after last (see After), composing with any predicate already set via Where. Pair it with
// Limit for the page size; the cursor for the following page is the term column's value from the
// last row returned.
//
//	page, _ := svc.Query.Booking.Resource.List(ctx,
//	    resource.List().KeysetAfter(resource.CreateTime.Asc(), lastCreateTime).Limit(50))
func (r *ListRequest) KeysetAfter(term OrderTerm, last any) *ListRequest {
	r.orderBy = append(r.orderBy, term)
	ks := After(term, last)
	if r.where.node == nil {
		r.where = ks
	} else {
		r.where = And(r.where, ks)
	}
	return r
}
