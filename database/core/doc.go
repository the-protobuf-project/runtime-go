// Package core is the code every store driver would otherwise copy.
//
// It sits between [store] and the backends: store defines the contract and
// deliberately depends on nothing but protobuf, so anything needing an id
// generator or a page token lives here instead. A driver imports core; core
// never imports a driver.
//
// Nothing here decides behavior. These are the mechanical parts — filling a
// generated key, stamping an audit timestamp, encoding an offset into a page
// token — that three backends had already written three times before this
// package existed, which is three chances to fill them in subtly differently
// and have a resource behave one way on Postgres and another on MongoDB.
package core
