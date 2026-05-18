// Package observability owns the boundary between collected storage facts and
// product-facing health signals.
//
// Collection adapters gather facts from providers, chain state, wallet-scoped
// data sets, local storage metadata, and task state. They should not decide how
// those facts affect operator attention beyond producing typed state rows.
//
// Interpretation belongs in this package. Status, reason codes, freshness,
// item signals, summary signals, and readiness/attention policies are computed
// here so admin APIs and UI surfaces do not duplicate health semantics.
//
// Presentation layers consume observations and policy outputs. They may choose
// labels, layout, and copy, but must not reimplement provider/data set status,
// freshness, or readiness rules.
package observability
