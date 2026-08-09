// Package clients defines small Go interfaces and implementations for the
// downstream internal services Treasury Orchestration depends on:
// liquidity-routing, fx-hedging, wallet-management, ledger-accounting,
// and audit-event-log.
//
// Each downstream service is represented by an interface so tests can
// substitute fakes (in this package) without standing up the real
// services. Real HTTP clients are provided using net/http and are used
// only when the corresponding *_URL env var is set.
package clients

// This file only hosts the package doc; implementations live in their
// sibling files within this package.