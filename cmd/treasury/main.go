// Command treasury is the entrypoint for the Treasury Orchestration
// service. It loads configuration, builds the wired server, and runs it.
//
// Run with `go run ./cmd/treasury` (local dev) or `make run`. See
// README.md for the full configuration surface.
package main

import (
	"context"
	"log"

	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/app"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/config"
	"github.com/ai-crypto-onramp/orchestrator-treasury/internal/otel"
)

func main() {
	shutdown, err := otel.Init("treasury-orchestration")
	if err != nil {
		log.Fatalf("otel init: %v", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	cfg := config.Load()
	srv, err := app.Build(cfg)
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	if err := srv.Run(); err != nil {
		log.Fatalf("run: %v", err)
	}
}