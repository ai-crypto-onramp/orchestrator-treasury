.PHONY: build test lint cover cover-check run docker-build docker-run docker-up docker-down clean migrate-up migrate-down

build:
	go build -o bin/treasury ./cmd/treasury

test:
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=./internal/...

lint:
	golangci-lint run

cover: test
	go tool cover -func=coverage.out | tail -1

# cover-check runs the test suite (if no coverage.out exists) and fails if
# total coverage is below 80%. Uses portable awk to parse the percentage.
cover-check:
	@test -f coverage.out || $(MAKE) test
	@pct=`go tool cover -func=coverage.out | tail -1 | awk '{print $$NF}' | sed 's/%//'`; \
	echo "Coverage: $$pct%"; \
	awk -v p="$$pct" 'BEGIN { if (p+0 < 80) { printf "coverage %.1f%% is below 80%% threshold\n", p; exit 1 } }'

run:
	go run ./cmd/treasury

docker-build:
	docker build -t ai-crypto-onramp/orchestrator-treasury .

docker-run:
	docker run --rm -p 8080:8080 ai-crypto-onramp/orchestrator-treasury

docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

clean:
	rm -rf bin/ coverage.out

migrate-up:
	go run ./cmd/migrate --up

migrate-down:
	go run ./cmd/migrate --down