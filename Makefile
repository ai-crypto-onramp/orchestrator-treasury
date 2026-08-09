.PHONY: build test lint cover run docker-build docker-run docker-up docker-down clean migrate-up migrate-down

build:
	go build -o bin/treasury ./cmd/treasury

test:
	go test ./internal/... -race -coverprofile=coverage.out -coverpkg=./internal/...

lint:
	golangci-lint run

cover: test
	go tool cover -func=coverage.out | tail -1

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