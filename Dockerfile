# syntax=docker/dockerfile:1.6

# --- builder ---
FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/treasury ./cmd/treasury

# --- runtime ---
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/treasury /treasury
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/treasury"]