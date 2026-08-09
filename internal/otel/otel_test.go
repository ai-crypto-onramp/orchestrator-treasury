package otel

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestInit_NoEndpointInstallsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	shutdown, err := Init("treasury")
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// shutdownOK calls the shutdown func ignoring the flush/upload error that
// arises when the OTLP exporter points at a listener that is not a real
// gRPC collector. Init itself must succeed; only the final flush can fail
// because the periodic reader tries to export pending metrics on shutdown.
func shutdownOK(t *testing.T, shutdown func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

func TestInit_EndpointWithServiceNameEnv(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ln.Addr().String())
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	shutdown, err := Init("default-name")
	if err != nil {
		t.Fatalf("init with endpoint: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown")
	}
	shutdownOK(t, shutdown)
}

func TestInit_EndpointWithSchemePrefix(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+ln.Addr().String())
	t.Setenv("OTEL_SERVICE_NAME", "")
	shutdown, err := Init("treasury")
	if err != nil {
		t.Fatalf("init with http:// prefix: %v", err)
	}
	shutdownOK(t, shutdown)
}

func TestInit_EndpointGrpcPrefix(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "grpc://"+ln.Addr().String())
	t.Setenv("OTEL_SERVICE_NAME", "")
	shutdown, err := Init("treasury")
	if err != nil {
		t.Fatalf("init with grpc:// prefix: %v", err)
	}
	shutdownOK(t, shutdown)
}