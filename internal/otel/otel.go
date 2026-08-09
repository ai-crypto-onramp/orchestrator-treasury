// Package otel wires the process-wide OpenTelemetry tracer and meter
// providers. Init is a no-op when OTEL_EXPORTER_OTLP_ENDPOINT is unset, so
// tests never require a real collector.
package otel

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init configures the global OTel tracer + meter providers from
// OTEL_EXPORTER_OTLP_ENDPOINT and OTEL_SERVICE_NAME. When the endpoint is
// unset it installs no-op providers and returns a no-op shutdown. The
// returned shutdown func flushes and stops the providers and MUST be called
// on process exit.
func Init(serviceName string) (func(context.Context) error, error) {
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
		otel.SetMeterProvider(noop.NewMeterProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}
	if name := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME")); name != "" {
		serviceName = name
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "grpc://")

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	spanExp, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel trace exporter: %w", err)
	}
	metricExp, err := otlpmetricgrpc.New(context.Background(),
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel metric exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExp, sdktrace.WithBatchTimeout(10*time.Second)),
		sdktrace.WithResource(res),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp,
			sdkmetric.WithInterval(10*time.Second),
		)),
		sdkmetric.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		var traceErr, metricErr error
		if err := tp.Shutdown(ctx); err != nil {
			traceErr = fmt.Errorf("trace shutdown: %w", err)
		}
		if err := mp.Shutdown(ctx); err != nil {
			metricErr = fmt.Errorf("metric shutdown: %w", err)
		}
		if traceErr != nil {
			return traceErr
		}
		return metricErr
	}, nil
}
