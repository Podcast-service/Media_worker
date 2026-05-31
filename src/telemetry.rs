use std::time::Duration;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::logs::LoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider;
use opentelemetry_sdk::{runtime, Resource};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

const DEFAULT_ENDPOINT: &str = "http://otel-collector:4317";
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
const METRIC_INTERVAL: Duration = Duration::from_secs(30);

pub struct TelemetryGuard {
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    logger_provider: LoggerProvider,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        let _ = self.tracer_provider.shutdown();
        let _ = self.meter_provider.shutdown();
        let _ = self.logger_provider.shutdown();
    }
}

fn endpoint() -> String {
    std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string())
}

fn build_resource(default_service_name: &str) -> Resource {
    let service_name = std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_service_name.to_string());
    let environment =
        std::env::var("DEPLOYMENT_ENV").unwrap_or_else(|_| "dev".to_string());

    Resource::new(vec![
        KeyValue::new("service.name", service_name),
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("deployment.environment", environment),
    ])
}

pub fn init(default_service_name: &str) -> TelemetryGuard {
    let endpoint = endpoint();
    let resource = build_resource(default_service_name);

    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // ---- Traces ------------------------------------------------------------
    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .expect("failed to build OTLP span exporter");
    let tracer_provider = TracerProvider::builder()
        .with_batch_exporter(span_exporter, runtime::Tokio)
        .with_resource(resource.clone())
        .build();
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // ---- Metrics -----------------------------------------------------------
    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .expect("failed to build OTLP metric exporter");
    let reader = PeriodicReader::builder(metric_exporter, runtime::Tokio)
        .with_interval(METRIC_INTERVAL)
        .build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource.clone())
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // ---- Logs --------------------------------------------------------------
    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .expect("failed to build OTLP log exporter");
    let logger_provider = LoggerProvider::builder()
        .with_batch_exporter(log_exporter, runtime::Tokio)
        .with_resource(resource)
        .build();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let tracer = tracer_provider.tracer("tracing-otel");
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    TelemetryGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
    }
}
