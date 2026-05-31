//! Application metrics exported to the OTEL collector via the global meter
//! (configured in [`crate::telemetry`]). Instruments are created lazily on
//! first use.
use std::sync::OnceLock;

use opentelemetry::metrics::Counter;
use opentelemetry::{global, KeyValue};

pub struct Metrics {
    messages_received: Counter<u64>,
    messages_processed: Counter<u64>,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Returns the process-wide metrics, initialising them on first call.
pub fn metrics() -> &'static Metrics {
    METRICS.get_or_init(|| {
        let meter = global::meter("media_worker");
        Metrics {
            messages_received: meter
                .u64_counter("media.messages.received")
                .with_description("Media events received from Kafka")
                .build(),
            messages_processed: meter
                .u64_counter("media.messages.processed")
                .with_description("Media events processed")
                .build(),
        }
    })
}

impl Metrics {
    /// One Kafka message pulled from the topic (before validation).
    pub fn record_received(&self) {
        self.messages_received.add(1, &[]);
    }

    /// One media event handled, labelled by outcome.
    pub fn record_processed(&self, status: &'static str) {
        self.messages_processed
            .add(1, &[KeyValue::new("status", status)]);
    }
}
