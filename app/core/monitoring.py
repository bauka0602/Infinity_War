from __future__ import annotations

import os

try:
    import sentry_sdk
except ImportError:  # pragma: no cover - optional dependency guard
    sentry_sdk = None

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
except ImportError:  # pragma: no cover - optional dependency guard
    trace = None
    OTLPSpanExporter = None
    FastAPIInstrumentor = None
    Resource = None
    TracerProvider = None
    BatchSpanProcessor = None

_OTEL_INITIALIZED = False


def init_monitoring():
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if dsn and sentry_sdk is not None:
        sentry_sdk.init(
            dsn=dsn,
            environment=os.getenv("SENTRY_ENVIRONMENT", os.getenv("ENVIRONMENT", "local")),
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
            send_default_pii=False,
        )

    init_tracing()


def init_tracing():
    global _OTEL_INITIALIZED
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if (
        _OTEL_INITIALIZED
        or not endpoint
        or trace is None
        or TracerProvider is None
        or OTLPSpanExporter is None
    ):
        return

    resource = Resource.create(
        {
            "service.name": os.getenv("OTEL_SERVICE_NAME", "timetableg-backend"),
            "deployment.environment": os.getenv("ENVIRONMENT", "local"),
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    )
    trace.set_tracer_provider(provider)
    _OTEL_INITIALIZED = True


def instrument_fastapi(app):
    if FastAPIInstrumentor is None:
        return
    if not os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip():
        return
    FastAPIInstrumentor.instrument_app(app)


def capture_exception(error, **context):
    if sentry_sdk is None:
        return

    if context:
        with sentry_sdk.push_scope() as scope:
            for key, value in context.items():
                scope.set_extra(key, value)
            sentry_sdk.capture_exception(error)
        return

    sentry_sdk.capture_exception(error)
