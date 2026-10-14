"""Plugin hook metadata may come from enum- or string-based SDK subtypes."""

from datetime import UTC, datetime, timedelta

import opentelemetry.context as otel_context
import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
)
from aws_durable_execution_sdk_python.plugin import (
    InvocationEndInfo,
    InvocationStartInfo,
    InvocationStatus,
    OperationEndInfo,
    OperationStartInfo,
    OperationType,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from aws_durable_execution_sdk_python_otel import (
    ExecutionOtelPlugin,
    InvocationOtelPlugin,
    OtelPluginConfig,
)


@pytest.mark.parametrize("plugin_type", [ExecutionOtelPlugin, InvocationOtelPlugin])
@pytest.mark.parametrize(
    "sub_type,expected",
    [
        (OperationSubType.WAIT, "Wait"),
        ("Wait", "Wait"),
        ("Library_CustomWait", "Library_CustomWait"),
        (None, None),
    ],
)
def test_hook_exports_enum_or_string_subtype_without_changing_type(
    plugin_type, sub_type, expected
):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    plugin = plugin_type(
        OtelPluginConfig(
            tracer_provider=provider,
            context_extractor=lambda _: None,
            enrich_logger=False,
        )
    )
    before = otel_context.get_current()
    start = datetime.now(UTC)
    invocation = {
        "execution_arn": "arn:aws:lambda:us-west-2:123456789012:function:subtype:$LATEST",
        "request_id": "request",
        "execution_start_time": start,
        "is_first_invocation": True,
    }
    operation = {
        "operation_id": "target",
        "operation_type": OperationType.WAIT,
        "sub_type": sub_type,
        "name": "target",
        "parent_id": None,
        "start_time": start,
        "is_replayed": False,
    }
    try:
        plugin.on_invocation_start(InvocationStartInfo(**invocation))
        plugin.on_operation_start(
            OperationStartInfo(**operation, status=OperationStatus.STARTED)
        )
        plugin.on_operation_end(
            OperationEndInfo(
                **operation,
                status=OperationStatus.SUCCEEDED,
                end_time=start + timedelta(seconds=1),
            )
        )
        (span,) = [
            span for span in exporter.get_finished_spans() if span.name == "target"
        ]
        assert span.attributes["durable.operation.type"] == "WAIT"
        assert span.attributes["durable.operation.status"] == "SUCCEEDED"
        if expected is None:
            assert "durable.operation.subtype" not in span.attributes
        else:
            assert span.attributes["durable.operation.subtype"] == expected
    finally:
        plugin.on_invocation_end(
            InvocationEndInfo(**invocation, status=InvocationStatus.SUCCEEDED)
        )
        provider.shutdown()
        assert otel_context.get_current() == before
