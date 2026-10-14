"""Temporary case11 diagnosis: read-only, allowlisted botocore wire observation.

This module is not an SDK feature or a conformance oracle. Remove it after the
outgoing carrier has been compared with the real child service TraceHeader.
"""

import hashlib
import json
import re
from typing import Any

from aws_durable_execution_sdk_python.lambda_service import LambdaClient


_CANONICAL = re.compile(
    r"Root=(1-[0-9a-f]{8}-[0-9a-f]{24});Parent=([0-9a-f]{16});Sampled=([01])"
)
_EVENT = "before-call.lambda.CheckpointDurableExecution"
_ID = "case11-readonly-wire-observer"


def _emit(record: dict[str, Any]) -> None:
    try:
        print("DURABLE_CHAIN_WIRE " + json.dumps(record, sort_keys=True), flush=True)
    except Exception:
        pass  # Diagnostic output must not affect checkpoint delivery.


def observe_request(model: Any, params: dict[str, Any], **_kwargs: Any) -> None:
    """Read the serialized body without changing the request or returning a response."""
    try:
        request = json.loads(params["body"])
        for update in request.get("Updates", []):
            if not (
                update.get("Type") == "CHAINED_INVOKE"
                and update.get("Action") == "START"
                and update.get("Name") == "otel-invoke"
            ):
                continue
            options = update.get("ChainedInvokeOptions", {})
            header = options.get("XAmznTraceId")
            match = _CANONICAL.fullmatch(header) if isinstance(header, str) else None
            record = {
                "event": "serialized_invoke_start",
                "operation_id": update.get("Id"),
                "field_present": "XAmznTraceId" in options,
                "canonical": match is not None,
                "model_has_field": "XAmznTraceId"
                in model.input_shape.members["Updates"]
                .member.members["ChainedInvokeOptions"]
                .members,
            }
            if match is not None:
                record.update(Root=match[1], Parent=match[2], Sampled=match[3])
            elif isinstance(header, str):
                # A malformed value is never printed, even if it contains data
                # that is not a trace context.
                record["noncanonical_header_sha256"] = hashlib.sha256(
                    header.encode()
                ).hexdigest()
            _emit(record)
    except Exception as error:
        # A diagnostic cannot change checkpoint behavior or print an exception
        # message that might contain request data.
        _emit({"event": "observer_error", "error_type": type(error).__name__})


def install_checkpoint_observer() -> None:
    """Observe the SDK's already-created client; never create/replace a client."""
    try:
        client = LambdaClient._cached_boto_client
        if client is None:
            _emit({"event": "observer_unavailable", "reason": "no_cached_sdk_client"})
            return
        client.meta.events.register(_EVENT, observe_request, unique_id=_ID)
    except Exception as error:
        _emit({"event": "observer_install_error", "error_type": type(error).__name__})
