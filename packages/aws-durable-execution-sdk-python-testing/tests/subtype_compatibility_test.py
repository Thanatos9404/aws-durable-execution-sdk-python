"""Read string metadata without widening current checkpoint/result contracts."""

import datetime

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    Operation,
    OperationStatus,
    OperationSubType,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.model import (
    CheckpointDurableExecutionRequest,
    Event,
    EventCreationContext,
    StartDurableExecutionInput,
    events_to_operations,
)
from aws_durable_execution_sdk_python_testing.exceptions import (
    InvalidParameterValueException,
)


@pytest.mark.parametrize(
    "sub_type,expected",
    [
        (OperationSubType.STEP, "Step"),
        ("Step", "Step"),
        ("Library_CustomStep", "Library_CustomStep"),
        (None, None),
        ("", None),
    ],
)
def test_history_projection_keeps_enum_or_plain_string_subtype(sub_type, expected):
    operation = Operation(
        operation_id="operation",
        operation_type=OperationType.STEP,
        status=OperationStatus.STARTED,
        sub_type=sub_type,
        start_timestamp=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
    )
    context = EventCreationContext(
        operation=operation,
        event_id=1,
        durable_execution_arn="execution",
        start_durable_execution_input=StartDurableExecutionInput(
            account_id="123456789012",
            function_name="function",
            function_qualifier="$LATEST",
            execution_name="execution",
            execution_timeout_seconds=30,
            execution_retention_period_days=1,
        ),
    )
    event = Event.create_event_started(context)
    if expected is None:
        assert "SubType" not in event.to_dict()
    else:
        assert event.to_dict()["SubType"] == expected
    assert Event.from_dict(event.to_dict()).sub_type == expected
    if expected == "Library_CustomStep":
        with pytest.raises(InvalidParameterValueException):
            events_to_operations([event])
        return
    (restored,) = events_to_operations([event])
    assert restored.operation_type is OperationType.STEP
    assert getattr(restored.sub_type, "value", restored.sub_type) == expected


@pytest.mark.parametrize("sub_type", ["Step", None, ""])
def test_checkpoint_request_preserves_existing_enum_contract(sub_type):
    request = CheckpointDurableExecutionRequest.from_dict(
        {
            "CheckpointToken": "token",
            "Updates": [
                {
                    "Id": "operation",
                    "Type": "STEP",
                    "Action": "START",
                    "SubType": sub_type,
                }
            ],
        },
        durable_execution_arn="execution",
    )
    assert request.updates is not None and len(request.updates) == 1
    update = request.updates[0]
    assert update.operation_type is OperationType.STEP
    if sub_type == "Step":
        assert update.sub_type is OperationSubType.STEP
    elif not sub_type:
        assert update.sub_type is None


@pytest.mark.parametrize("sub_type", ["Library_CustomStep", 42, True])
def test_checkpoint_request_rejects_unsupported_subtype(sub_type):
    with pytest.raises(ValueError, match="not a valid OperationSubType"):
        CheckpointDurableExecutionRequest.from_dict(
            {
                "CheckpointToken": "token",
                "Updates": [
                    {
                        "Id": "operation",
                        "Type": "STEP",
                        "Action": "START",
                        "SubType": sub_type,
                    }
                ],
            },
            durable_execution_arn="execution",
        )
