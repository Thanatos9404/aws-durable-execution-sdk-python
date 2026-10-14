"""Read subtype labels through the public cloud-history result API."""

from datetime import UTC, datetime

import pytest
from aws_durable_execution_sdk_python.lambda_service import (
    OperationStatus,
    OperationSubType,
    OperationType,
)

from aws_durable_execution_sdk_python_testing.model import (
    GetDurableExecutionHistoryResponse,
    GetDurableExecutionResponse,
)
from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestResult


@pytest.mark.parametrize("label", ["Step", None])
def test_history_result_preserves_declared_enum_contract(label):
    events = []
    for index, (name, sub_type) in enumerate(
        [("known", "Step"), ("target", label)], start=1
    ):
        event = {
            "EventType": "StepSucceeded",
            "EventId": index,
            "EventTimestamp": datetime(2024, 1, 1, tzinfo=UTC),
            "Id": name,
            "Name": name,
            "StepSucceededDetails": {
                "Result": {"Payload": '"saved"', "Truncated": False}
            },
        }
        if sub_type is not None:
            event["SubType"] = sub_type
        events.append(event)
    history = GetDurableExecutionHistoryResponse.from_dict({"Events": events})
    # Exercise the history DTO round trip as well as the public result factory.
    history = GetDurableExecutionHistoryResponse.from_dict(history.to_dict())
    result = DurableFunctionTestResult.from_execution_history(
        GetDurableExecutionResponse(
            durable_execution_arn="execution",
            durable_execution_name="execution",
            function_arn="function",
            status="SUCCEEDED",
            start_timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        ),
        history,
    )
    assert len(result.operations) == 2
    assert result.get_step("known").sub_type is OperationSubType.STEP
    target = result.get_step("target")
    assert target.operation_type is OperationType.STEP
    assert target.status is OperationStatus.SUCCEEDED
    assert target.result == '"saved"'
    if label == "Step":
        assert target.sub_type is OperationSubType.STEP
    elif label is None:
        assert target.sub_type is None
