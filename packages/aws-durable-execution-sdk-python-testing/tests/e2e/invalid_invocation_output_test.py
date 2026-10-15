"""End-to-end handling of invalid invocation outputs through the test runner."""

from typing import Any

import pytest
from aws_durable_execution_sdk_python.execution import InvocationStatus
from aws_durable_execution_sdk_python.lambda_service import ErrorObject

from aws_durable_execution_sdk_python_testing.execution import ExecutionStatus
from aws_durable_execution_sdk_python_testing.runner import (
    DurableFunctionTestResult,
    DurableFunctionTestRunner,
)


@pytest.mark.parametrize(
    ("output", "message"),
    [
        pytest.param(
            {"Status": "FAILED", "Result": "result"},
            "Cannot provide a Result for FAILED status.",
            id="failed-with-result",
        ),
        pytest.param(
            {"Status": "SUCCEEDED", "Error": {"ErrorMessage": "boom"}},
            "Cannot provide an Error for SUCCEEDED status.",
            id="succeeded-with-error",
        ),
        pytest.param(
            {"Result": "result"},
            "Invalid invocation output: None is not a valid InvocationStatus",
            id="missing-status",
        ),
        pytest.param(
            {"Status": "DONE"},
            "Invalid invocation output: 'DONE' is not a valid InvocationStatus",
            id="unknown-status",
        ),
        pytest.param(
            {"Status": "RETRY"},
            "Unexpected invocation status: InvocationStatus.RETRY",
            id="internal-status",
        ),
        pytest.param(
            ["SUCCEEDED"],
            "Invalid invocation output: 'list' object has no attribute 'get'",
            id="not-an-object",
        ),
    ],
)
def test_invalid_output_fails_the_execution_after_one_invocation(
    output: Any, message: str
) -> None:
    """The service fails the execution on the first invalid output, with an
    InvalidParameterValueException error, and does not invoke again."""
    invocations: list[Any] = []

    def handler(event: Any, context: Any) -> Any:  # noqa: ARG001
        invocations.append(event)
        return output

    with DurableFunctionTestRunner(handler=handler, execution_timeout=10) as runner:
        execution_arn = runner.run_async(input="input str")
        result: DurableFunctionTestResult = runner.wait_for_result(
            execution_arn, timeout=10
        )

    assert len(invocations) == 1
    assert result.status is InvocationStatus.FAILED
    assert result.execution_status is ExecutionStatus.FAILED
    assert result.error == ErrorObject(
        message=message,
        type="InvalidParameterValueException",
        data=None,
        stack_trace=None,
    )
