"""Unsupported wire subtypes must not consume checkpoint state or tokens."""

from threading import Event
from urllib.parse import quote

import pytest

from aws_durable_execution_sdk_python_testing.runner import DurableFunctionTestRunner
from aws_durable_execution_sdk_python_testing.stores.filesystem import (
    FileSystemExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.memory import (
    InMemoryExecutionStore,
)
from aws_durable_execution_sdk_python_testing.stores.sqlite import SQLiteExecutionStore
from aws_durable_execution_sdk_python_testing.web.handlers import (
    CheckpointDurableExecutionHandler,
)
from aws_durable_execution_sdk_python_testing.web.models import HTTPRequest
from aws_durable_execution_sdk_python_testing.web.routes import (
    CheckpointDurableExecutionRoute,
    Route,
)


@pytest.mark.parametrize("store_kind", ["memory", "filesystem", "sqlite"])
@pytest.mark.parametrize("label", ["Step", "Library_CustomStep"])
def test_checkpoint_subtype_validation_precedes_mutation(store_kind, label, tmp_path):
    if store_kind == "memory":
        store = InMemoryExecutionStore()
    elif store_kind == "filesystem":
        store = FileSystemExecutionStore.create(tmp_path / "store")
    else:
        store = SQLiteExecutionStore.create_and_initialize(str(tmp_path / "store.db"))

    ready = Event()
    release = Event()
    inputs = []

    def handler(event, context):
        inputs.append(event)
        ready.set()
        release.wait(10)
        return {"Status": "SUCCEEDED", "Result": '"done"'}

    runner = DurableFunctionTestRunner(
        handler=handler, store=store, skip_time=False, execution_timeout=20
    )
    try:
        arn = runner.run_async(input="{}")
        assert ready.wait(3), "The real invocation must start before checkpointing"
        token = inputs[0].checkpoint_token
        before = store.load(arn)
        before_sequence = before.token_sequence
        before_ids = [operation.operation_id for operation in before.operations]
        route = CheckpointDurableExecutionRoute.from_route(
            Route.from_string(
                "/2025-12-01/durable-executions/" + quote(arn, safe="") + "/checkpoint"
            )
        )
        # Wire the actual endpoint to the runner's actual executor; the runner
        # establishes the invocation gate, worker and checkpoint token itself.
        endpoint = CheckpointDurableExecutionHandler(runner._executor)  # noqa: SLF001
        response = endpoint.handle(
            route,
            HTTPRequest(
                method="POST",
                path=route,
                headers={},
                query_params={},
                body={
                    "CheckpointToken": token,
                    "Updates": [
                        {
                            "Id": "target",
                            "Type": "STEP",
                            "Action": "START",
                            "Name": "target",
                            "SubType": label,
                        }
                    ],
                },
            ),
        )
        after = store.load(arn)
        after_ids = [operation.operation_id for operation in after.operations]
        if label == "Step":
            assert response.status_code == 200
            assert after.token_sequence == before_sequence + 1
            assert after_ids == [*before_ids, "target"]
        else:
            assert response.status_code == 400
            assert "is not a valid OperationSubType" in str(response.body)
            assert after.token_sequence == before_sequence
            assert after_ids == before_ids

        retry = endpoint.handle(
            route,
            HTTPRequest(
                method="POST",
                path=route,
                headers={},
                query_params={},
                body={"CheckpointToken": token, "Updates": []},
            ),
        )
        assert retry.status_code == (400 if label == "Step" else 200)
    finally:
        runner.close()
        release.set()
