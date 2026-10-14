"""The temporary diagnostic cannot alter or leak the checkpoint request."""

import importlib.util
import json
from pathlib import Path

import boto3
import pytest
from botocore.config import Config


SPEC = importlib.util.spec_from_file_location(
    "chain_wire_observer",
    Path(__file__).resolve().parents[1] / "src/chain_wire_observer.py",
)
observer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(observer)


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "private-invalid-header",
        "Root=1-6ad0049a-2ee7ec7a5f0694681ab47ffa;Parent=1ae88917648ea5bc;Sampled=1",
    ],
)
def test_observer_readonly_and_allowlisted(header, capsys):
    client = boto3.client(
        "lambda",
        region_name="us-west-2",
        aws_access_key_id="local",
        aws_secret_access_key="local",
        endpoint_url="https://lambda.example.invalid",
        config=Config(retries={"max_attempts": 0}),
    )
    model = client.meta.service_model.operation_model("CheckpointDurableExecution")
    options = {} if header is None else {"XAmznTraceId": header}
    raw = json.dumps(
        {
            "CheckpointToken": "PRIVATE_CHECKPOINT_TOKEN",
            "ClientToken": "PRIVATE_CLIENT_TOKEN",
            "Updates": [
                {
                    "Id": "operation-id",
                    "Type": "CHAINED_INVOKE",
                    "Action": "START",
                    "Name": "otel-invoke",
                    "Payload": "PRIVATE_PAYLOAD",
                    "ChainedInvokeOptions": options,
                }
            ],
        }
    ).encode()
    params = {"body": raw, "headers": {"Authorization": "PRIVATE_AUTHORIZATION"}}
    try:
        assert observer.observe_request(model, params) is None
        assert params["body"] is raw and params["headers"] == {
            "Authorization": "PRIVATE_AUTHORIZATION"
        }
        output = capsys.readouterr().out
        assert "PRIVATE_" not in output and "private-invalid-header" not in output
        record = json.loads(output.removeprefix("DURABLE_CHAIN_WIRE "))
        assert record["operation_id"] == "operation-id" and record["model_has_field"]
        assert record["field_present"] is (header is not None)
        assert record["canonical"] is bool(header and header.startswith("Root="))
    finally:
        client.close()
