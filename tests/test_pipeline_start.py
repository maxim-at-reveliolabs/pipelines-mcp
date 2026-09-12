import json

import pytest

from pipelines_mcp.errors import DomainError
from pipelines_mcp.pipeline_start import parse_start

_ARGUMENTS: str = '{ "batchtime": "202608", "client": "isaca" }'
_START: dict[str, str] = {
    "level": "info",
    "@timestamp": "2026-09-11T03:11:01.402652973Z",
    "caller": "k8s/client.go:414",
    "message": "[k8s-client] starting pipeline",
    "request-id": "b80cfed1-b9f6-44dd-a0ac-1ecb714c8805",
    "job-name": "pipelines-req-0-0",
    "image-name": "pipelines-rust:v1.1.1",
    "container-name": "pipelines-rust",
    "namespace": "pipelines-prd",
    "arguments": _ARGUMENTS,
    "pipeline-id": "req",
}


def test_parse_start_returns_keys_and_leaves_arguments_intact() -> None:
    noise = json.dumps({"message": "pipeline request queued", "pipeline-id": "req"})
    line = json.dumps(_START)
    start = parse_start((noise, line))
    assert start.timestamp == "2026-09-11T03:11:01.402652973Z"
    assert start.job_name == "pipelines-req-0-0"
    assert start.image_name == "pipelines-rust:v1.1.1"
    assert start.container_name == "pipelines-rust"
    assert start.namespace == "pipelines-prd"
    assert start.pipeline_id == "req"
    assert start.arguments == _ARGUMENTS


def test_parse_start_raises_when_line_is_missing() -> None:
    noise = json.dumps({"message": "pipeline request queued"})
    with pytest.raises(DomainError, match="No starting pipeline line"):
        _ = parse_start((noise,))


@pytest.mark.parametrize(
    "line",
    [
        '{"message":"[k8s-client] starting pipeline"',
        json.dumps({**_START, "arguments": {"batchtime": "202608"}}),
    ],
)
def test_parse_start_raises_when_start_line_is_not_valid(line: str) -> None:
    with pytest.raises(DomainError, match="not valid"):
        _ = parse_start((line,))
