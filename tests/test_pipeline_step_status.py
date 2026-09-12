import json

from pipelines_mcp.pipeline_step_status import parse_step_status

_REQUEST_ID = "req"
_ARGUMENTS = '{ "batchtime": "202608", "client": "isaca" }'
_START = {
    "level": "info",
    "@timestamp": "2026-09-11T03:11:01.402652973Z",
    "caller": "k8s/client.go:414",
    "message": "[k8s-client] starting pipeline",
    "request-id": _REQUEST_ID,
    "job-name": "pipelines-req-0-0",
    "image-name": "pipelines-rust:v1.1.1",
    "container-name": "pipelines-rust",
    "namespace": "pipelines-prd",
    "arguments": _ARGUMENTS,
    "pipeline-id": _REQUEST_ID,
}


def test_parse_step_status_returns_running_when_started() -> None:
    noise = json.dumps(
        {"message": "pipeline request queued", "pipeline-id": _REQUEST_ID}
    )
    parsed = parse_step_status(_REQUEST_ID, (noise, json.dumps(_START)))
    assert parsed.request_id == _REQUEST_ID
    assert len(parsed.steps) == 1
    step = parsed.steps[0]
    assert step.step_index == 0
    assert step.replica == 0
    assert step.status == "running"
    assert step.job_name == "pipelines-req-0-0"


def test_parse_step_status_returns_error_from_index_line() -> None:
    line = json.dumps(
        {
            "message": "Unable to send slack message, logging instead",
            "detail": "Index: 0, replica: 0, status: error ",
        }
    )
    parsed = parse_step_status(_REQUEST_ID, (line,))
    assert len(parsed.steps) == 1
    step = parsed.steps[0]
    assert step.step_index == 0
    assert step.replica == 0
    assert step.status == "error"
    assert step.job_name == "pipelines-req-0-0"


def test_parse_step_status_returns_complete_from_index_line() -> None:
    line = "Index: 1, replica: 2, status: complete "
    parsed = parse_step_status(_REQUEST_ID, (line,))
    assert len(parsed.steps) == 1
    step = parsed.steps[0]
    assert step.step_index == 1
    assert step.replica == 2
    assert step.status == "complete"
    assert step.job_name == "pipelines-req-1-2"


def test_parse_step_status_returns_empty_when_missing() -> None:
    noise = json.dumps({"message": "pipeline request queued"})
    parsed = parse_step_status(_REQUEST_ID, (noise,))
    assert parsed.request_id == _REQUEST_ID
    assert parsed.steps == ()


def test_parse_step_status_keeps_pipeline_order() -> None:
    lines = (
        "Index: 1, replica: 0, status: complete ",
        "Index: 0, replica: 1, status: error ",
        "Index: 0, replica: 0, status: complete ",
    )
    parsed = parse_step_status(_REQUEST_ID, lines)
    keys = tuple((step.step_index, step.replica) for step in parsed.steps)
    assert keys == ((0, 0), (0, 1), (1, 0))


def test_parse_step_status_index_overrides_start() -> None:
    lines = (
        json.dumps(_START),
        "Index: 0, replica: 0, status: error ",
    )
    parsed = parse_step_status(_REQUEST_ID, lines)
    assert len(parsed.steps) == 1
    step = parsed.steps[0]
    assert step.status == "error"
    assert step.job_name == "pipelines-req-0-0"
