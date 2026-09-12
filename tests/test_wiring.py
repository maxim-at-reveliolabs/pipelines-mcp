import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final, NoReturn

import httpx2
import pytest
from kubernetes.client import (
    V1Job,
    V1JobList,
    V1JobSpec,
    V1ObjectMeta,
    V1PodList,
    V1PodTemplateSpec,
)
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, InputRequiredResult
from pydantic import SecretStr, TypeAdapter

from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.logs import JsonValue, create_async_client
from pipelines_mcp.models import PipelineStatus, PipelineStepStatus
from pipelines_mcp.server import mcp
from pipelines_mcp.server_app import (
    set_k8s_factory,
    set_logs_factory,
    set_object_store_factory,
    set_reauth,
    set_status_store_factory,
)
from pipelines_mcp.settings import EsAuth

pytestmark = pytest.mark.anyio

_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])


@dataclass(frozen=True, slots=True)
class FakeBatch:
    jobs: dict[str, V1Job]

    def read_namespaced_job(self, name: str, namespace: str) -> V1Job:
        _ = namespace
        job = self.jobs.get(name)
        if job is None:
            raise K8sApiError(status=404)
        return job

    def list_namespaced_job(self, namespace: str) -> V1JobList:
        _ = namespace
        return V1JobList(items=[])


@dataclass(frozen=True, slots=True)
class FakeCore:
    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> V1PodList:
        _ = namespace
        _ = label_selector
        return V1PodList(items=[])

    def read_namespaced_pod(self, name: str, namespace: str) -> NoReturn:
        _ = name
        _ = namespace
        raise K8sApiError(status=404)


@dataclass(frozen=True, slots=True)
class Recorder:
    bodies: list[dict[str, JsonValue]]


def _tool_json(result: CallToolResult | InputRequiredResult) -> JsonValue:
    match result:
        case CallToolResult() as call:
            dumped = _JSON_OBJECT.validate_json(call.model_dump_json())
        case InputRequiredResult():
            pytest.fail("tool asked for input")
    structured = dumped.get("structured_content")
    if isinstance(structured, dict) and "result" in structured:
        return structured["result"]
    if structured is not None:
        return structured
    content = dumped["content"]
    assert isinstance(content, list)
    first = content[0]
    assert isinstance(first, dict)
    text = first["text"]
    assert isinstance(text, str)
    return _JSON_OBJECT.validate_json(text)


def _page_from_tool(
    result: CallToolResult | InputRequiredResult,
) -> dict[str, JsonValue]:
    value = _tool_json(result)
    assert isinstance(value, dict)
    return value


@asynccontextmanager
async def _wired(
    *,
    jobs: dict[str, V1Job] | None = None,
    hits: list[dict[str, JsonValue]] | None = None,
) -> AsyncGenerator[Recorder]:
    recorder = Recorder(bodies=[])

    def handler(http_request: httpx2.Request) -> httpx2.Response:
        recorder.bodies.append(_JSON_OBJECT.validate_json(http_request.content))
        payload = [] if hits is None else hits
        return httpx2.Response(200, json={"hits": {"hits": payload}})

    client = create_async_client(
        auth=EsAuth(username="u", password=SecretStr("p")),
        transport=httpx2.MockTransport(handler),
    )
    k8s = K8s(
        batch=FakeBatch(jobs={} if jobs is None else jobs),
        core=FakeCore(),
    )
    set_logs_factory(lambda: client)
    set_k8s_factory(lambda: k8s)
    set_reauth(lambda: None)
    try:
        yield recorder
    finally:
        set_logs_factory(None)
        set_k8s_factory(None)
        set_status_store_factory(None)
        set_reauth(None)
        await client.aclose()


async def test_get_pipeline_status_through_tool() -> None:
    payload = PipelineStatus(
        request_id="req-1",
        name="Pipelines Rust Lifecycle",
        status="error",
        start_time="t0",
        end_time="t1",
        created_at="t2",
        steps=(
            PipelineStepStatus(
                step_index=0,
                name="",
                arguments='{"batchtime": "202609"}',
            ),
        ),
    )

    @dataclass(frozen=True, slots=True)
    class Store:
        def get(self, request_id: str) -> PipelineStatus:
            _ = request_id
            return payload

    set_status_store_factory(Store)
    try:
        async with _wired():
            result = await mcp.call_tool(
                "get_pipeline_status",
                {"request_id": "req-1"},
            )
    finally:
        set_status_store_factory(None)
    value = _tool_json(result)
    assert isinstance(value, dict)
    assert value["status"] == "error"
    steps = value["steps"]
    assert isinstance(steps, list)
    first = steps[0]
    assert isinstance(first, dict)
    assert first["step_index"] == 0
    assert first["arguments"] == '{"batchtime": "202609"}'


async def test_list_pipeline_pods_empty_through_tool() -> None:
    jobs = {
        "pipelines-r1-0-0": V1Job(
            metadata=V1ObjectMeta(name="pipelines-r1-0-0"),
            spec=V1JobSpec(template=V1PodTemplateSpec()),
        ),
    }
    async with _wired(jobs=jobs):
        result = await mcp.call_tool(
            "list_pipeline_pods",
            {"request_id": "r1", "step_index": 0, "replica": 0},
        )
    value = _tool_json(result)
    assert value == []


async def test_get_pipeline_step_status_through_tool() -> None:
    start = json.dumps(
        {
            "@timestamp": "2026-09-11T03:11:01Z",
            "message": "[k8s-client] starting pipeline",
            "job-name": "pipelines-req-0-0",
            "image-name": "pipelines-rust:v1.1.1",
            "container-name": "pipelines-rust",
            "namespace": "pipelines-prd",
            "arguments": '{ "batchtime": "202608", "client": "isaca" }',
            "pipeline-id": "req",
        }
    )
    slack = json.dumps(
        {
            "message": "Unable to send slack message, logging instead",
            "detail": "Index: 0, replica: 0, status: error ",
        }
    )
    hits: list[dict[str, JsonValue]] = [
        {"_source": {"log": start}, "sort": ["t1"]},
        {"_source": {"log": slack}, "sort": ["t2"]},
    ]
    async with _wired(hits=hits):
        result = await mcp.call_tool(
            "get_pipeline_step_status",
            {"request_id": "req"},
        )
    value = _tool_json(result)
    assert isinstance(value, dict)
    assert value["request_id"] == "req"
    steps = value["steps"]
    assert isinstance(steps, list)
    first = steps[0]
    assert isinstance(first, dict)
    assert first["step_index"] == 0
    assert first["replica"] == 0
    assert first["status"] == "error"
    assert first["job_name"] == "pipelines-req-0-0"


async def test_get_pipeline_start_asks_for_twenty_lines() -> None:
    line = json.dumps(
        {
            "@timestamp": "2026-09-11T03:11:01Z",
            "message": "[k8s-client] starting pipeline",
            "job-name": "pipelines-req-0-0",
            "image-name": "pipelines-rust:v1.1.1",
            "container-name": "pipelines-rust",
            "namespace": "pipelines-prd",
            "arguments": '{ "batchtime": "202608", "client": "isaca" }',
            "pipeline-id": "req",
        }
    )
    hits: list[dict[str, JsonValue]] = [{"_source": {"log": line}, "sort": ["t1"]}]
    async with _wired(hits=hits) as recorder:
        result = await mcp.call_tool("get_pipeline_start", {"request_id": "req"})
    value = _page_from_tool(result)
    assert value["job_name"] == "pipelines-req-0-0"
    assert recorder.bodies[0]["size"] == 20


@pytest.mark.parametrize(
    ("tool", "filter_text"),
    [
        ("search_pipeline_log", 'kubernetes.pod_name:"req-1"'),
        ("search_pipeline_service_log", 'parsed.pipeline-id:"req-1"'),
    ],
)
async def test_search_log_query_string_has_filter_and_text(
    tool: str, filter_text: str
) -> None:
    hits: list[dict[str, JsonValue]] = [
        {"_source": {"log": "boom"}, "sort": ["t1"]},
    ]
    async with _wired(hits=hits) as recorder:
        result = await mcp.call_tool(
            tool,
            {"request_id": "req-1", "query": "boom"},
        )
    value = _page_from_tool(result)
    assert value["lines"] == ["boom"]
    query = recorder.bodies[0]["query"]
    assert isinstance(query, dict)
    clause = query["query_string"]
    assert isinstance(clause, dict)
    assert clause["query"] == f'{filter_text} AND "boom"'


async def test_get_pipeline_step_log_filters_by_job_name() -> None:
    hits: list[dict[str, JsonValue]] = [
        {"_source": {"log": "step-ok"}, "sort": ["t1"]},
    ]
    async with _wired(hits=hits) as recorder:
        result = await mcp.call_tool(
            "get_pipeline_step_log",
            {"request_id": "req-1", "step_index": 2, "replica": 0},
        )
    value = _page_from_tool(result)
    assert value["lines"] == ["step-ok"]
    query = recorder.bodies[0]["query"]
    assert isinstance(query, dict)
    clause = query["query_string"]
    assert isinstance(clause, dict)
    assert clause["query"] == 'kubernetes.pod_name:"pipelines-req-1-2-0"'


async def test_get_pipeline_step_log_rejects_uuid_cursor() -> None:
    hits: list[dict[str, JsonValue]] = [
        {"_source": {"log": "mixed"}, "sort": ["t1"]},
    ]
    async with _wired(hits=hits):
        mixed = await mcp.call_tool("get_pipeline_log", {"request_id": "req-1"})
        page = _page_from_tool(mixed)
        cursor = page["cursor"]
        assert isinstance(cursor, str)
        with pytest.raises(ToolError, match="invalid cursor"):
            _ = await mcp.call_tool(
                "get_pipeline_step_log",
                {
                    "request_id": "req-1",
                    "step_index": 2,
                    "replica": 0,
                    "cursor": cursor,
                },
            )


async def test_timescaling_log_tool_reads_store() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            _ = prefix
            return (
                "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr",
            )

        def get_bytes(self, key: str) -> bytes:
            _ = key
            return b"gpu-fail\n"

    set_object_store_factory(Store)
    try:
        result = await mcp.call_tool(
            "get_timescaling_log",
            {
                "client": "acme",
                "batchtime": "202608",
                "comptype": "dashboard",
            },
        )
    finally:
        set_object_store_factory(None)
    value = _page_from_tool(result)
    assert value["lines"] == ["gpu-fail"]


async def test_list_pipeline_artifacts_tool_reads_store() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            _ = prefix
            return (
                "202608/acme/dashboard/output/a",
                "202608/acme/dashboard/timescaling/logs/b",
            )

        def get_bytes(self, key: str) -> bytes:
            raise AssertionError(key)

    set_object_store_factory(Store)
    try:
        result = await mcp.call_tool(
            "list_pipeline_artifacts",
            {
                "client": "acme",
                "batchtime": "202608",
                "comptype": "dashboard",
            },
        )
    finally:
        set_object_store_factory(None)
    value = _page_from_tool(result)
    assert value["prefix"] == "202608/acme/dashboard/"
    assert value["folders"] == [
        {"name": "output", "object_count": 1},
        {"name": "timescaling", "object_count": 1},
    ]


async def test_list_pipeline_artifact_files_tool_reads_store() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            _ = prefix
            return (
                "202608/acme/dashboard/timescaling/model_input/part-0.json",
                "202608/acme/dashboard/timescaling/model_output/part-1.json",
            )

        def get_bytes(self, key: str) -> bytes:
            raise AssertionError(key)

    set_object_store_factory(Store)
    try:
        result = await mcp.call_tool(
            "list_pipeline_artifact_files",
            {
                "client": "acme",
                "batchtime": "202608",
                "comptype": "dashboard",
                "folder": "timescaling",
            },
        )
    finally:
        set_object_store_factory(None)
    value = _page_from_tool(result)
    assert value["prefix"] == "202608/acme/dashboard/timescaling/"
    assert value["keys"] == [
        "model_input/part-0.json",
        "model_output/part-1.json",
    ]


async def test_list_pipeline_lifecycle_artifacts_tool_reads_store() -> None:
    request_id = "11111111-1111-1111-1111-111111111111"

    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            if prefix == f"202608/rust-unloads/{request_id}/":
                return (f"{prefix}reference/plan.json",)
            if prefix == (
                "202608/input_pipelines/main/final/globals_rs/timescaling_v4/"
            ):
                return (f"{prefix}company.jsonl",)
            return ()

        def get_bytes(self, key: str) -> bytes:
            raise AssertionError(key)

    set_object_store_factory(Store)
    try:
        result = await mcp.call_tool(
            "list_pipeline_lifecycle_artifacts",
            {"batchtime": "202608", "request_id": request_id},
        )
    finally:
        set_object_store_factory(None)
    value = _page_from_tool(result)
    assert value["unloads_prefix"] == f"202608/rust-unloads/{request_id}/"
    assert value["folders"] == [{"name": "reference", "object_count": 1}]
    assert value["jsonl_prefix"] == (
        "202608/input_pipelines/main/final/globals_rs/timescaling_v4/"
    )
    assert value["jsonl_names"] == ["company.jsonl"]
