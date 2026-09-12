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
from mcp.types import CallToolResult, InputRequiredResult
from pydantic import SecretStr, TypeAdapter

from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.logs import create_async_client
from pipelines_mcp.server import mcp
from pipelines_mcp.server_app import (
    set_k8s_factory,
    set_logs_factory,
    set_object_store_factory,
    set_reauth,
)
from pipelines_mcp.settings import EsAuth

pytestmark = pytest.mark.anyio

type JsonValue = (
    str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None
)

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
        set_reauth(None)
        await client.aclose()


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
