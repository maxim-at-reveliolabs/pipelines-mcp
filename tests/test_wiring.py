from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Final, NoReturn

import httpx2
import pytest
from mcp.types import CallToolResult, InputRequiredResult
from pydantic import SecretStr, TypeAdapter

from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.logs import create_async_client
from pipelines_mcp.server import mcp
from pipelines_mcp.server_app import App, set_builder, set_k8s_factory
from pipelines_mcp.settings import EsAuth

pytestmark = pytest.mark.anyio

type JsonValue = (
    str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None
)

_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])
_SRC: Final = Path(__file__).resolve().parents[1] / "src"
_FORBIDDEN: Final[tuple[str, ...]] = (
    "read_namespaced_secret",
    "read_namespaced_pod_log",
    "subprocess",
    "kubectl",
    "load_kube_config",
)


@dataclass(frozen=True, slots=True)
class FakeMeta:
    name: str | None = None
    uid: str | None = None
    labels: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class FakeJob:
    metadata: FakeMeta | None = None
    spec: None = None
    status: None = None


@dataclass(frozen=True, slots=True)
class FakePodList:
    items: tuple[()] = ()


@dataclass(frozen=True, slots=True)
class FakeBatch:
    jobs: dict[str, FakeJob]

    def read_namespaced_job(self, name: str, namespace: str) -> FakeJob:
        _ = namespace
        job = self.jobs.get(name)
        if job is None:
            raise K8sApiError(status=404)
        return job

    def list_namespaced_job(self, namespace: str) -> FakePodList:
        _ = namespace
        return FakePodList()


@dataclass(frozen=True, slots=True)
class FakeCore:
    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> FakePodList:
        _ = namespace
        _ = label_selector
        return FakePodList()

    def read_namespaced_pod(self, name: str, namespace: str) -> NoReturn:
        _ = name
        _ = namespace
        raise K8sApiError(status=404)


@dataclass(frozen=True, slots=True)
class Recorder:
    bodies: list[dict[str, JsonValue]]


def _as_tool_result(
    result: CallToolResult | InputRequiredResult,
) -> CallToolResult:
    match result:
        case CallToolResult() as call:
            return call
        case InputRequiredResult():
            pytest.fail("tool asked for input")


def _tool_json(result: CallToolResult | InputRequiredResult) -> JsonValue:
    dumped = _JSON_OBJECT.validate_json(_as_tool_result(result).model_dump_json())
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


def _kind_clause(body: dict[str, JsonValue]) -> dict[str, JsonValue]:
    query = body["query"]
    assert isinstance(query, dict)
    clause = query["query_string"]
    assert isinstance(clause, dict)
    return clause


@asynccontextmanager
async def _wired(
    *,
    jobs: dict[str, FakeJob] | None = None,
    hits: list[dict[str, JsonValue]] | None = None,
) -> AsyncGenerator[Recorder]:
    recorder = Recorder(bodies=[])

    def handler(http_request: httpx2.Request) -> httpx2.Response:
        if http_request.method == "DELETE":
            return httpx2.Response(200, json={"succeeded": True})
        if "/_search/scroll" in str(http_request.url):
            return httpx2.Response(200, json={"_scroll_id": "s1", "hits": {"hits": []}})
        recorder.bodies.append(_JSON_OBJECT.validate_json(http_request.content))
        payload = [] if hits is None else hits
        return httpx2.Response(
            200, json={"_scroll_id": "s1", "hits": {"hits": payload}}
        )

    client = create_async_client(
        auth=EsAuth(username="u", password=SecretStr("p")),
        transport=httpx2.MockTransport(handler),
    )
    k8s = K8s(
        batch=FakeBatch(jobs={} if jobs is None else jobs),
        core=FakeCore(),
    )
    set_builder(lambda: App(logs_client=client))
    set_k8s_factory(lambda: k8s)
    try:
        yield recorder
    finally:
        set_builder(None)
        set_k8s_factory(None)
        await client.aclose()


async def test_list_pipeline_pods_empty_through_tool() -> None:
    # Given: the job exists and its pods are already gone
    jobs = {
        "pipelines-r1-0-0": FakeJob(metadata=FakeMeta(name="pipelines-r1-0-0")),
    }

    # When: list_pipeline_pods is called through the MCP tool
    async with _wired(jobs=jobs):
        result = await mcp.call_tool(
            "list_pipeline_pods",
            {"request_id": "r1", "step_index": 0, "replica": 0},
        )

    # Then: the tool returns an empty list
    value = _tool_json(result)
    assert value == []


@pytest.mark.parametrize(
    ("tool", "line", "query", "absent"),
    [
        (
            "get_pipeline_service_log",
            "svc",
            'parsed.pipeline-id:"req-1"',
            "kubernetes.pod_name",
        ),
        (
            "get_pipeline_log",
            "pod",
            'kubernetes.pod_name:"req-1"',
            "parsed.pipeline-id",
        ),
    ],
)
async def test_log_tool_filters_kind(
    tool: str, line: str, query: str, absent: str
) -> None:
    # Given: logs for a request
    hits: list[dict[str, JsonValue]] = [{"_source": {"log": line}, "sort": ["t1"]}]

    # When: the matching log tool is called
    async with _wired(hits=hits) as recorder:
        result = await mcp.call_tool(tool, {"request_id": "req-1"})

    # Then: query_string matches that tool's field
    value = _page_from_tool(result)
    assert value["lines"] == [line]
    clause = _kind_clause(recorder.bodies[0])
    assert "default_field" not in clause
    assert clause["query"] == query
    assert absent not in str(recorder.bodies[0])


def test_src_has_no_forbidden_cluster_apis() -> None:
    # Given: production sources under src/
    # When: scanning for forbidden cluster APIs
    hits: list[str] = []
    for path in _SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        hits.extend(f"{path}:{needle}" for needle in _FORBIDDEN if needle in text)

    # Then: none of those APIs appear
    assert hits == []
