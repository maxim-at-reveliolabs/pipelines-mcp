from base64 import b64encode
from typing import Final

import httpx2
import pytest
from pydantic import SecretStr, TypeAdapter

from pipelines_mcp.errors import DomainError
from pipelines_mcp.logs import (
    JsonValue,
    LogRequest,
    create_logs_client,
    fetch_logs,
)
from pipelines_mcp.models import LogKind, LogPage
from pipelines_mcp.settings import BasicAuth

pytestmark = pytest.mark.anyio

_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])
_AUTH = BasicAuth(username="u", password=SecretStr("p"))


def _es_hit(line: str, stamp: str) -> dict[str, JsonValue]:
    return {"_source": {"log": line}, "sort": [stamp]}


async def _fetch(
    request: LogRequest,
    hits: list[dict[str, JsonValue]],
) -> tuple[LogPage, list[dict[str, JsonValue]]]:
    bodies: list[dict[str, JsonValue]] = []

    def handler(http_request: httpx2.Request) -> httpx2.Response:
        bodies.append(_JSON_OBJECT.validate_json(http_request.content))
        return httpx2.Response(200, json={"hits": {"hits": hits}})

    transport = httpx2.MockTransport(handler)
    async with create_logs_client(auth=_AUTH, transport=transport) as client:
        page = await fetch_logs(client, request)
    return page, bodies


def _query_string(body: dict[str, JsonValue]) -> dict[str, JsonValue]:
    query = body["query"]
    assert isinstance(query, dict)
    clause = query["query_string"]
    assert isinstance(clause, dict)
    return clause


def _assert_search(
    body: dict[str, JsonValue], *, order: str, size: int
) -> None:
    assert body["sort"] == [{"@timestamp": {"order": order}}]
    assert "_source" not in body
    assert body["size"] == size


@pytest.mark.parametrize(
    ("kind", "line", "query", "absent"),
    [
        (LogKind.SERVICE, "svc", 'parsed.pipeline-id:"req-1"', "kubernetes.pod_name"),
        (LogKind.WORKER, "pod", 'kubernetes.pod_name:"req-1"', "parsed.pipeline-id"),
    ],
)
async def test_kind_filters_query_string(
    kind: LogKind, line: str, query: str, absent: str
) -> None:
    request = LogRequest(match="req-1", log_kind=kind)
    page, bodies = await _fetch(request, [_es_hit(line, "t1")])
    assert page.lines == (line,)
    clause = _query_string(bodies[0])
    assert "default_field" not in clause
    assert clause["query"] == query
    assert absent not in str(bodies[0])
    _assert_search(bodies[0], order="desc", size=100)


async def test_returns_whole_log_line_not_message() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER)
    hits: list[dict[str, JsonValue]] = [
        {
            "_source": {"log": "whole-line message=extracted", "message": "extracted"},
            "sort": ["t1"],
        },
        {"_source": {"message": "only-message"}, "sort": ["t2"]},
    ]
    page, _ = await _fetch(request, hits)
    assert page.lines == ("whole-line message=extracted",)


async def test_cursor_search_after_passthrough() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER)
    first_hits = [
        _es_hit("newer", "2026-01-01T00:00:02Z"),
        _es_hit("older", "2026-01-01T00:00:01Z"),
    ]
    first_page, first_bodies = await _fetch(request, first_hits)
    follow = LogRequest(
        match="r1",
        log_kind=LogKind.WORKER,
        cursor=first_page.cursor,
    )
    second_page, second_bodies = await _fetch(
        follow,
        [_es_hit("newest", "2026-01-01T00:00:03Z")],
    )
    assert first_page.lines == ("older", "newer")
    _assert_search(first_bodies[0], order="desc", size=100)
    assert second_page.lines == ("newest",)
    body = second_bodies[0]
    assert body["search_after"] == ["2026-01-01T00:00:02Z"]
    _assert_search(body, order="asc", size=100)


async def test_full_byte_cap() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER, full=True)
    hits = [
        _es_hit("a" * 20000, "t1"),
        _es_hit("b" * 20000, "t2"),
        _es_hit("cccc", "t3"),
    ]
    page, bodies = await _fetch(request, hits)
    assert page.lines == ("a" * 20000,)
    assert page.truncated is True
    _assert_search(bodies[0], order="asc", size=500)


async def test_empty_hits_returns_cursor_string() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER)
    page, bodies = await _fetch(request, [])
    assert page.lines == ()
    assert page.truncated is False
    assert page.note is not None
    assert "running" in page.note
    assert page.cursor != ""
    _assert_search(bodies[0], order="desc", size=100)


async def test_invalid_cursor_raises() -> None:
    request = LogRequest(
        match="r1",
        log_kind=LogKind.WORKER,
        cursor="not-a-cursor",
    )
    with pytest.raises(DomainError, match="invalid cursor"):
        _ = await _fetch(request, [])


async def test_mismatched_cursor_raises() -> None:
    first = LogRequest(match="r1", log_kind=LogKind.SERVICE)
    first_page, _ = await _fetch(first, [_es_hit("x", "t1")])
    mismatched = LogRequest(
        match="r1",
        log_kind=LogKind.WORKER,
        cursor=first_page.cursor,
    )
    with pytest.raises(DomainError, match="invalid cursor"):
        _ = await _fetch(mismatched, [])


async def test_tail_keeps_last_hundred_chronological_lines() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER)
    hits = [_es_hit(f"line-{index}", f"t{index:03d}") for index in range(99, -1, -1)]
    page, bodies = await _fetch(request, hits)
    assert page.lines == tuple(f"line-{index}" for index in range(100))
    assert page.truncated is True
    _assert_search(bodies[0], order="desc", size=100)


async def test_http_error_becomes_domain_error() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER)

    def handler(http_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, request=http_request)

    transport = httpx2.MockTransport(handler)
    async with create_logs_client(auth=_AUTH, transport=transport) as client:
        with pytest.raises(DomainError, match="log store 503"):
            _ = await fetch_logs(client, request)


async def test_client_sets_basic_auth_header() -> None:
    auth = BasicAuth(username="elastic", password=SecretStr("es-pass"))
    async with create_logs_client(
        auth=auth,
        transport=httpx2.MockTransport(lambda _req: httpx2.Response(200)),
    ) as client:
        token = b64encode(b"elastic:es-pass").decode("ascii")
        assert client.headers["Authorization"] == f"Basic {token}"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("boom", 'kubernetes.pod_name:"req-1" AND "boom"'),
        (
            'status:error AND "fail"',
            'kubernetes.pod_name:"req-1" AND "status:error AND \\"fail\\""',
        ),
    ],
)
async def test_query_is_quoted_and_anded_with_kind_filter(
    text: str, expected: str
) -> None:
    request = LogRequest(match="req-1", log_kind=LogKind.WORKER, query=text)
    _, bodies = await _fetch(request, [])
    assert _query_string(bodies[0])["query"] == expected


async def test_empty_query_raises() -> None:
    request = LogRequest(match="r1", log_kind=LogKind.WORKER, query="")
    with pytest.raises(DomainError, match="empty query"):
        _ = await _fetch(request, [])
