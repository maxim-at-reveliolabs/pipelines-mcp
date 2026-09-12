from base64 import b64encode
from typing import Final

import httpx2
import pytest
from pydantic import SecretStr, TypeAdapter, ValidationError

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.logs import (
    LogRequest,
    create_async_client,
    fetch_logs,
)
from pipelines_mcp.models import LogKind, LogPage
from pipelines_mcp.settings import EsAuth

type JsonValue = (
    str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None
)

pytestmark = pytest.mark.anyio

_JSON_OBJECT: Final = TypeAdapter(dict[str, JsonValue])
_AUTH = EsAuth(username="u", password=SecretStr("p"))


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
    async with create_async_client(auth=_AUTH, transport=transport) as client:
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
        (LogKind.PIPELINE, "pod", 'kubernetes.pod_name:"req-1"', "parsed.pipeline-id"),
    ],
)
async def test_kind_filters_query_string(
    kind: LogKind, line: str, query: str, absent: str
) -> None:
    # Given: logs for a request of one kind
    request = LogRequest(request_id="req-1", log_kind=kind)

    # When: the first tail page is fetched
    page, bodies = await _fetch(request, [_es_hit(line, "t1")])

    # Then: query_string matches that kind's field
    assert page.lines == (line,)
    clause = _query_string(bodies[0])
    assert "default_field" not in clause
    assert clause["query"] == query
    assert absent not in str(bodies[0])
    _assert_search(bodies[0], order="desc", size=100)


async def test_returns_whole_log_line_not_message() -> None:
    # Given: a hit with both a raw log line and a parsed message
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE)
    hits: list[dict[str, JsonValue]] = [
        {
            "_source": {"log": "whole-line message=extracted", "message": "extracted"},
            "sort": ["t1"],
        },
        {"_source": {"message": "only-message"}, "sort": ["t2"]},
    ]

    # When: logs are fetched
    page, _ = await _fetch(request, hits)

    # Then: the raw log line is returned and message-only hits are skipped
    assert page.lines == ("whole-line message=extracted",)


async def test_cursor_search_after_passthrough() -> None:
    # Given: a tail page whose newest hit sort is known
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE)
    first_hits = [
        _es_hit("newer", "2026-01-01T00:00:02Z"),
        _es_hit("older", "2026-01-01T00:00:01Z"),
    ]

    # When: the first page is fetched, then a follow-up uses that cursor
    first_page, first_bodies = await _fetch(request, first_hits)
    follow = LogRequest(
        request_id="r1",
        log_kind=LogKind.PIPELINE,
        cursor=first_page.cursor,
    )
    second_page, second_bodies = await _fetch(
        follow,
        [_es_hit("newest", "2026-01-01T00:00:03Z")],
    )

    # Then: lines are chronological and search_after is the newest first-page sort
    assert first_page.lines == ("older", "newer")
    _assert_search(first_bodies[0], order="desc", size=100)
    assert second_page.lines == ("newest",)
    body = second_bodies[0]
    assert body["search_after"] == ["2026-01-01T00:00:02Z"]
    _assert_search(body, order="asc", size=100)


async def test_full_byte_cap() -> None:
    # Given: full mode with lines that exceed the 32768-byte cap
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE, full=True)
    hits = [
        _es_hit("a" * 20000, "t1"),
        _es_hit("b" * 20000, "t2"),
        _es_hit("cccc", "t3"),
    ]

    # When: the full page is fetched
    page, bodies = await _fetch(request, hits)

    # Then: whole lines are dropped from the end and truncated is set
    assert page.lines == ("a" * 20000,)
    assert page.truncated is True
    _assert_search(bodies[0], order="asc", size=500)


async def test_empty_hits_returns_cursor_string() -> None:
    # Given: a first tail page with no hits
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE)

    # When: the page is fetched
    page, bodies = await _fetch(request, [])

    # Then: lines are empty, truncated is false, and cursor is still a string
    assert page.lines == ()
    assert page.truncated is False
    assert page.note is not None
    assert "running" in page.note
    assert isinstance(page.cursor, str)
    assert page.cursor != ""
    _assert_search(bodies[0], order="desc", size=100)


async def test_invalid_cursor_raises() -> None:
    # Given: a cursor that is not valid base64 JSON
    request = LogRequest(
        request_id="r1",
        log_kind=LogKind.PIPELINE,
        cursor="not-a-cursor",
    )

    # When: logs are fetched with that cursor
    # Then: the call fails as an invalid cursor
    with pytest.raises(SettingsError, match="invalid cursor"):
        _ = await _fetch(request, [])


async def test_mismatched_cursor_raises() -> None:
    # Given: a valid tail cursor for service logs
    first = LogRequest(request_id="r1", log_kind=LogKind.SERVICE)
    first_page, _ = await _fetch(first, [_es_hit("x", "t1")])
    mismatched = LogRequest(
        request_id="r1",
        log_kind=LogKind.PIPELINE,
        cursor=first_page.cursor,
    )

    # When: the same cursor is reused with a different kind
    # Then: the call fails as an invalid cursor
    with pytest.raises(SettingsError, match="invalid cursor"):
        _ = await _fetch(mismatched, [])


async def test_tail_keeps_last_hundred_chronological_lines() -> None:
    # Given: a desc page of 100 hits, newest first
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE)
    hits = [_es_hit(f"line-{index}", f"t{index:03d}") for index in range(99, -1, -1)]

    # When: the first tail page is fetched
    page, bodies = await _fetch(request, hits)

    # Then: lines are oldest to newest and the page is full
    assert page.lines == tuple(f"line-{index}" for index in range(100))
    assert page.truncated is True
    _assert_search(bodies[0], order="desc", size=100)


async def test_http_error_becomes_settings_error() -> None:
    # Given: the log store returns 503
    request = LogRequest(request_id="r1", log_kind=LogKind.PIPELINE)

    def handler(http_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(503, request=http_request)

    # When: logs are fetched
    transport = httpx2.MockTransport(handler)
    async with create_async_client(auth=_AUTH, transport=transport) as client:
        # Then: the agent-facing error names the log store status
        with pytest.raises(SettingsError, match="log store 503"):
            _ = await fetch_logs(client, request)


async def test_client_sets_basic_auth_header() -> None:
    # Given: log store username and password
    auth = EsAuth(username="elastic", password=SecretStr("es-pass"))

    # When: the client is built with that auth
    async with create_async_client(
        auth=auth,
        transport=httpx2.MockTransport(lambda _req: httpx2.Response(200)),
    ) as client:
        token = b64encode(b"elastic:es-pass").decode("ascii")

        # Then: Authorization is Basic auth for those credentials
        assert client.headers["Authorization"] == f"Basic {token}"


def test_es_auth_rejects_blank_password() -> None:
    # Given: a password that is only whitespace
    # When: the auth model is built
    # Then: validation fails
    with pytest.raises(ValidationError):
        _ = EsAuth(username="elastic", password=SecretStr("   "))
