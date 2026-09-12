"""Elasticsearch query bodies, log pages, and the HTTP client factory."""

import base64
import binascii
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum, unique
from socket import IPPROTO_TCP, TCP_NODELAY
from typing import ClassVar, Final, Literal

import httpx2
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pipelines_mcp.errors import EmptyQueryError, InvalidCursorError, SettingsError
from pipelines_mcp.models import LogKind, LogPage
from pipelines_mcp.redact import redact_text
from pipelines_mcp.settings import EsAuth, load_es_auth

type JsonValue = (
    str | int | float | bool | Sequence[JsonValue] | Mapping[str, JsonValue] | None
)

_ES_CONTENT_HEADERS: Final[Mapping[str, str]] = {
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
}

TAIL_LINES: Final[int] = 100
FULL_MAX_HITS: Final[int] = 500
_FULL_MAX_BYTES: Final[int] = 32768


def create_async_client(
    *,
    auth: EsAuth | None = None,
    transport: httpx2.AsyncBaseTransport | None = None,
) -> httpx2.AsyncClient:
    """Build an async client with production HTTP/2 defaults."""
    client_transport = (
        transport
        if transport is not None
        else httpx2.AsyncHTTPTransport(
            http2=True,
            retries=3,
            limits=httpx2.Limits(
                max_connections=200,
                max_keepalive_connections=40,
                keepalive_expiry=30.0,
            ),
            socket_options=((IPPROTO_TCP, TCP_NODELAY, 1),),
            verify=True,
        )
    )
    resolved = load_es_auth() if auth is None else auth
    name = resolved.username
    secret = resolved.password.get_secret_value()
    headers = dict(_ES_CONTENT_HEADERS)
    token = base64.b64encode(f"{name}:{secret}".encode()).decode("ascii")
    headers["Authorization"] = f"Basic {token}"
    return httpx2.AsyncClient(
        transport=client_transport,
        timeout=httpx2.Timeout(
            connect=5.0,
            read=30.0,
            write=10.0,
            pool=10.0,
        ),
        base_url="https://elastic.gp0.k8s.dc1.rlc.cloud.reveliolabs.com:443",
        headers=headers,
        follow_redirects=True,
        verify=True,
    )


@dataclass(frozen=True, slots=True)
class LogRequest:
    """One request-level log read: kind, cursor, and page mode."""

    request_id: str
    log_kind: LogKind
    cursor: str | None = None
    full: bool = False
    size: int | None = None


@dataclass(frozen=True, slots=True)
class _SearchPage:
    order: str
    size: int
    search_after: list[str | int | float] | None


def _search_body(request: LogRequest, page: _SearchPage) -> dict[str, JsonValue]:
    match request.log_kind:
        case LogKind.SERVICE:
            field = "parsed.pipeline-id"
        case LogKind.PIPELINE:
            field = "kubernetes.pod_name"
    body: dict[str, JsonValue] = {
        "query": {"query_string": {"query": f'{field}:"{request.request_id}"'}},
        "sort": [{"@timestamp": {"order": page.order}}],
        "size": page.size,
    }
    if page.search_after is not None:
        body["search_after"] = page.search_after
    return body


@unique
class LogMode(StrEnum):
    """Cursor page mode."""

    TAIL = "tail"
    FULL = "full"


@dataclass(frozen=True, slots=True)
class _Hit:
    line: str
    sort: tuple[str | int | float, ...]


@dataclass(frozen=True, slots=True)
class _PageCtx:
    request: LogRequest
    mode: LogMode
    prior_sa: list[str | int | float] | None
    size: int


class _CursorPayload(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    v: Literal[1]
    sa: list[str | int | float] | None
    mode: LogMode
    kind: LogKind
    req: str


class _EsSource(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    log: str | None = None


class _EsHit(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    source: _EsSource = Field(alias="_source")
    sort: tuple[str | int | float, ...] = ()


class _EsHits(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    hits: tuple[_EsHit, ...] = ()


class _EsResponse(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    hits: _EsHits = _EsHits()


def _decode_cursor(cursor: str) -> _CursorPayload:
    try:
        raw = base64.b64decode(cursor.encode("ascii"), validate=True)
        return _CursorPayload.model_validate_json(raw)
    except (ValueError, binascii.Error, UnicodeError, ValidationError) as exc:
        raise InvalidCursorError from exc


def _check_cursor(payload: _CursorPayload, request: LogRequest, mode: LogMode) -> None:
    mismatched = (
        payload.kind != request.log_kind
        or payload.mode != mode
        or payload.req != request.request_id
    )
    if mismatched:
        raise InvalidCursorError


def cap_log_bytes[T](
    items: tuple[T, ...],
    *,
    drop_from_front: bool,
    text_of: Callable[[T], str],
) -> tuple[tuple[T, ...], bool]:
    """Keep a prefix or suffix of items whose UTF-8 size fits the byte cap."""
    sizes = tuple(len(text_of(item).encode("utf-8")) for item in items)
    total = sum(sizes)
    if total <= _FULL_MAX_BYTES:
        return items, False
    if drop_from_front:
        index = 0
        running = total
        while index < len(items) and running > _FULL_MAX_BYTES:
            running -= sizes[index]
            index += 1
        return items[index:], True
    end = 0
    running = 0
    while end < len(items) and running + sizes[end] <= _FULL_MAX_BYTES:
        running += sizes[end]
        end += 1
    return items[:end], True


def _to_hits(parsed: _EsResponse) -> tuple[_Hit, ...]:
    out: list[_Hit] = []
    for hit in parsed.hits.hits:
        if hit.source.log is None:
            continue
        out.append(_Hit(line=hit.source.log, sort=hit.sort))
    return tuple(out)


def _cursor_for(ctx: _PageCtx, sa: list[str | int | float] | None) -> str:
    dumped = _CursorPayload(
        v=1,
        sa=sa,
        mode=ctx.mode,
        kind=ctx.request.log_kind,
        req=ctx.request.request_id,
    ).model_dump(mode="json")
    raw = json.dumps(dumped, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return redact_text(base64.b64encode(raw).decode("ascii"))


def _empty_note(kind: LogKind) -> str:
    match kind:
        case LogKind.PIPELINE:
            return "No lines. Worker logs stay empty until a pod is running."
        case LogKind.SERVICE:
            return "No lines."


def _build_page(hits: tuple[_Hit, ...], ctx: _PageCtx) -> LogPage:
    if not hits:
        return LogPage(
            lines=(),
            cursor=_cursor_for(ctx, ctx.prior_sa),
            truncated=False,
            note=_empty_note(ctx.request.log_kind),
        )
    match ctx.mode:
        case LogMode.TAIL:
            chronological = (
                hits if ctx.prior_sa is not None else tuple(reversed(hits))
            )
        case LogMode.FULL:
            chronological = hits
    redacted = tuple(
        _Hit(line=redact_text(hit.line), sort=hit.sort) for hit in chronological
    )
    kept, dropped = cap_log_bytes(
        redacted,
        drop_from_front=ctx.mode is LogMode.TAIL,
        text_of=lambda hit: hit.line,
    )
    sa = ctx.prior_sa if not kept else list(kept[-1].sort)
    return LogPage(
        lines=tuple(hit.line for hit in kept),
        cursor=_cursor_for(ctx, sa),
        truncated=dropped or len(hits) == ctx.size,
        note=_empty_note(ctx.request.log_kind) if not kept else None,
    )


def _normalize(request: LogRequest) -> LogRequest:
    request_id = request.request_id.strip()
    if request_id == "":
        raise EmptyQueryError(field="request_id")
    return replace(request, request_id=request_id)


async def fetch_logs(
    client: httpx2.AsyncClient,
    request: LogRequest,
) -> LogPage:
    """Fetch one capped log page from Elasticsearch."""
    normalized = _normalize(request)
    mode = LogMode.FULL if normalized.full else LogMode.TAIL
    prior_sa: list[str | int | float] | None = None
    if normalized.cursor is not None:
        payload = _decode_cursor(normalized.cursor)
        _check_cursor(payload, normalized, mode)
        prior_sa = payload.sa
    match mode:
        case LogMode.TAIL:
            order = "asc" if normalized.cursor is not None else "desc"
            size = TAIL_LINES
        case LogMode.FULL:
            order = "asc"
            size = FULL_MAX_HITS if normalized.size is None else normalized.size
    content = json.dumps(
        _search_body(
            normalized,
            _SearchPage(order=order, size=size, search_after=prior_sa),
        ),
        separators=(",", ":"),
    ).encode("utf-8")
    try:
        response = await client.post(
            "/fluentd.iip-dev-*,fluentd.gp0.general-*/_search",
            content=content,
            headers=_ES_CONTENT_HEADERS,
        )
        _ = response.raise_for_status()
    except httpx2.HTTPStatusError as exc:
        raise SettingsError(reason=f"log store {exc.response.status_code}") from exc
    except httpx2.RequestError as exc:
        raise SettingsError(reason="log store unreachable") from exc
    parsed = _EsResponse.model_validate_json(response.content)
    return _build_page(
        _to_hits(parsed),
        _PageCtx(request=normalized, mode=mode, prior_sa=prior_sa, size=size),
    )
