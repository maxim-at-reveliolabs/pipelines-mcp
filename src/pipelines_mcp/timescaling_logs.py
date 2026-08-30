"""Timescaling model log pages from an injected object store."""

from __future__ import annotations

import base64
import binascii
import gzip
import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, ClassVar, Final, Literal

from anyio.to_thread import run_sync
from pydantic import BaseModel, ConfigDict, ValidationError

from pipelines_mcp.errors import EmptyQueryError, InvalidCursorError, SettingsError
from pipelines_mcp.logs import LogMode
from pipelines_mcp.models import LogPage
from pipelines_mcp.redact import redact_text

if TYPE_CHECKING:
    from pipelines_mcp.object_store import LogStore

_TAIL_LINES: Final = 100
_FULL_MAX_HITS: Final = 500
_FULL_MAX_BYTES: Final = 32768
_PREFERRED: Final = ("stderr", "controller", "stdout")
_EMPTY_NOTE: Final = (
    "No lines. Timescaling model logs stay empty until the model writes them."
)


@dataclass(frozen=True, slots=True)
class TimescalingLogRequest:
    """One timescaling log read: run identity, cursor, and page mode."""

    client: str
    batchtime: str
    comptype: str
    cursor: str | None = None
    full: bool = False


class _CursorPayload(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="forbid")
    v: Literal[1]
    sa: int | None
    mode: LogMode
    client: str
    batchtime: str
    comptype: str


def _token(raw: str, field: str) -> str:
    stripped = raw.strip()
    if stripped == "" or "/" in stripped:
        raise EmptyQueryError(field=field)
    return stripped


def _preferred_rank(name: str) -> int:
    for index, prefix in enumerate(_PREFERRED):
        if name == prefix or name.startswith(f"{prefix}."):
            return index
    return len(_PREFERRED)


def _pick_files(keys: tuple[str, ...]) -> tuple[str, ...]:
    by_dir: dict[str, list[str]] = {}
    for key in keys:
        parent, _, name = key.rpartition("/")
        if _preferred_rank(name) == len(_PREFERRED):
            continue
        by_dir.setdefault(parent, []).append(key)
    picked: list[str] = []
    for parent in sorted(by_dir):
        files = sorted(
            by_dir[parent],
            key=lambda item: _preferred_rank(item.rpartition("/")[2]),
        )
        picked.append(files[0])
    return tuple(picked)


def _decode(key: str, raw: bytes) -> str:
    if key.endswith(".gz"):
        try:
            raw = gzip.decompress(raw)
        except OSError as exc:
            raise SettingsError(reason="log store unreachable") from exc
    return raw.decode("utf-8", errors="replace")


def _decode_cursor(cursor: str) -> _CursorPayload:
    try:
        raw = base64.b64decode(cursor.encode("ascii"), validate=True)
        return _CursorPayload.model_validate_json(raw)
    except (ValueError, binascii.Error, UnicodeError, ValidationError) as exc:
        raise InvalidCursorError from exc


def _cursor_for(
    request: TimescalingLogRequest,
    mode: LogMode,
    sa: int | None,
) -> str:
    dumped = _CursorPayload(
        v=1,
        sa=sa,
        mode=mode,
        client=request.client,
        batchtime=request.batchtime,
        comptype=request.comptype,
    ).model_dump(mode="json")
    raw = json.dumps(dumped, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return redact_text(base64.b64encode(raw).decode("ascii"))


def _cap_bytes(
    lines: tuple[str, ...],
    *,
    drop_from_front: bool,
) -> tuple[tuple[str, ...], bool]:
    sizes = tuple(len(line.encode("utf-8")) for line in lines)
    total = sum(sizes)
    if total <= _FULL_MAX_BYTES:
        return lines, False
    if drop_from_front:
        index = 0
        running = total
        while index < len(lines) and running > _FULL_MAX_BYTES:
            running -= sizes[index]
            index += 1
        return lines[index:], True
    end = 0
    running = 0
    while end < len(lines) and running + sizes[end] <= _FULL_MAX_BYTES:
        running += sizes[end]
        end += 1
    return lines[:end], True


def _build_page(
    lines: tuple[str, ...],
    request: TimescalingLogRequest,
    mode: LogMode,
    prior_sa: int | None,
) -> LogPage:
    match mode:
        case LogMode.TAIL:
            if prior_sa is None:
                start = max(0, len(lines) - _TAIL_LINES)
                window = lines[start:]
                next_sa = len(lines)
                overflow = start > 0
                drop_front = True
            else:
                window = lines[prior_sa : prior_sa + _TAIL_LINES]
                next_sa = prior_sa + len(window)
                overflow = next_sa < len(lines)
                drop_front = False
        case LogMode.FULL:
            start = 0 if prior_sa is None else prior_sa
            window = lines[start : start + _FULL_MAX_HITS]
            next_sa = start + len(window)
            overflow = next_sa < len(lines)
            drop_front = False
    redacted = tuple(redact_text(line) for line in window)
    kept, dropped = _cap_bytes(redacted, drop_from_front=drop_front)
    if not kept:
        return LogPage(
            lines=(),
            cursor=_cursor_for(request, mode, prior_sa),
            truncated=False,
            note=_EMPTY_NOTE,
        )
    if dropped and not drop_front:
        start = 0 if prior_sa is None else prior_sa
        next_sa = start + len(kept)
    return LogPage(
        lines=kept,
        cursor=_cursor_for(request, mode, next_sa),
        truncated=dropped or overflow,
        note=None,
    )


async def fetch_timescaling_logs(
    store: LogStore,
    request: TimescalingLogRequest,
) -> LogPage:
    """Fetch one capped timescaling log page."""
    normalized = replace(
        request,
        client=_token(request.client, "client"),
        batchtime=_token(request.batchtime, "batchtime"),
        comptype=_token(request.comptype, "comptype"),
    )
    mode = LogMode.FULL if normalized.full else LogMode.TAIL
    prior_sa: int | None = None
    if normalized.cursor is not None:
        payload = _decode_cursor(normalized.cursor)
        mismatched = (
            payload.mode != mode
            or payload.client != normalized.client
            or payload.batchtime != normalized.batchtime
            or payload.comptype != normalized.comptype
        )
        if mismatched:
            raise InvalidCursorError
        prior_sa = payload.sa
    name = f"{normalized.client}_{normalized.batchtime}_{normalized.comptype}"
    prefixes = (
        f"{normalized.batchtime}/{normalized.client}/{normalized.comptype}/timescaling/logs/",
        f"{normalized.batchtime}/logs/{name}_replica_0/",
        f"{normalized.batchtime}/logs/{name}/",
    )
    keys: tuple[str, ...] = ()
    for prefix in prefixes:
        keys = await run_sync(store.list_keys, prefix)
        if keys:
            break
    chunks: list[str] = []
    for key in _pick_files(keys):
        raw = await run_sync(store.get_bytes, key)
        chunks.extend(line for line in _decode(key, raw).splitlines())
    return _build_page(tuple(chunks), normalized, mode, prior_sa)
