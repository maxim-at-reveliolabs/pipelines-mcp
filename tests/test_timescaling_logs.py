from dataclasses import dataclass, field
from gzip import compress

import pytest

from pipelines_mcp.errors import EmptyQueryError, InvalidCursorError
from pipelines_mcp.timescaling_logs import (
    TimescalingLogRequest,
    fetch_timescaling_logs,
)

pytestmark = pytest.mark.anyio


@dataclass(slots=True)
class FakeStore:
    """In-memory log objects. Mutation is the documented purpose."""

    objects: dict[str, bytes]
    prefixes: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        self.prefixes.append(prefix)
        return tuple(
            key for key in sorted(self.objects) if key.startswith(prefix)
        )

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]


def _request(
    *,
    client: str = "acme",
    cursor: str | None = None,
    full: bool = False,
) -> TimescalingLogRequest:
    return TimescalingLogRequest(
        client=client,
        batchtime="202608",
        comptype="dashboard",
        cursor=cursor,
        full=full,
    )


def _line_store(count: int) -> FakeStore:
    key = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr"
    body = "".join(f"line-{index}\n" for index in range(count)).encode()
    return FakeStore(objects={key: body})


async def test_lists_rust_prefix() -> None:
    # Given: a rust-layout stderr file for the run
    key = (
        "202608/acme/dashboard/timescaling/logs/timescaling-acme-dashboard-202608"
        "/j-ABC/steps/s-1/stderr"
    )
    store = FakeStore(objects={key: b"gpu-ok\n"})

    # When: the first tail page is fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: the rust prefix is listed and the line is returned
    assert store.prefixes[0] == "202608/acme/dashboard/timescaling/logs/"
    assert page.lines == ("gpu-ok",)
    assert page.truncated is False
    assert page.note is None


async def test_falls_back_to_legacy_prefix_when_rust_empty() -> None:
    # Given: only the legacy Go log layout has files
    key = "202608/logs/acme_202608_dashboard/j-ABC/steps/s-1/stderr"
    store = FakeStore(objects={key: b"legacy-ok\n"})

    # When: the first tail page is fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: rust is tried first, then the legacy prefix, and the line is returned
    assert store.prefixes == [
        "202608/acme/dashboard/timescaling/logs/",
        "202608/logs/acme_202608_dashboard_replica_0/",
        "202608/logs/acme_202608_dashboard/",
    ]
    assert page.lines == ("legacy-ok",)


async def test_falls_back_to_legacy_replica_prefix_when_rust_empty() -> None:
    # Given: only the Go replica log layout has files
    key = (
        "202608/logs/acme_202608_dashboard_replica_0/"
        "j-ABC/node/i-1/bootstrap-actions/1/stderr.gz"
    )
    store = FakeStore(objects={key: compress(b"glacier\n")})

    # When: the first tail page is fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: rust then the replica prefix, and the bootstrap stderr line is returned
    assert store.prefixes == [
        "202608/acme/dashboard/timescaling/logs/",
        "202608/logs/acme_202608_dashboard_replica_0/",
    ]
    assert page.lines == ("glacier",)


async def test_prefers_stderr_in_each_step_dir() -> None:
    # Given: one step with stderr and stdout
    base = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1"
    store = FakeStore(
        objects={
            f"{base}/stdout": b"out\n",
            f"{base}/stderr": b"err\n",
        }
    )

    # When: logs are fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: only stderr from that step is used
    assert page.lines == ("err",)


async def test_gunzips_preferred_file() -> None:
    # Given: a gzipped stderr file
    key = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr.gz"
    store = FakeStore(objects={key: compress(b"zipped\n")})

    # When: logs are fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: the decoded line is returned
    assert page.lines == ("zipped",)


async def test_tail_keeps_last_hundred_chronological_lines() -> None:
    # Given: 120 stderr lines
    store = _line_store(120)

    # When: the first tail page is fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: the last 100 lines are kept in order
    assert page.lines == tuple(f"line-{index}" for index in range(20, 120))
    assert page.truncated is True


async def test_full_keeps_lines_from_the_start() -> None:
    # Given: 120 stderr lines
    store = _line_store(120)

    # When: a full page is fetched
    page = await fetch_timescaling_logs(store, _request(full=True))

    # Then: lines start at the beginning, not the tail
    assert page.lines == tuple(f"line-{index}" for index in range(120))
    assert page.truncated is False


async def test_empty_hits_returns_note() -> None:
    # Given: no objects for the run
    store = FakeStore(objects={})

    # When: logs are fetched
    page = await fetch_timescaling_logs(store, _request())

    # Then: lines are empty and a note explains why
    assert page.lines == ()
    assert page.truncated is False
    assert page.note == (
        "No lines. Timescaling model logs stay empty until the model writes them."
    )
    assert page.cursor != ""


@pytest.mark.parametrize("client", ["  ", "a/b"])
async def test_bad_client_raises(client: str) -> None:
    # Given: a blank or slash-containing client
    store = FakeStore(objects={})

    # When: logs are fetched
    # Then: the client is rejected
    with pytest.raises(EmptyQueryError) as caught:
        _ = await fetch_timescaling_logs(store, _request(client=client))
    assert caught.value.field == "client"


async def test_invalid_cursor_raises() -> None:
    # Given: a cursor that is not valid
    store = FakeStore(objects={})

    # When: logs are fetched with that cursor
    # Then: the call fails as an invalid cursor
    with pytest.raises(InvalidCursorError):
        _ = await fetch_timescaling_logs(store, _request(cursor="not-a-cursor"))


async def test_mismatched_cursor_raises() -> None:
    # Given: a valid cursor for one run
    key = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr"
    store = FakeStore(objects={key: b"ok\n"})
    first = await fetch_timescaling_logs(store, _request())

    # When: the same cursor is reused with a different client
    # Then: the call fails as an invalid cursor
    with pytest.raises(InvalidCursorError):
        _ = await fetch_timescaling_logs(
            store, _request(client="other", cursor=first.cursor)
        )
