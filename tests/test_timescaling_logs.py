from dataclasses import dataclass, field
from gzip import compress

import pytest

from pipelines_mcp.errors import DomainError
from pipelines_mcp.timescaling_logs import (
    TimescalingLogRequest,
    fetch_timescaling_logs,
)


@dataclass(slots=True)
class FakeStore:
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


def test_lists_rust_prefix() -> None:
    key = (
        "202608/acme/dashboard/timescaling/logs/timescaling-acme-dashboard-202608"
        "/j-ABC/steps/s-1/stderr"
    )
    store = FakeStore(objects={key: b"gpu-ok\n"})
    page = fetch_timescaling_logs(store, _request())
    assert store.prefixes[0] == "202608/acme/dashboard/timescaling/logs/"
    assert page.lines == ("gpu-ok",)
    assert page.truncated is False
    assert page.note is None


def test_falls_back_to_legacy_prefix_when_rust_empty() -> None:
    key = "202608/logs/acme_202608_dashboard/j-ABC/steps/s-1/stderr"
    store = FakeStore(objects={key: b"legacy-ok\n"})
    page = fetch_timescaling_logs(store, _request())
    assert store.prefixes == [
        "202608/acme/dashboard/timescaling/logs/",
        "202608/logs/acme_202608_dashboard_replica_0/",
        "202608/logs/acme_202608_dashboard/",
    ]
    assert page.lines == ("legacy-ok",)


def test_falls_back_to_legacy_replica_prefix_when_rust_empty() -> None:
    key = (
        "202608/logs/acme_202608_dashboard_replica_0/"
        "j-ABC/node/i-1/bootstrap-actions/1/stderr.gz"
    )
    store = FakeStore(objects={key: compress(b"glacier\n")})
    page = fetch_timescaling_logs(store, _request())
    assert store.prefixes == [
        "202608/acme/dashboard/timescaling/logs/",
        "202608/logs/acme_202608_dashboard_replica_0/",
    ]
    assert page.lines == ("glacier",)


def test_prefers_stderr_in_each_step_dir() -> None:
    base = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1"
    store = FakeStore(
        objects={
            f"{base}/stdout": b"out\n",
            f"{base}/stderr": b"err\n",
        }
    )
    page = fetch_timescaling_logs(store, _request())
    assert page.lines == ("err",)


def test_gunzips_preferred_file() -> None:
    key = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr.gz"
    store = FakeStore(objects={key: compress(b"zipped\n")})
    page = fetch_timescaling_logs(store, _request())
    assert page.lines == ("zipped",)


def test_tail_keeps_last_hundred_chronological_lines() -> None:
    store = _line_store(120)
    page = fetch_timescaling_logs(store, _request())
    assert page.lines == tuple(f"line-{index}" for index in range(20, 120))
    assert page.truncated is True


def test_tail_follow_up_empty_when_no_new_lines() -> None:
    store = _line_store(120)
    first = fetch_timescaling_logs(store, _request())
    second = fetch_timescaling_logs(store, _request(cursor=first.cursor))
    assert second.lines == ()
    assert second.truncated is False


def test_full_keeps_lines_from_the_start() -> None:
    store = _line_store(120)
    page = fetch_timescaling_logs(store, _request(full=True))
    assert page.lines == tuple(f"line-{index}" for index in range(120))
    assert page.truncated is False


def test_empty_hits_returns_note() -> None:
    store = FakeStore(objects={})
    page = fetch_timescaling_logs(store, _request())
    assert page.lines == ()
    assert page.truncated is False
    assert page.note == (
        "No lines. Timescaling model logs stay empty until the model writes them."
    )
    assert page.cursor != ""


@pytest.mark.parametrize("client", ["  ", "a/b"])
def test_bad_client_raises(client: str) -> None:
    store = FakeStore(objects={})
    with pytest.raises(DomainError, match="empty client"):
        _ = fetch_timescaling_logs(store, _request(client=client))


def test_invalid_cursor_raises() -> None:
    store = FakeStore(objects={})
    with pytest.raises(DomainError, match="invalid cursor"):
        _ = fetch_timescaling_logs(store, _request(cursor="not-a-cursor"))


def test_mismatched_cursor_raises() -> None:
    key = "202608/acme/dashboard/timescaling/logs/c/j-1/steps/s-1/stderr"
    store = FakeStore(objects={key: b"ok\n"})
    first = fetch_timescaling_logs(store, _request())
    with pytest.raises(DomainError, match="invalid cursor"):
        _ = fetch_timescaling_logs(
            store, _request(client="other", cursor=first.cursor)
        )
