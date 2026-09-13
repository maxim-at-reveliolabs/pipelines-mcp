from dataclasses import dataclass, field

import pytest

from pipelines_mcp.errors import DomainError
from pipelines_mcp.lifecycle_artifacts import (
    LifecycleArtifactTextRequest,
    get_lifecycle_artifact_text,
    get_lifecycle_jsonl_text,
    list_lifecycle_artifact_files,
    list_lifecycle_artifacts,
)
from pipelines_mcp.models import (
    ArtifactFiles,
    ArtifactFolder,
    ArtifactText,
    LifecycleArtifacts,
)

_REQUEST: str = "11111111-1111-1111-1111-111111111111"
_UNLOADS: str = f"202608/rust-unloads/{_REQUEST}/"
_JSONL: str = "202608/input_pipelines/main/final/globals_rs/timescaling_v4/"
_BAD_SEGMENTS: tuple[tuple[str, str, str, str], ...] = (
    ("  ", _REQUEST, "reference", "empty batchtime"),
    ("2026/08", _REQUEST, "reference", "invalid batchtime"),
    ("202608", "  ", "reference", "empty request_id"),
    ("202608", "aa/bb", "reference", "invalid request_id"),
    ("202608", _REQUEST, "  ", "empty folder"),
    ("202608", _REQUEST, "a/b", "invalid folder"),
)


@dataclass(slots=True)
class FakeStore:
    keys: tuple[str, ...]
    prefixes: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        self.prefixes.append(prefix)
        return tuple(key for key in self.keys if key.startswith(prefix))

    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
        _ = max_bytes
        raise AssertionError(key)


def test_lists_unload_folders_and_shared_jsonl_names() -> None:
    store = FakeStore(
        keys=(
            f"{_UNLOADS}reference/plan.json",
            f"{_UNLOADS}dashboard-input/a.parquet",
            f"{_UNLOADS}dashboard-input/b.parquet",
            f"{_UNLOADS}dashboard-timescaling/x",
            f"{_UNLOADS}breakdowns/y",
            f"{_UNLOADS}scraping-lags/z",
            "202608/rust-unloads/other-id/reference/nope",
            f"{_JSONL}company.jsonl",
            f"{_JSONL}region.jsonl",
            "202609/input_pipelines/main/final/globals_rs/timescaling_v4/country.jsonl",
        )
    )
    listing = list_lifecycle_artifacts(store, "202608", _REQUEST)
    assert listing == LifecycleArtifacts(
        unloads_prefix=_UNLOADS,
        folders=(
            ArtifactFolder(name="breakdowns", object_count=1),
            ArtifactFolder(name="dashboard-input", object_count=2),
            ArtifactFolder(name="dashboard-timescaling", object_count=1),
            ArtifactFolder(name="reference", object_count=1),
            ArtifactFolder(name="scraping-lags", object_count=1),
        ),
        jsonl_prefix=_JSONL,
        jsonl_names=("company.jsonl", "region.jsonl"),
    )
    assert store.prefixes == [_UNLOADS, _JSONL]


def test_empty_prefixes_return_empty_tuples() -> None:
    listing = list_lifecycle_artifacts(FakeStore(keys=()), "202608", _REQUEST)
    assert listing == LifecycleArtifacts(
        unloads_prefix=_UNLOADS,
        folders=(),
        jsonl_prefix=_JSONL,
        jsonl_names=(),
    )


@pytest.mark.parametrize(
    ("batchtime", "request_id", "reason"),
    [
        ("  ", _REQUEST, "empty batchtime"),
        ("2026/08", _REQUEST, "invalid batchtime"),
        ("202608", "  ", "empty request_id"),
        ("202608", "aa/bb", "invalid request_id"),
    ],
)
def test_bad_path_segment_raises(
    batchtime: str, request_id: str, reason: str
) -> None:
    with pytest.raises(DomainError, match=reason):
        _ = list_lifecycle_artifacts(FakeStore(keys=()), batchtime, request_id)


def test_lists_relative_keys_under_unload_folder() -> None:
    store = FakeStore(
        keys=(
            f"{_UNLOADS}reference/plan.json",
            f"{_UNLOADS}reference/nested/data.parquet",
            f"{_UNLOADS}dashboard-input/a.parquet",
        )
    )
    listing = list_lifecycle_artifact_files(store, "202608", _REQUEST, "reference")
    assert listing == ArtifactFiles(
        prefix=f"{_UNLOADS}reference/",
        keys=(
            "nested/data.parquet",
            "plan.json",
        ),
    )
    assert store.prefixes == [f"{_UNLOADS}reference/"]


def test_empty_unload_folder_returns_prefix_and_no_keys() -> None:
    listing = list_lifecycle_artifact_files(
        FakeStore(keys=()), "202608", _REQUEST, "reference"
    )
    assert listing == ArtifactFiles(
        prefix=f"{_UNLOADS}reference/",
        keys=(),
    )


def test_skips_the_unload_prefix_key() -> None:
    store = FakeStore(
        keys=(
            f"{_UNLOADS}reference/",
            f"{_UNLOADS}reference/plan.json",
        )
    )
    listing = list_lifecycle_artifact_files(store, "202608", _REQUEST, "reference")
    assert listing.keys == ("plan.json",)


@pytest.mark.parametrize(
    ("batchtime", "request_id", "folder", "reason"),
    _BAD_SEGMENTS,
)
def test_bad_file_path_segment_raises(
    batchtime: str, request_id: str, folder: str, reason: str
) -> None:
    with pytest.raises(DomainError, match=reason):
        _ = list_lifecycle_artifact_files(
            FakeStore(keys=()), batchtime, request_id, folder
        )


@dataclass(slots=True)
class BytesStore:
    objects: dict[str, bytes]
    reads: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(key for key in self.objects if key.startswith(prefix))

    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
        self.reads.append(key)
        body = self.objects.get(key)
        if body is None:
            raise DomainError(reason="object not found")
        if max_bytes is None:
            return body
        return body[:max_bytes]


def test_reads_json_text_for_relative_key_with_slashes() -> None:
    store = BytesStore(
        objects={f"{_UNLOADS}reference/nested/plan.json": b'{"ok": true}\n'}
    )
    result = get_lifecycle_artifact_text(
        store,
        LifecycleArtifactTextRequest(
            batchtime="202608",
            request_id=_REQUEST,
            folder="reference",
            key="nested/plan.json",
        ),
    )
    assert result == ArtifactText(
        prefix=f"{_UNLOADS}reference/",
        key="nested/plan.json",
        text='{"ok": true}\n',
    )
    assert store.reads == [f"{_UNLOADS}reference/nested/plan.json"]


@pytest.mark.parametrize(
    ("batchtime", "request_id", "folder", "reason"),
    _BAD_SEGMENTS,
)
def test_bad_text_path_segment_raises(
    batchtime: str, request_id: str, folder: str, reason: str
) -> None:
    with pytest.raises(DomainError, match=reason):
        _ = get_lifecycle_artifact_text(
            FakeStore(keys=()),
            LifecycleArtifactTextRequest(
                batchtime=batchtime,
                request_id=request_id,
                folder=folder,
                key="plan.json",
            ),
        )


def test_reads_shared_jsonl_text() -> None:
    store = BytesStore(objects={f"{_JSONL}company.jsonl": b'{"entity": "acme"}\n'})
    result = get_lifecycle_jsonl_text(store, "202608", "company.jsonl")
    assert result == ArtifactText(
        prefix=_JSONL,
        key="company.jsonl",
        text='{"entity": "acme"}\n',
    )
    assert store.reads == [f"{_JSONL}company.jsonl"]


@pytest.mark.parametrize(
    ("batchtime", "reason"),
    [("  ", "empty batchtime"), ("2026/08", "invalid batchtime")],
)
def test_bad_jsonl_batchtime_raises(batchtime: str, reason: str) -> None:
    with pytest.raises(DomainError, match=reason):
        _ = get_lifecycle_jsonl_text(BytesStore(objects={}), batchtime, "company.jsonl")
