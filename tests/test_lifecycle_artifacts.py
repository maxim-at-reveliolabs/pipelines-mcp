from dataclasses import dataclass, field

import pytest

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.lifecycle_artifacts import (
    list_lifecycle_artifact_files,
    list_lifecycle_artifacts,
)
from pipelines_mcp.models import ArtifactFiles, ArtifactFolder, LifecycleArtifacts

_REQUEST: str = "11111111-1111-1111-1111-111111111111"
_UNLOADS: str = f"202608/rust-unloads/{_REQUEST}/"
_JSONL: str = "202608/input_pipelines/main/final/globals_rs/timescaling_v4/"


@dataclass(slots=True)
class FakeStore:
    keys: tuple[str, ...]
    prefixes: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        self.prefixes.append(prefix)
        return tuple(key for key in self.keys if key.startswith(prefix))

    def get_bytes(self, key: str) -> bytes:
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
    ("batchtime", "request_id", "field"),
    [
        ("  ", _REQUEST, "batchtime"),
        ("2026/08", _REQUEST, "batchtime"),
        ("202608", "  ", "request_id"),
        ("202608", "aa/bb", "request_id"),
    ],
)
def test_bad_token_raises(batchtime: str, request_id: str, field: str) -> None:
    with pytest.raises(SettingsError, match=f"empty {field}"):
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
    ("batchtime", "request_id", "folder", "field"),
    [
        ("  ", _REQUEST, "reference", "batchtime"),
        ("2026/08", _REQUEST, "reference", "batchtime"),
        ("202608", "  ", "reference", "request_id"),
        ("202608", "aa/bb", "reference", "request_id"),
        ("202608", _REQUEST, "  ", "folder"),
        ("202608", _REQUEST, "a/b", "folder"),
    ],
)
def test_bad_file_token_raises(
    batchtime: str, request_id: str, folder: str, field: str
) -> None:
    with pytest.raises(SettingsError, match=f"empty {field}"):
        _ = list_lifecycle_artifact_files(
            FakeStore(keys=()), batchtime, request_id, folder
        )
