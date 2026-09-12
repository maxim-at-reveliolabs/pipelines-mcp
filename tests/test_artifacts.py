from dataclasses import dataclass, field

import pytest

from pipelines_mcp.artifacts import list_artifact_files, list_artifacts
from pipelines_mcp.errors import SettingsError
from pipelines_mcp.models import ArtifactFiles, ArtifactFolder, ArtifactListing


@dataclass(slots=True)
class FakeStore:
    keys: tuple[str, ...]
    prefixes: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        self.prefixes.append(prefix)
        return tuple(key for key in self.keys if key.startswith(prefix))

    def get_bytes(self, key: str) -> bytes:
        raise AssertionError(key)


def test_groups_keys_by_immediate_child_folder() -> None:
    store = FakeStore(
        keys=(
            "202608/acme/dashboard/dataset_unique/a.parquet",
            "202608/acme/dashboard/dataset_unique/b.parquet",
            "202608/acme/dashboard/timescaling/logs/stderr",
            "202608/acme/dashboard/output/result.json",
            "202608/acme/dashboard/mappings_input/m.csv",
            "202608/other/dashboard/output/nope",
        )
    )
    listing = list_artifacts(store, "acme", "202608", "dashboard")
    assert listing == ArtifactListing(
        prefix="202608/acme/dashboard/",
        folders=(
            ArtifactFolder(name="dataset_unique", object_count=2),
            ArtifactFolder(name="mappings_input", object_count=1),
            ArtifactFolder(name="output", object_count=1),
            ArtifactFolder(name="timescaling", object_count=1),
        ),
    )
    assert store.prefixes == ["202608/acme/dashboard/"]


def test_root_files_use_empty_folder_name() -> None:
    store = FakeStore(
        keys=(
            "202608/acme/dashboard/readme.txt",
            "202608/acme/dashboard/output/x",
        )
    )
    listing = list_artifacts(store, "acme", "202608", "dashboard")
    assert listing.folders == (
        ArtifactFolder(name="", object_count=1),
        ArtifactFolder(name="output", object_count=1),
    )


def test_empty_prefix_returns_no_folders() -> None:
    listing = list_artifacts(FakeStore(keys=()), "acme", "202608", "dashboard")
    assert listing == ArtifactListing(prefix="202608/acme/dashboard/", folders=())


@pytest.mark.parametrize(
    ("client", "batchtime", "comptype", "field"),
    [
        ("  ", "202608", "dashboard", "client"),
        ("a/b", "202608", "dashboard", "client"),
        ("acme", "x/y", "dashboard", "batchtime"),
        ("acme", "202608", "x/y", "comptype"),
    ],
)
def test_bad_token_raises(
    client: str, batchtime: str, comptype: str, field: str
) -> None:
    with pytest.raises(SettingsError, match=f"empty {field}"):
        _ = list_artifacts(FakeStore(keys=()), client, batchtime, comptype)


def test_lists_relative_keys_under_folder() -> None:
    store = FakeStore(
        keys=(
            "202608/acme/dashboard/timescaling/model_output/part-0.json",
            "202608/acme/dashboard/timescaling/logs/stderr",
            "202608/acme/dashboard/timescaling/model_input/part-0.json",
            "202608/acme/dashboard/output/result.json",
        )
    )
    listing = list_artifact_files(store, "acme", "202608", "dashboard", "timescaling")
    assert listing == ArtifactFiles(
        prefix="202608/acme/dashboard/timescaling/",
        keys=(
            "logs/stderr",
            "model_input/part-0.json",
            "model_output/part-0.json",
        ),
    )
    assert store.prefixes == ["202608/acme/dashboard/timescaling/"]


def test_empty_folder_returns_prefix_and_no_keys() -> None:
    listing = list_artifact_files(
        FakeStore(keys=()), "acme", "202608", "dashboard", "timescaling"
    )
    assert listing == ArtifactFiles(
        prefix="202608/acme/dashboard/timescaling/",
        keys=(),
    )


def test_skips_the_prefix_key() -> None:
    store = FakeStore(
        keys=(
            "202608/acme/dashboard/timescaling/",
            "202608/acme/dashboard/timescaling/model_input/part-0.json",
        )
    )
    listing = list_artifact_files(store, "acme", "202608", "dashboard", "timescaling")
    assert listing.keys == ("model_input/part-0.json",)


@pytest.mark.parametrize(
    ("client", "batchtime", "comptype", "folder", "field"),
    [
        ("  ", "202608", "dashboard", "timescaling", "client"),
        ("a/b", "202608", "dashboard", "timescaling", "client"),
        ("acme", "x/y", "dashboard", "timescaling", "batchtime"),
        ("acme", "202608", "x/y", "timescaling", "comptype"),
        ("acme", "202608", "dashboard", "  ", "folder"),
        ("acme", "202608", "dashboard", "a/b", "folder"),
    ],
)
def test_bad_file_token_raises(
    client: str, batchtime: str, comptype: str, folder: str, field: str
) -> None:
    with pytest.raises(SettingsError, match=f"empty {field}"):
        _ = list_artifact_files(
            FakeStore(keys=()), client, batchtime, comptype, folder
        )
