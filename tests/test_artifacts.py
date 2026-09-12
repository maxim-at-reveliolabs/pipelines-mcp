from dataclasses import dataclass, field

import pytest

from pipelines_mcp.artifacts import list_artifacts
from pipelines_mcp.errors import SettingsError
from pipelines_mcp.models import ArtifactFolder, ArtifactListing


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
