from dataclasses import dataclass, field
from gzip import compress

import pytest

from pipelines_mcp.artifacts import (
    ArtifactTextRequest,
    get_artifact_text,
    list_artifact_files,
    list_artifacts,
)
from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import (
    ArtifactFiles,
    ArtifactFolder,
    ArtifactListing,
    ArtifactText,
)


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
def test_bad_path_segment_raises(
    client: str, batchtime: str, comptype: str, field: str
) -> None:
    with pytest.raises(DomainError, match=f"empty {field}"):
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
def test_bad_file_path_segment_raises(
    client: str, batchtime: str, comptype: str, folder: str, field: str
) -> None:
    with pytest.raises(DomainError, match=f"empty {field}"):
        _ = list_artifact_files(
            FakeStore(keys=()), client, batchtime, comptype, folder
        )


@dataclass(slots=True)
class BytesStore:
    objects: dict[str, bytes]
    reads: list[str] = field(default_factory=list)

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        return tuple(key for key in self.objects if key.startswith(prefix))

    def get_bytes(self, key: str) -> bytes:
        self.reads.append(key)
        body = self.objects.get(key)
        if body is None:
            raise DomainError(reason="object not found")
        return body


def _text_request(*, key: str = "model_input/part-0.json") -> ArtifactTextRequest:
    return ArtifactTextRequest(
        client="acme",
        batchtime="202608",
        comptype="dashboard",
        folder="timescaling",
        key=key,
    )


def test_reads_json_text_for_relative_key_with_slashes() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/model_input/part-0.json": (
                b'{"ok": true}\n'
            ),
        }
    )
    result = get_artifact_text(store, _text_request())
    assert result == ArtifactText(
        prefix="202608/acme/dashboard/timescaling/",
        key="model_input/part-0.json",
        text='{"ok": true}\n',
    )
    assert store.reads == [
        "202608/acme/dashboard/timescaling/model_input/part-0.json",
    ]


def test_redacts_credential_shaped_text() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/model_input/part-0.json": (
                b'{"key": "AKIAIOSFODNN7EXAMPLE"}'
            ),
        }
    )
    result = get_artifact_text(store, _text_request())
    assert result.text == '{"key": "[redacted]"}'


def test_gunzips_when_key_ends_with_gz() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/logs/stderr.gz": compress(b"gpu-ok\n"),
        }
    )
    result = get_artifact_text(store, _text_request(key="logs/stderr.gz"))
    assert result.text == "gpu-ok\n"


def test_caps_text_to_log_byte_cap() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/logs/stderr": ("x" * 40000).encode(),
        }
    )
    result = get_artifact_text(store, _text_request(key="logs/stderr"))
    assert result.text == "x" * 32768


def test_decodes_utf8_with_replace() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/logs/stderr": b"ok\xff",
        }
    )
    result = get_artifact_text(store, _text_request(key="logs/stderr"))
    assert result.text == "ok\ufffd"


def test_rejects_parquet_extension_without_read() -> None:
    store = BytesStore(
        objects={
            "202608/acme/dashboard/timescaling/model_input/part-0.parquet": (
                b"PAR1secret-body"
            ),
        }
    )
    with pytest.raises(DomainError, match="parquet is not text"):
        _ = get_artifact_text(
            store, _text_request(key="model_input/part-0.parquet")
        )
    assert store.reads == []


def test_rejects_parquet_magic_without_returning_body() -> None:
    body = b"PAR1" + b"secret-bytes-not-for-output"
    store = BytesStore(
        objects={"202608/acme/dashboard/timescaling/model_input/part-0.bin": body}
    )
    with pytest.raises(DomainError, match="parquet is not text") as caught:
        _ = get_artifact_text(store, _text_request(key="model_input/part-0.bin"))
    assert store.reads == [
        "202608/acme/dashboard/timescaling/model_input/part-0.bin",
    ]
    assert "secret-bytes-not-for-output" not in str(caught.value)


def test_missing_object_raises_domain_error() -> None:
    with pytest.raises(DomainError, match="object not found"):
        _ = get_artifact_text(BytesStore(objects={}), _text_request())


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
def test_bad_text_path_segment_raises(
    client: str, batchtime: str, comptype: str, folder: str, field: str
) -> None:
    with pytest.raises(DomainError, match=f"empty {field}"):
        _ = get_artifact_text(
            BytesStore(objects={}),
            ArtifactTextRequest(
                client=client,
                batchtime=batchtime,
                comptype=comptype,
                folder=folder,
                key="model_input/part-0.json",
            ),
        )


@pytest.mark.parametrize(
    "key",
    ["", "  ", "/model_input/part-0.json", "foo/../bar.json", "foo//bar.json", "foo/"],
)
def test_bad_key_raises(key: str) -> None:
    with pytest.raises(DomainError, match="empty key"):
        _ = get_artifact_text(BytesStore(objects={}), _text_request(key=key))
