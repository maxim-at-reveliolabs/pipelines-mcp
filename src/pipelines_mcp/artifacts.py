"""Artifact folders, keys, and short text heads from an injected object store."""

from __future__ import annotations

import gzip
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from pipelines_mcp.errors import DomainError
from pipelines_mcp.logs import nonempty, path_segment
from pipelines_mcp.models import (
    ArtifactFiles,
    ArtifactFolder,
    ArtifactListing,
    ArtifactText,
)
from pipelines_mcp.redact import redact_text

if TYPE_CHECKING:
    from pipelines_mcp.object_store import ObjectStore

_PARQUET_MAGIC: Final = b"PAR1"
_TEXT_CAP: Final = 32768


def _artifact_prefix(client: str, batchtime: str, comptype: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/"
        f"{path_segment(client, 'client')}/"
        f"{path_segment(comptype, 'comptype')}/"
    )


def child_folders(store: ObjectStore, prefix: str) -> tuple[ArtifactFolder, ...]:
    """Immediate child folders under prefix."""
    counts: dict[str, int] = {}
    for key in store.list_keys(prefix):
        rest = key.removeprefix(prefix)
        name = rest.partition("/")[0] if "/" in rest else ""
        counts[name] = counts.get(name, 0) + 1
    return tuple(
        ArtifactFolder(name=name, object_count=counts[name]) for name in sorted(counts)
    )


def folder_prefix(prefix: str, folder: str) -> str:
    """Join prefix with one validated folder name."""
    return f"{prefix}{path_segment(folder, 'folder')}/"


def child_keys(store: ObjectStore, prefix: str) -> tuple[str, ...]:
    """Object keys under prefix, relative to that prefix."""
    return tuple(
        sorted(
            key.removeprefix(prefix)
            for key in store.list_keys(prefix)
            if key.startswith(prefix) and key != prefix
        )
    )


def list_artifacts(
    store: ObjectStore,
    client: str,
    batchtime: str,
    comptype: str,
) -> ArtifactListing:
    """List immediate child folders under one rust job prefix."""
    prefix = _artifact_prefix(client, batchtime, comptype)
    return ArtifactListing(prefix=prefix, folders=child_folders(store, prefix))


def list_artifact_files(
    store: ObjectStore,
    client: str,
    batchtime: str,
    comptype: str,
    folder: str,
) -> ArtifactFiles:
    """List object keys under one rust artifact folder."""
    prefix = folder_prefix(_artifact_prefix(client, batchtime, comptype), folder)
    return ArtifactFiles(prefix=prefix, keys=child_keys(store, prefix))


@dataclass(frozen=True, slots=True)
class ArtifactTextRequest:
    """One rust artifact object to read as text."""

    client: str
    batchtime: str
    comptype: str
    folder: str
    key: str


def object_text(store: ObjectStore, prefix: str, key: str) -> ArtifactText:
    """Return a short redacted text head of one object under prefix."""
    stripped = nonempty(key, "key")
    if stripped.startswith("/") or ".." in stripped or "" in stripped.split("/"):
        raise DomainError(reason="empty key")
    if stripped.lower().endswith(".parquet"):
        raise DomainError(reason="parquet is not text")
    raw = store.get_bytes(
        f"{prefix}{stripped}",
        max_bytes=None if stripped.endswith(".gz") else _TEXT_CAP,
    )
    body = raw
    if stripped.endswith(".gz"):
        try:
            body = gzip.decompress(raw)
        except OSError as exc:
            raise DomainError(reason="object store unreachable") from exc
    if body.startswith(_PARQUET_MAGIC):
        raise DomainError(reason="parquet is not text")
    text = redact_text(body.decode("utf-8", errors="replace"))
    encoded = text.encode("utf-8")
    if len(encoded) > _TEXT_CAP:
        text = encoded[:_TEXT_CAP].decode("utf-8", errors="replace")
    return ArtifactText(prefix=prefix, key=stripped, text=text)


def get_artifact_text(store: ObjectStore, request: ArtifactTextRequest) -> ArtifactText:
    """Return a short redacted text head of one rust artifact object."""
    return object_text(
        store,
        folder_prefix(
            _artifact_prefix(
                request.client, request.batchtime, request.comptype
            ),
            request.folder,
        ),
        request.key,
    )
