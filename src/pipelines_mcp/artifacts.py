"""Artifact folders and keys from an injected object store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pipelines_mcp.logs import token
from pipelines_mcp.models import ArtifactFiles, ArtifactFolder, ArtifactListing

if TYPE_CHECKING:
    from pipelines_mcp.object_store import LogStore


def _job_prefix(client: str, batchtime: str, comptype: str) -> str:
    return (
        f"{token(batchtime, 'batchtime')}/"
        f"{token(client, 'client')}/"
        f"{token(comptype, 'comptype')}/"
    )


def child_folders(store: LogStore, prefix: str) -> tuple[ArtifactFolder, ...]:
    """Immediate child folders under prefix."""
    counts: dict[str, int] = {}
    for key in store.list_keys(prefix):
        rest = key.removeprefix(prefix)
        name = rest.partition("/")[0] if "/" in rest else ""
        counts[name] = counts.get(name, 0) + 1
    return tuple(
        ArtifactFolder(name=name, object_count=counts[name]) for name in sorted(counts)
    )


def list_artifacts(
    store: LogStore,
    client: str,
    batchtime: str,
    comptype: str,
) -> ArtifactListing:
    """List immediate child folders under one rust job prefix."""
    prefix = _job_prefix(client, batchtime, comptype)
    return ArtifactListing(prefix=prefix, folders=child_folders(store, prefix))


def list_artifact_files(
    store: LogStore,
    client: str,
    batchtime: str,
    comptype: str,
    folder: str,
) -> ArtifactFiles:
    """List object keys under one rust artifact folder."""
    prefix = f"{_job_prefix(client, batchtime, comptype)}{token(folder, 'folder')}/"
    keys = tuple(
        sorted(
            key.removeprefix(prefix)
            for key in store.list_keys(prefix)
            if key.startswith(prefix) and key != prefix
        )
    )
    return ArtifactFiles(prefix=prefix, keys=keys)
