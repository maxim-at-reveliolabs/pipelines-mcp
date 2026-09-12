"""Artifact folder listing from an injected object store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pipelines_mcp.logs import token
from pipelines_mcp.models import ArtifactFolder, ArtifactListing

if TYPE_CHECKING:
    from pipelines_mcp.object_store import LogStore


def list_artifacts(
    store: LogStore,
    client: str,
    batchtime: str,
    comptype: str,
) -> ArtifactListing:
    """List immediate child folders under one rust job prefix."""
    prefix = (
        f"{token(batchtime, 'batchtime')}/"
        f"{token(client, 'client')}/"
        f"{token(comptype, 'comptype')}/"
    )
    counts: dict[str, int] = {}
    for key in store.list_keys(prefix):
        rest = key.removeprefix(prefix)
        name = rest.partition("/")[0] if "/" in rest else ""
        counts[name] = counts.get(name, 0) + 1
    folders = tuple(
        ArtifactFolder(name=name, object_count=counts[name]) for name in sorted(counts)
    )
    return ArtifactListing(prefix=prefix, folders=folders)
