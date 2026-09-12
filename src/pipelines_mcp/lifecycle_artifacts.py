"""Lifecycle unload folders, files, and jsonl names from an injected object store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pipelines_mcp.artifacts import child_folders, child_keys
from pipelines_mcp.logs import path_segment
from pipelines_mcp.models import ArtifactFiles, LifecycleArtifacts

if TYPE_CHECKING:
    from pipelines_mcp.object_store import ObjectStore


def _unloads_prefix(batchtime: str, request_id: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/rust-unloads/"
        f"{path_segment(request_id, 'request_id')}/"
    )


def list_lifecycle_artifacts(
    store: ObjectStore,
    batchtime: str,
    request_id: str,
) -> LifecycleArtifacts:
    """List unload folders and shared jsonl names for one lifecycle run."""
    unloads_prefix = _unloads_prefix(batchtime, request_id)
    jsonl_prefix = (
        f"{path_segment(batchtime, 'batchtime')}/"
        "input_pipelines/main/final/globals_rs/timescaling_v4/"
    )
    folders = child_folders(store, unloads_prefix)
    names: set[str] = set()
    for key in store.list_keys(jsonl_prefix):
        rest = key.removeprefix(jsonl_prefix)
        if rest == "":
            continue
        names.add(rest.partition("/")[0])
    return LifecycleArtifacts(
        unloads_prefix=unloads_prefix,
        folders=folders,
        jsonl_prefix=jsonl_prefix,
        jsonl_names=tuple(sorted(names)),
    )


def list_lifecycle_artifact_files(
    store: ObjectStore,
    batchtime: str,
    request_id: str,
    folder: str,
) -> ArtifactFiles:
    """List object keys under one lifecycle unload folder."""
    prefix = (
        f"{_unloads_prefix(batchtime, request_id)}"
        f"{path_segment(folder, 'folder')}/"
    )
    return ArtifactFiles(prefix=prefix, keys=child_keys(store, prefix))
