"""Lifecycle unload folders, files, jsonl names, and short text heads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pipelines_mcp.artifacts import (
    child_folders,
    child_keys,
    folder_prefix,
    object_text,
)
from pipelines_mcp.logs import path_segment
from pipelines_mcp.models import ArtifactFiles, ArtifactText, LifecycleArtifacts

if TYPE_CHECKING:
    from pipelines_mcp.object_store import ObjectStore


def _unloads_prefix(batchtime: str, request_id: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/rust-unloads/"
        f"{path_segment(request_id, 'request_id')}/"
    )


def _jsonl_prefix(batchtime: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/"
        "input_pipelines/main/final/globals_rs/timescaling_v4/"
    )


def list_lifecycle_artifacts(
    store: ObjectStore,
    batchtime: str,
    request_id: str,
) -> LifecycleArtifacts:
    """List unload folders and shared jsonl names for one lifecycle run."""
    unloads_prefix = _unloads_prefix(batchtime, request_id)
    jsonl_prefix = _jsonl_prefix(batchtime)
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
    prefix = folder_prefix(_unloads_prefix(batchtime, request_id), folder)
    return ArtifactFiles(prefix=prefix, keys=child_keys(store, prefix))


@dataclass(frozen=True, slots=True)
class LifecycleArtifactTextRequest:
    """One lifecycle unload object to read as text."""

    batchtime: str
    request_id: str
    folder: str
    key: str


def get_lifecycle_artifact_text(
    store: ObjectStore,
    request: LifecycleArtifactTextRequest,
) -> ArtifactText:
    """Return a short redacted text head of one lifecycle unload object."""
    return object_text(
        store,
        folder_prefix(
            _unloads_prefix(request.batchtime, request.request_id),
            request.folder,
        ),
        request.key,
    )


def get_lifecycle_jsonl_text(
    store: ObjectStore,
    batchtime: str,
    key: str,
) -> ArtifactText:
    """Return a short redacted text head of one shared jsonl object."""
    return object_text(store, _jsonl_prefix(batchtime), key)
