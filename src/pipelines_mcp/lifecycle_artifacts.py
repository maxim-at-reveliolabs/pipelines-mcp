"""Lifecycle unload folders, files, jsonl and user_pdfs names, and short text heads."""

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

JSONL_FOLDER = "timescaling_v4"
USER_PDFS_FOLDER = "user_pdfs"


def _unloads_prefix(batchtime: str, request_id: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/rust-unloads/"
        f"{path_segment(request_id, 'request_id')}/"
    )


def _globals_prefix(batchtime: str, folder: str) -> str:
    return (
        f"{path_segment(batchtime, 'batchtime')}/"
        "input_pipelines/main/final/globals_rs/"
        f"{path_segment(folder, 'folder')}/"
    )


def _child_names(store: ObjectStore, prefix: str) -> tuple[str, ...]:
    names: set[str] = set()
    for key in store.list_keys(prefix):
        rest = key.removeprefix(prefix)
        if rest == "":
            continue
        names.add(rest.partition("/")[0])
    return tuple(sorted(names))


def list_lifecycle_artifacts(
    store: ObjectStore,
    batchtime: str,
    request_id: str,
) -> LifecycleArtifacts:
    """List unload folders and shared jsonl and user_pdfs names."""
    unloads_prefix = _unloads_prefix(batchtime, request_id)
    jsonl_prefix = _globals_prefix(batchtime, JSONL_FOLDER)
    user_pdfs_prefix = _globals_prefix(batchtime, USER_PDFS_FOLDER)
    return LifecycleArtifacts(
        unloads_prefix=unloads_prefix,
        folders=child_folders(store, unloads_prefix),
        jsonl_prefix=jsonl_prefix,
        jsonl_names=_child_names(store, jsonl_prefix),
        user_pdfs_prefix=user_pdfs_prefix,
        user_pdfs_names=_child_names(store, user_pdfs_prefix),
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


def get_lifecycle_globals_text(
    store: ObjectStore,
    batchtime: str,
    folder: str,
    key: str,
) -> ArtifactText:
    """Return a short redacted text head of one globals_rs object."""
    return object_text(store, _globals_prefix(batchtime, folder), key)
