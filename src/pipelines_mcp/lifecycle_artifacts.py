"""Lifecycle unload folder and jsonl name listing from an injected object store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pipelines_mcp.artifacts import child_folders
from pipelines_mcp.logs import token
from pipelines_mcp.models import LifecycleArtifacts

if TYPE_CHECKING:
    from pipelines_mcp.object_store import LogStore


def list_lifecycle_artifacts(
    store: LogStore,
    batchtime: str,
    request_id: str,
) -> LifecycleArtifacts:
    """List unload folders and shared jsonl names for one lifecycle run."""
    batch = token(batchtime, "batchtime")
    req = token(request_id, "request_id")
    unloads_prefix = f"{batch}/rust-unloads/{req}/"
    jsonl_prefix = f"{batch}/input_pipelines/main/final/globals_rs/timescaling_v4/"
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
