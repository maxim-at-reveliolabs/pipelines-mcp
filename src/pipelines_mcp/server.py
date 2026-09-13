"""MCP server with the read-only pipeline tools. Does not call run()."""

from collections.abc import Awaitable, Callable
from functools import wraps
from inspect import cleandoc

from anyio.to_thread import run_sync
from mcp.server import MCPServer

from pipelines_mcp.artifacts import (
    ArtifactTextRequest,
    get_artifact_text,
    list_artifact_files,
    list_artifacts,
)
from pipelines_mcp.lifecycle_artifacts import (
    JSONL_FOLDER,
    USER_PDFS_FOLDER,
    LifecycleArtifactTextRequest,
    get_lifecycle_artifact_text,
    get_lifecycle_globals_text,
    list_lifecycle_artifact_files,
    list_lifecycle_artifacts,
)
from pipelines_mcp.logs import LogRequest, fetch_logs, nonempty
from pipelines_mcp.models import (
    ArtifactFiles,
    ArtifactListing,
    ArtifactText,
    Job,
    LifecycleArtifacts,
    LogKind,
    LogPage,
    ObjectConfig,
    PipelineConfigCheck,
    PipelineImages,
    PipelineQueueItem,
    PipelineStart,
    PipelineStatus,
    PipelineStepRuns,
    Pod,
    job_name,
)
from pipelines_mcp.pipeline_config import check_pipeline_config
from pipelines_mcp.pipeline_start import parse_start
from pipelines_mcp.pipeline_step_status import parse_step_status
from pipelines_mcp.server_app import (
    get_images_store,
    get_k8s,
    get_logs_client,
    get_object_store,
    get_queue_store,
    get_status_store,
    tool_boundary,
)
from pipelines_mcp.timescaling_logs import TimescalingLogRequest, fetch_timescaling_logs

mcp = MCPServer(
    "pipelines-mcp",
    description=(
        "Read-only debug for pipeline jobs, pods, configs, and logs. Use a "
        "request_id UUID, not a GitHub or Jenkins run id."
    ),
    instructions="""
This MCP debugs a pipeline by request_id UUID. A GitHub Actions or Jenkins
URL/run id is not a request_id. Passing it as request_id will fail or time out.

If the user gives a GitHub Actions or Jenkins URL or run id: fetch that CI log
first (outside this MCP), extract the request_id UUID, then call
get_pipeline_log. Never call list_pipeline_jobs for that. Typical UUID lines:
Fast pipeline <uuid> ... failed; StartMultipartPipeline; pipeline-id;
pipeline id.

If the user already gave a request_id UUID, call get_pipeline_status
next. After get_pipeline_status, if the rust job started, call
get_pipeline_start, then get_pipeline_images. Compare
get_pipeline_start.image_name (for example pipelines-rust:v1.1.1)
with get_pipeline_images.rust.version (for example v1.1.16). If they
differ, say both versions in the first failure summary: "This run
used rust image v1.1.1. The current rust image is v1.1.16." If they
match, mention the image only if the user asks. Do not treat an old
image as the cause unless logs or config check show that. Still
report the version gap. Keep the real failure reason first.
get_pipeline_images does not start a pipeline. lifecycle may be
null. Do not invent a current timescaling image. If the job JSON has
timescaling.image_override, you may mention that tag, but do not
call it "latest" unless a tool gives a current timescaling version.
Then get_pipeline_step_status to see which step failed.
get_pipeline_step_status reads service logs, not the status API.
Overall status stays on get_pipeline_status. Then get_pipeline_log
and get_pipeline_service_log.
When get_pipeline_status is pending and there is no cluster job, call
get_pipeline_queue to see if the run is waiting in the queue.
When a run has many steps, use get_pipeline_step_log instead of mixed
get_pipeline_log. Search with search_pipeline_step_log instead of mixed
search_pipeline_log.
search_pipeline_log and search_pipeline_service_log find matching lines
in those logs. query is required.
get_pipeline_start returns the keys from the service line that starts the
job. arguments stays the original JSON string. Call validate_pipeline_config
only on rust job JSON (it has dataset). Do not call it on lifecycle unload
JSON (no dataset). It does not start a pipeline.
If the worker says the timescaling cluster failed: get_pipeline_job_config
(step_index and replica are usually 0) for client, batchtime, and comptype,
then get_timescaling_log.
list_pipeline_artifacts lists artifact folders for one rust job after the
pod is gone. Pass client, batchtime, and comptype from
get_pipeline_job_config.
After list_pipeline_artifacts, pass a folder name to
list_pipeline_artifact_files to see files inside it (model_input vs
model_output, parquet parts, log dirs).
get_pipeline_artifact_text reads a short head of one small json/jsonl/log
object. Pass the relative key from list_pipeline_artifact_files.
Parquet returns an error.
list_pipeline_lifecycle_artifacts lists unload folders and shared jsonl
names. Pass batchtime and the request_id UUID. Rust job artifacts stay
on list_pipeline_artifacts.
After list_pipeline_lifecycle_artifacts, pass a folder name to
list_pipeline_lifecycle_artifact_files to see files inside it.
get_pipeline_lifecycle_artifact_text reads a short head of one small
json/jsonl/log object. Pass the relative key from
list_pipeline_lifecycle_artifact_files. Parquet returns an error.
get_pipeline_lifecycle_jsonl_text reads a short head of one shared jsonl
name from list_pipeline_lifecycle_artifacts. Parquet returns an error.
get_pipeline_lifecycle_user_pdfs_text reads a short head of one user_pdfs
name from list_pipeline_lifecycle_artifacts. Parquet returns an error.
get_pipeline_job / list_pipeline_pods only if you need job or pod state.
If the job is gone from the cluster, that is expected after it finishes.
Use the log tools instead.

list_pipeline_jobs is only to browse currently running jobs when the user did
not give a request_id, GitHub URL, or Jenkins run.

Cluster tools may start AWS login. If the tool says login started, retry the
same request. Do not ask the human to open a URL unless the tool returned one.
""".strip(),
)


def _tool[**P, R](fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    @wraps(fn)
    async def bound(*args: P.args, **kwargs: P.kwargs) -> R:
        async with tool_boundary():
            return await fn(*args, **kwargs)

    description = None if fn.__doc__ is None else cleandoc(fn.__doc__)
    return mcp.tool(description=description)(bound)


def _job_name(request_id: str, step_index: int, replica: int) -> str:
    return job_name(nonempty(request_id, "request_id"), step_index, replica)


async def _es_log(
    match: str,
    log_kind: LogKind,
    cursor: str | None = None,
    *,
    full: bool = False,
    query: str | None = None,
) -> LogPage:
    return await fetch_logs(
        get_logs_client(),
        LogRequest(
            match=match,
            log_kind=log_kind,
            cursor=cursor,
            full=full,
            query=query,
        ),
    )


@_tool
async def list_pipeline_jobs(
    status: str | None = None,
    request_id: str | None = None,
    limit: int = 50,
) -> tuple[Job, ...]:
    """List currently running or recent jobs.

    Do not use this when the user gave a request_id UUID, GitHub URL, or Jenkins
    run; fetch the CI log if needed, then call get_pipeline_log. Optional
    filters: status (Active, Complete, Failed, Pending, or running),
    request_id, limit (default 50, max 100). Job names are
    pipelines-{request_id}-{step_index}-{replica}.
    """
    stripped_id = "" if request_id is None else request_id.strip()
    prefix = None if stripped_id == "" else f"pipelines-{stripped_id}"
    stripped_status = "" if status is None else status.strip()
    status_filter = (
        "Active" if stripped_status.lower() == "running" else stripped_status or None
    )
    return await run_sync(
        get_k8s().list_jobs,
        status_filter,
        prefix,
        min(100, max(1, limit)),
    )


@_tool
async def get_pipeline_status(request_id: str) -> PipelineStatus:
    """Pipeline service status and steps for a request_id UUID.

    Use this first when the user gave a request_id. Jobs may already be gone.
    Status is pending, running, error, complete, cancelled, or split. Steps
    are in pipeline service order as step_index. arguments is the step JSON
    string.
    """
    return await run_sync(
        get_status_store().get, nonempty(request_id, "request_id")
    )


@_tool
async def get_pipeline_queue() -> tuple[PipelineQueueItem, ...]:
    """Pending and waiting runs in pipeline service order.

    Use when get_pipeline_status is pending and there is no cluster job.
    Empty when nothing is waiting.
    """
    return await run_sync(get_queue_store().get)


@_tool
async def get_pipeline_images() -> PipelineImages:
    """Current rust and lifecycle image versions from pipeline service metadata.

    Does not start a pipeline.
    """
    return await run_sync(get_images_store().get)


@_tool
async def get_pipeline_step_status(request_id: str) -> PipelineStepRuns:
    """Per-step run status for a request_id UUID.

    Use after get_pipeline_status to see which step failed. This reads
    service logs, not the status API. Overall status stays on
    get_pipeline_status. Empty logs return no steps.
    """
    stripped = nonempty(request_id, "request_id")
    page = await fetch_logs(
        get_logs_client(),
        LogRequest(match=stripped, log_kind=LogKind.SERVICE, full=True),
    )
    return parse_step_status(stripped, page.lines)


@_tool
async def get_pipeline_job(request_id: str, step_index: int, replica: int) -> Job:
    """Get one job by request_id UUID, step_index, and replica (usually 0).

    Use only if you need job status. Start with get_pipeline_status. If the
    job is gone, use the log tools.
    """
    return await run_sync(get_k8s().get_job, _job_name(request_id, step_index, replica))


@_tool
async def list_pipeline_pods(
    request_id: str, step_index: int, replica: int
) -> tuple[Pod, ...]:
    """List live pods for one job.

    Empty if they are already gone. Use for pod state; use log tools for the
    failure reason.
    """
    return await run_sync(
        get_k8s().list_pods, _job_name(request_id, step_index, replica)
    )


@_tool
async def get_pipeline_job_config(
    request_id: str, step_index: int, replica: int
) -> ObjectConfig:
    """Full YAML for one job by request_id, step_index, and replica.

    Secrets are redacted. Use to read client, batchtime, and comptype before
    get_timescaling_log or list_pipeline_artifacts. If the job is gone, use
    the log tools.
    """
    return await run_sync(
        get_k8s().job_config, _job_name(request_id, step_index, replica)
    )


@_tool
async def get_pipeline_pod(pod_name: str) -> Pod:
    """Get one pod by pod_name from list_pipeline_pods."""
    return await run_sync(get_k8s().get_pod, nonempty(pod_name, "pod_name"))


@_tool
async def get_pipeline_pod_config(pod_name: str) -> ObjectConfig:
    """Full YAML for one pod by pod_name. Secrets are redacted."""
    return await run_sync(
        get_k8s().pod_config, nonempty(pod_name, "pod_name")
    )


@_tool
async def get_pipeline_log(
    request_id: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Worker logs for a request_id UUID.

    Empty until the worker is running. Default is the last 100 lines plus a
    cursor. Pass cursor to get only new lines. Reuse a cursor only on the
    same log tool and full setting. A mismatched cursor fails. full=true
    still caps size.
    """
    return await _es_log(request_id, LogKind.WORKER, cursor, full=full)


@_tool
async def get_pipeline_step_log(
    request_id: str,
    step_index: int,
    replica: int,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Worker logs for one job.

    Use this when a run has many steps. get_pipeline_log mixes every step.
    Same cursor rules as get_pipeline_log.
    """
    return await _es_log(
        _job_name(request_id, step_index, replica),
        LogKind.WORKER,
        cursor,
        full=full,
    )


@_tool
async def get_pipeline_service_log(
    request_id: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Service logs for the same request_id UUID.

    Use with worker logs. Same cursor rules as get_pipeline_log.
    """
    return await _es_log(request_id, LogKind.SERVICE, cursor, full=full)


@_tool
async def search_pipeline_log(
    request_id: str,
    query: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Find matching worker log lines for a request_id UUID.

    query is required and must not be empty. Same cursor rules as get_pipeline_log.
    """
    return await _es_log(request_id, LogKind.WORKER, cursor, full=full, query=query)


@_tool
async def search_pipeline_step_log(  # noqa: PLR0913  # MCP tool args
    request_id: str,
    step_index: int,
    replica: int,
    query: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Find matching worker log lines for one job.

    Use this when a run has many steps. search_pipeline_log mixes every step.
    query is required and must not be empty. Same cursor rules as get_pipeline_log.
    """
    return await _es_log(
        _job_name(request_id, step_index, replica),
        LogKind.WORKER,
        cursor,
        full=full,
        query=query,
    )


@_tool
async def search_pipeline_service_log(
    request_id: str,
    query: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Find matching service log lines for a request_id UUID.

    query is required and must not be empty. Same cursor rules as get_pipeline_log.
    """
    return await _es_log(request_id, LogKind.SERVICE, cursor, full=full, query=query)


@_tool
async def get_pipeline_start(request_id: str) -> PipelineStart:
    """Keys from the service line that starts the job for a request_id UUID.

    arguments stays the original JSON string. Use when the job is gone and you
    need those keys. Retry if the line is not there yet.
    """
    page = await fetch_logs(
        get_logs_client(),
        LogRequest(match=request_id, log_kind=LogKind.SERVICE, full=True),
        size=20,
    )
    return parse_start(page.lines)


@_tool
async def get_timescaling_log(
    client: str,
    batchtime: str,
    comptype: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Timescaling model logs.

    Use only when worker logs say the cluster failed. Pass client, batchtime,
    and comptype from get_pipeline_job_config. Empty until the model writes
    logs. Same cursor rules as get_pipeline_log.
    """
    return await run_sync(
        fetch_timescaling_logs,
        get_object_store(),
        TimescalingLogRequest(
            client=client,
            batchtime=batchtime,
            comptype=comptype,
            cursor=cursor,
            full=full,
        ),
    )


@_tool
async def list_pipeline_artifacts(
    client: str,
    batchtime: str,
    comptype: str,
) -> ArtifactListing:
    """Artifact folders for one rust job.

    Pass client, batchtime, and comptype from get_pipeline_job_config.
    Groups objects under {batchtime}/{client}/{comptype}/ by the first folder
    name. Typical folders: dataset_unique, timescaling, output, mappings_input.
    Does not return file contents.
    """
    return await run_sync(
        list_artifacts,
        get_object_store(),
        client,
        batchtime,
        comptype,
    )


@_tool
async def list_pipeline_artifact_files(
    client: str,
    batchtime: str,
    comptype: str,
    folder: str,
) -> ArtifactFiles:
    """Object keys inside one rust artifact folder.

    After list_pipeline_artifacts, pass a folder name to see files inside it
    (model_input vs model_output, parquet parts, log dirs). Keys are relative
    to {batchtime}/{client}/{comptype}/{folder}/. Does not return file contents.
    """
    return await run_sync(
        list_artifact_files,
        get_object_store(),
        client,
        batchtime,
        comptype,
        folder,
    )


@_tool
async def get_pipeline_artifact_text(
    client: str,
    batchtime: str,
    comptype: str,
    folder: str,
    key: str,
) -> ArtifactText:
    """Short text head of one rust artifact object.

    After list_pipeline_artifact_files, pass a relative key to read a short
    head of one small json, jsonl, or log object. Parquet returns an error.
    """
    return await run_sync(
        get_artifact_text,
        get_object_store(),
        ArtifactTextRequest(
            client=client,
            batchtime=batchtime,
            comptype=comptype,
            folder=folder,
            key=key,
        ),
    )


@_tool
async def list_pipeline_lifecycle_artifacts(
    batchtime: str,
    request_id: str,
) -> LifecycleArtifacts:
    """Unload folders and shared jsonl names for one lifecycle run.

    Pass batchtime and the request_id UUID. Lists first-level folders under
    {batchtime}/rust-unloads/{request_id}/. Typical folders: reference,
    dashboard-input, dashboard-timescaling, breakdowns, scraping-lags.
    Also lists names under the shared jsonl prefix
    {batchtime}/input_pipelines/main/final/globals_rs/timescaling_v4/.
    Typical names: company.jsonl, region.jsonl. Also lists names under
    {batchtime}/input_pipelines/main/final/globals_rs/user_pdfs/. Those
    prefixes are per batchtime, not per request. Rust job artifacts stay
    on list_pipeline_artifacts. Does not return file contents.
    """
    return await run_sync(
        list_lifecycle_artifacts,
        get_object_store(),
        batchtime,
        request_id,
    )


@_tool
async def list_pipeline_lifecycle_artifact_files(
    batchtime: str,
    request_id: str,
    folder: str,
) -> ArtifactFiles:
    """Object keys inside one lifecycle unload folder.

    After list_pipeline_lifecycle_artifacts, pass a folder name to see files
    inside it. Keys are relative to
    {batchtime}/rust-unloads/{request_id}/{folder}/. Does not return file
    contents.
    """
    return await run_sync(
        list_lifecycle_artifact_files,
        get_object_store(),
        batchtime,
        request_id,
        folder,
    )


@_tool
async def get_pipeline_lifecycle_artifact_text(
    batchtime: str,
    request_id: str,
    folder: str,
    key: str,
) -> ArtifactText:
    """Short text head of one lifecycle unload object.

    After list_pipeline_lifecycle_artifact_files, pass a relative key to
    read a short head of one small json, jsonl, or log object. Parquet
    returns an error.
    """
    return await run_sync(
        get_lifecycle_artifact_text,
        get_object_store(),
        LifecycleArtifactTextRequest(
            batchtime=batchtime,
            request_id=request_id,
            folder=folder,
            key=key,
        ),
    )


@_tool
async def get_pipeline_lifecycle_jsonl_text(
    batchtime: str,
    key: str,
) -> ArtifactText:
    """Short text head of one shared jsonl object.

    After list_pipeline_lifecycle_artifacts, pass a jsonl name such as
    company.jsonl. Parquet returns an error.
    """
    return await run_sync(
        get_lifecycle_globals_text,
        get_object_store(),
        batchtime,
        JSONL_FOLDER,
        key,
    )


@_tool
async def get_pipeline_lifecycle_user_pdfs_text(
    batchtime: str,
    key: str,
) -> ArtifactText:
    """Short text head of one user_pdfs object.

    After list_pipeline_lifecycle_artifacts, pass a user_pdfs name.
    Parquet returns an error.
    """
    return await run_sync(
        get_lifecycle_globals_text,
        get_object_store(),
        batchtime,
        USER_PDFS_FOLDER,
        key,
    )


@_tool
async def validate_pipeline_config(arguments: str) -> PipelineConfigCheck:
    """Check rust job arguments JSON.

    Pass arguments from a status step that has dataset. Do not pass lifecycle
    unload JSON. Does not start a pipeline. Bad JSON returns valid false and a
    short error.
    """
    return await run_sync(check_pipeline_config, nonempty(arguments, "arguments"))
