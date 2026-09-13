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
Debug one pipeline by request_id UUID. A GitHub or Jenkins URL/run id is not
a request_id; using it fails or times out.

CI URL/run: fetch that CI log outside this MCP, extract the UUID, then
get_pipeline_log. Never list_pipeline_jobs. UUID lines: Fast pipeline <uuid>
... failed; StartMultipartPipeline; pipeline-id; pipeline id.

UUID given: get_pipeline_status first.
If the rust job started, get_pipeline_start then get_pipeline_images.
Compare image_name (pipelines-rust:v1.1.1) to rust.version (v1.1.16). If they
differ, put both in the first failure summary: "This run used rust image
v1.1.1. The current rust image is v1.1.16." If they match, mention the image
only if asked. Report a version gap, but do not blame an old image unless logs
or config check show that. Keep the real failure first. get_pipeline_images
does not start a pipeline. lifecycle may be null. Do not invent a current
timescaling image. timescaling.image_override is a tag, not "latest", unless
a tool gives a current timescaling version.
Then get_pipeline_step_status (service logs, not the status API) to see which
step failed. Overall status stays on get_pipeline_status. Then worker and
service logs. Many steps: use step log and search tools, not the mixed ones.
Search query is required. Pending with no cluster job: get_pipeline_queue.
get_pipeline_start keeps arguments as the original JSON.
validate_pipeline_config only on rust job JSON (has dataset), never lifecycle
unload JSON. It does not start a pipeline.
Timescaling cluster failed: get_pipeline_job_config (step_index and replica
usually 0) for client, batchtime, and comptype, then get_timescaling_log.
Rust artifacts after the pod is gone: client, batchtime, and comptype from
job config, then list_pipeline_artifacts, list_pipeline_artifact_files, and
get_pipeline_artifact_text. Parquet is not text.
Lifecycle unloads: list_pipeline_lifecycle_artifacts with batchtime and
request_id, then the matching files and text tools. Shared jsonl and
user_pdfs names are per batchtime. Rust artifacts stay on
list_pipeline_artifacts.
Job and pod tools only for live cluster state. Gone after finish is expected;
use logs.
list_pipeline_jobs only to browse running jobs when there is no request_id or
CI run.

Cluster tools may start AWS login. If login started, retry the same request.
Do not ask the human to open a URL unless the tool returned one.
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
    """Browse running or recent jobs.

    Use only when there is no request_id or CI run. Optional: status (Active,
    Complete, Failed, Pending, or running), request_id, limit (1-100, default
    50). Names: pipelines-{request_id}-{step_index}-{replica}.
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
    """Service status and steps for a request_id UUID.

    Call first. Jobs may already be gone. Status: pending, running, error,
    complete, cancelled, or split. steps are in service order as step_index.
    arguments is the step JSON.
    """
    return await run_sync(
        get_status_store().get, nonempty(request_id, "request_id")
    )


@_tool
async def get_pipeline_queue() -> tuple[PipelineQueueItem, ...]:
    """Pending and waiting runs in service order.

    Use when status is pending and there is no cluster job. Empty if nothing
    is waiting.
    """
    return await run_sync(get_queue_store().get)


@_tool
async def get_pipeline_images() -> PipelineImages:
    """Current rust and lifecycle image versions.

    Does not start a pipeline. lifecycle may be null.
    """
    return await run_sync(get_images_store().get)


@_tool
async def get_pipeline_step_status(request_id: str) -> PipelineStepRuns:
    """Per-step run status from service logs.

    Use after get_pipeline_status to see which step failed. Overall status
    stays on get_pipeline_status. Empty logs return no steps.
    """
    stripped = nonempty(request_id, "request_id")
    page = await fetch_logs(
        get_logs_client(),
        LogRequest(match=stripped, log_kind=LogKind.SERVICE, full=True),
    )
    return parse_step_status(stripped, page.lines)


@_tool
async def get_pipeline_job(request_id: str, step_index: int, replica: int) -> Job:
    """One job by request_id, step_index, and replica (usually 0).

    Use only for live job status. If the job is gone, use logs.
    """
    return await run_sync(get_k8s().get_job, _job_name(request_id, step_index, replica))


@_tool
async def list_pipeline_pods(
    request_id: str, step_index: int, replica: int
) -> tuple[Pod, ...]:
    """Live pods for one job.

    Empty if they are already gone. Use logs for the failure reason.
    """
    return await run_sync(
        get_k8s().list_pods, _job_name(request_id, step_index, replica)
    )


@_tool
async def get_pipeline_job_config(
    request_id: str, step_index: int, replica: int
) -> ObjectConfig:
    """Redacted YAML for one job.

    Use for client, batchtime, and comptype before timescaling logs or rust
    artifacts. If the job is gone, use logs.
    """
    return await run_sync(
        get_k8s().job_config, _job_name(request_id, step_index, replica)
    )


@_tool
async def get_pipeline_pod(pod_name: str) -> Pod:
    """One pod by pod_name from list_pipeline_pods."""
    return await run_sync(get_k8s().get_pod, nonempty(pod_name, "pod_name"))


@_tool
async def get_pipeline_pod_config(pod_name: str) -> ObjectConfig:
    """Redacted YAML for one pod."""
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

    Empty until a pod is running. Default: last 100 lines plus a cursor. Pass
    cursor for new lines only. Reuse a cursor only on this tool and the same
    full setting. Mismatch fails. full=true still caps size.
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

    Use when a run has many steps. get_pipeline_log mixes every step. Same
    cursor rules as get_pipeline_log.
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
    """Service logs for a request_id UUID.

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
    """Matching worker log lines for a request_id UUID.

    query is required. Same cursor rules as get_pipeline_log.
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
    """Matching worker log lines for one job.

    Use when a run has many steps. search_pipeline_log mixes every step.
    query is required. Same cursor rules as get_pipeline_log.
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
    """Matching service log lines for a request_id UUID.

    query is required. Same cursor rules as get_pipeline_log.
    """
    return await _es_log(request_id, LogKind.SERVICE, cursor, full=full, query=query)


@_tool
async def get_pipeline_start(request_id: str) -> PipelineStart:
    """Keys from the service line that starts the job.

    arguments stays the original JSON. Use when the job is gone and you need
    those keys. Retry if the line is not there yet.
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

    Pass client, batchtime, and comptype from get_pipeline_job_config. Groups
    {batchtime}/{client}/{comptype}/ by first folder. Typical: dataset_unique,
    timescaling, output, mappings_input. No file contents.
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
    """Object keys in one rust artifact folder.

    After list_pipeline_artifacts, pass a folder name (model_input vs
    model_output, parquet parts, log dirs). Keys are relative to
    {batchtime}/{client}/{comptype}/{folder}/. No file contents.
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

    After list_pipeline_artifact_files, pass a relative key. json/jsonl/log
    only. Parquet returns an error.
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
    """Unload folders and shared jsonl and user_pdfs names.

    Pass batchtime and request_id. Folders under
    {batchtime}/rust-unloads/{request_id}/. Typical: reference,
    dashboard-input, dashboard-timescaling, breakdowns, scraping-lags. Also
    lists names under
    {batchtime}/input_pipelines/main/final/globals_rs/timescaling_v4/ and
    {batchtime}/input_pipelines/main/final/globals_rs/user_pdfs/. Typical
    jsonl: company.jsonl, region.jsonl. Those prefixes are per batchtime.
    Rust artifacts stay on list_pipeline_artifacts. No file contents.
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
    """Object keys in one lifecycle unload folder.

    After list_pipeline_lifecycle_artifacts, pass a folder name. Keys are
    relative to {batchtime}/rust-unloads/{request_id}/{folder}/. No file
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

    After list_pipeline_lifecycle_artifact_files, pass a relative key.
    json/jsonl/log only. Parquet returns an error.
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

    After list_pipeline_lifecycle_artifacts, pass a user_pdfs name. Parquet
    returns an error.
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

    Pass step arguments that include dataset. Not for lifecycle unload JSON.
    Does not start a pipeline. Bad JSON: valid false plus a short error.
    """
    return await run_sync(check_pipeline_config, nonempty(arguments, "arguments"))
