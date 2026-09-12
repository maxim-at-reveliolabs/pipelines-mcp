"""MCP server with the read-only pipeline tools. Does not call run()."""

from collections.abc import Awaitable, Callable
from functools import wraps
from inspect import cleandoc

from anyio.to_thread import run_sync
from mcp.server import MCPServer

from pipelines_mcp.logs import LogRequest, fetch_logs, nonempty
from pipelines_mcp.models import (
    Job,
    LogKind,
    LogPage,
    ObjectConfig,
    PipelineStart,
    Pod,
    job_name,
)
from pipelines_mcp.pipeline_start import parse_start
from pipelines_mcp.server_app import (
    get_k8s,
    get_logs_client,
    get_object_store,
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

If the user already gave a request_id UUID, call get_pipeline_log next. Also
get_pipeline_service_log for the service that launched the worker.
get_pipeline_start returns the keys from the service line that starts the
job. arguments stays the original JSON string. As part of troubleshooting,
parse arguments and check that this config JSON is valid and uses the
latest versions and schema, unless the run intentionally pinned something
else.
If the worker says the timescaling cluster failed: get_pipeline_job_config
(step_index and replica are usually 0) for client, batchtime, and comptype,
then get_timescaling_log.
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
    request_id: str,
    log_kind: LogKind,
    cursor: str | None = None,
    *,
    full: bool = False,
    size: int | None = None,
) -> LogPage:
    return await fetch_logs(
        get_logs_client(),
        LogRequest(
            request_id=nonempty(request_id, "request_id"),
            log_kind=log_kind,
            cursor=cursor,
            full=full,
        ),
        size=size,
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
async def get_pipeline_job(request_id: str, step_index: int, replica: int) -> Job:
    """Get one job by request_id UUID, step_index, and replica (usually 0).

    Use only if you need job status; start with get_pipeline_log. If the job is
    gone, use the log tools.
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
    get_timescaling_log. If the job is gone, use the log tools.
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
    cursor. Pass cursor to get only new lines. full=true still caps size.
    """
    return await _es_log(request_id, LogKind.PIPELINE, cursor, full=full)


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
async def get_pipeline_start(request_id: str) -> PipelineStart:
    """Keys from the service line that starts the job for a request_id UUID.

    arguments stays the original JSON string. Use when the job is gone and you
    need those keys. Retry if the line is not there yet.
    """
    page = await _es_log(request_id, LogKind.SERVICE, full=True, size=20)
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
    return await fetch_timescaling_logs(
        get_object_store(),
        TimescalingLogRequest(
            client=client,
            batchtime=batchtime,
            comptype=comptype,
            cursor=cursor,
            full=full,
        ),
    )
