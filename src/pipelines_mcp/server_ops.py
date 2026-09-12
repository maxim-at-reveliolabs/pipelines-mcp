"""Tool implementations over lazy clients."""

from typing import Final

from pipelines_mcp.errors import EmptyQueryError
from pipelines_mcp.logs import LogRequest, fetch_logs
from pipelines_mcp.models import (
    Job,
    JobName,
    LogKind,
    LogPage,
    ObjectConfig,
    PipelineStart,
    Pod,
    PodName,
    Replica,
    RequestId,
    StepIndex,
    job_name,
)
from pipelines_mcp.pipeline_start import parse_start
from pipelines_mcp.server_app import get_k8s, get_logs_client, get_object_store
from pipelines_mcp.timescaling_logs import (
    TimescalingLogRequest,
    fetch_timescaling_logs,
)

_LIST_MAX: Final = 100
_START_LINES: Final = 20


def _nonempty(raw: str, field: str) -> str:
    stripped = raw.strip()
    if stripped == "":
        raise EmptyQueryError(field=field)
    return stripped


def _job_name(request_id: str, step_index: int, replica: int) -> JobName:
    return job_name(
        RequestId(_nonempty(request_id, "request_id")),
        StepIndex(step_index),
        Replica(replica),
    )


async def get_job(request_id: str, step_index: int, replica: int) -> Job:
    """Get one job by request, step, and replica."""
    return await get_k8s().get_job(_job_name(request_id, step_index, replica))


async def list_pods(request_id: str, step_index: int, replica: int) -> tuple[Pod, ...]:
    """List pods for one job. Empty if they are gone."""
    return await get_k8s().list_pods(_job_name(request_id, step_index, replica))


async def get_pod(pod_name: str) -> Pod:
    """Get one pod by name."""
    return await get_k8s().get_pod(PodName(_nonempty(pod_name, "pod_name")))


async def list_jobs(
    status: str | None,
    request_id: str | None,
    limit: int,
) -> tuple[Job, ...]:
    """List jobs, optionally filtered by status and request."""
    prefix: str | None = None
    if request_id is not None:
        stripped = request_id.strip()
        if stripped != "":
            prefix = f"pipelines-{stripped}"
    status_filter: str | None = None
    if status is not None:
        stripped_status = status.strip()
        if stripped_status != "":
            status_filter = (
                "Active" if stripped_status.lower() == "running" else stripped_status
            )
    return await get_k8s().list_jobs(
        status_filter,
        prefix,
        min(_LIST_MAX, max(1, limit)),
    )


async def job_config(request_id: str, step_index: int, replica: int) -> ObjectConfig:
    """Read one job's redacted config."""
    return await get_k8s().job_config(_job_name(request_id, step_index, replica))


async def pod_config(pod_name: str) -> ObjectConfig:
    """Read one pod's redacted config."""
    return await get_k8s().pod_config(PodName(_nonempty(pod_name, "pod_name")))


async def read_log(
    request_id: str,
    log_kind: LogKind,
    cursor: str | None,
    *,
    full: bool,
) -> LogPage:
    """Read a request log page for one kind."""
    return await fetch_logs(
        get_logs_client(),
        LogRequest(
            request_id=request_id,
            log_kind=log_kind,
            cursor=cursor,
            full=full,
        ),
    )


async def read_start(request_id: str) -> PipelineStart:
    """Read the start-line keys for a request."""
    page = await fetch_logs(
        get_logs_client(),
        LogRequest(
            request_id=request_id,
            log_kind=LogKind.SERVICE,
            full=True,
            size=_START_LINES,
        ),
    )
    return parse_start(page.lines)


async def read_timescaling_log(
    client: str,
    batchtime: str,
    comptype: str,
    cursor: str | None,
    *,
    full: bool,
) -> LogPage:
    """Read a timescaling model log page for one run."""
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
