"""MCP server with the read-only pipeline tools. Does not call run()."""

from typing import Final

from mcp.server import MCPServer

from pipelines_mcp import server_ops as ops
from pipelines_mcp.models import Job, LogKind, LogPage, ObjectConfig, Pod
from pipelines_mcp.server_app import tool_boundary

_INSTRUCTIONS: Final = """
This MCP debugs a pipeline by request_id UUID. A GitHub Actions or Jenkins
URL/run id is not a request_id. Passing it as request_id will fail or time out.

If the user gives a GitHub Actions or Jenkins URL or run id: fetch that CI log
first (outside this MCP), extract the request_id UUID, then call
get_pipeline_log. Never call list_pipeline_jobs for that. Typical UUID lines:
Fast pipeline <uuid> ... failed; StartMultipartPipeline; pipeline-id;
pipeline id.

If the user already gave a request_id UUID, call get_pipeline_log next. Also
get_pipeline_service_log for the service that launched the worker.
If the worker says the timescaling cluster failed: get_pipeline_job_config
(step_index and replica are usually 0) for client, batchtime, and comptype,
then get_timescaling_log.
get_pipeline_job / list_pipeline_pods only if you need job or pod state.

list_pipeline_jobs is only to browse currently running jobs when the user did
not give a request_id, GitHub URL, or Jenkins run.
""".strip()

mcp = MCPServer(
    "pipelines-mcp",
    description=(
        "Read-only debug for pipeline jobs, pods, configs, and logs. Use a "
        "request_id UUID, not a GitHub or Jenkins run id."
    ),
    instructions=_INSTRUCTIONS,
)


@mcp.tool(
    description=(
        "List currently running or recent jobs. Do not use this when the user "
        "gave a request_id UUID, GitHub URL, or Jenkins run; fetch the CI log "
        "if needed, then call get_pipeline_log. Optional filters: status "
        "(Active, Complete, Failed, Pending, or running), request_id, limit "
        "(default 50, max 100). Job names are "
        "pipelines-{request_id}-{step_index}-{replica}."
    )
)
async def list_pipeline_jobs(
    status: str | None = None,
    request_id: str | None = None,
    limit: int = 50,
) -> tuple[Job, ...]:
    """List jobs with optional status and request filters."""
    async with tool_boundary():
        return await ops.list_jobs(status, request_id, limit)


@mcp.tool(
    description=(
        "Get one job by request_id UUID, step_index, and replica (usually 0). "
        "Use only if you need job status; start with get_pipeline_log."
    )
)
async def get_pipeline_job(request_id: str, step_index: int, replica: int) -> Job:
    """Get one job by request, step, and replica."""
    async with tool_boundary():
        return await ops.get_job(request_id, step_index, replica)


@mcp.tool(
    description=(
        "List live pods for one job. Empty if they are already gone. Use for "
        "pod state; use log tools for the failure reason."
    )
)
async def list_pipeline_pods(
    request_id: str, step_index: int, replica: int
) -> tuple[Pod, ...]:
    """List pods for one job."""
    async with tool_boundary():
        return await ops.list_pods(request_id, step_index, replica)


@mcp.tool(
    description=(
        "Full YAML for one job by request_id, step_index, and replica. "
        "Secrets are redacted. Use to read client, batchtime, and comptype "
        "before get_timescaling_log."
    )
)
async def get_pipeline_job_config(
    request_id: str, step_index: int, replica: int
) -> ObjectConfig:
    """Get the full redacted config for one job."""
    async with tool_boundary():
        return await ops.job_config(request_id, step_index, replica)


@mcp.tool(description="Get one pod by pod_name from list_pipeline_pods.")
async def get_pipeline_pod(pod_name: str) -> Pod:
    """Get one pod by name."""
    async with tool_boundary():
        return await ops.get_pod(pod_name)


@mcp.tool(description="Full YAML for one pod by pod_name. Secrets are redacted.")
async def get_pipeline_pod_config(pod_name: str) -> ObjectConfig:
    """Get the full redacted config for one pod."""
    async with tool_boundary():
        return await ops.pod_config(pod_name)


@mcp.tool(
    description=(
        "Worker logs for a request_id UUID. Empty until the worker is running. "
        "Default is the last 100 lines plus a cursor. Pass cursor to get only "
        "new lines. full=true still caps size."
    )
)
async def get_pipeline_log(
    request_id: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Read pipeline logs for a request."""
    async with tool_boundary():
        return await ops.read_log(request_id, LogKind.PIPELINE, cursor, full=full)


@mcp.tool(
    description=(
        "Service logs for the same request_id UUID. Use with worker logs. "
        "Same cursor rules as get_pipeline_log."
    )
)
async def get_pipeline_service_log(
    request_id: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Read service logs for a request."""
    async with tool_boundary():
        return await ops.read_log(request_id, LogKind.SERVICE, cursor, full=full)


@mcp.tool(
    description=(
        "Timescaling model logs. Use only when worker logs say the cluster "
        "failed. Pass client, batchtime, and comptype from "
        "get_pipeline_job_config. Empty until the model writes logs. Same "
        "cursor rules as get_pipeline_log."
    )
)
async def get_timescaling_log(
    client: str,
    batchtime: str,
    comptype: str,
    cursor: str | None = None,
    *,
    full: bool = False,
) -> LogPage:
    """Read timescaling model logs for a run."""
    async with tool_boundary():
        return await ops.read_timescaling_log(
            client, batchtime, comptype, cursor, full=full
        )
