"""MCP server with the read-only pipeline tools. Does not call run()."""

from mcp.server import MCPServer

from pipelines_mcp import server_ops as ops
from pipelines_mcp.models import Job, LogKind, LogPage, ObjectConfig, Pod
from pipelines_mcp.server_app import tool_boundary

mcp = MCPServer("pipelines-mcp")


@mcp.tool(
    description=(
        "List pipeline jobs, newest first. Optional filters: status "
        "(Active, Complete, Failed, Pending, or running), request_id, "
        "limit (default 50, max 100). Job names are "
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


@mcp.tool(description="Get one job by request_id, step_index, and replica.")
async def get_pipeline_job(request_id: str, step_index: int, replica: int) -> Job:
    """Get one job by request, step, and replica."""
    async with tool_boundary():
        return await ops.get_job(request_id, step_index, replica)


@mcp.tool(description="List pods for one job. Empty if they are already gone.")
async def list_pipeline_pods(
    request_id: str, step_index: int, replica: int
) -> tuple[Pod, ...]:
    """List pods for one job."""
    async with tool_boundary():
        return await ops.list_pods(request_id, step_index, replica)


@mcp.tool(
    description=(
        "Get the full YAML config for one job by request_id, step_index, "
        "and replica. Secrets are redacted."
    )
)
async def get_pipeline_job_config(
    request_id: str, step_index: int, replica: int
) -> ObjectConfig:
    """Get the full redacted config for one job."""
    async with tool_boundary():
        return await ops.job_config(request_id, step_index, replica)


@mcp.tool(description="Get one pod by pod_name.")
async def get_pipeline_pod(pod_name: str) -> Pod:
    """Get one pod by name."""
    async with tool_boundary():
        return await ops.get_pod(pod_name)


@mcp.tool(
    description=(
        "Get the full YAML config for one pod by pod_name. Secrets are redacted."
    )
)
async def get_pipeline_pod_config(pod_name: str) -> ObjectConfig:
    """Get the full redacted config for one pod."""
    async with tool_boundary():
        return await ops.pod_config(pod_name)


@mcp.tool(
    description=(
        "Read pipeline worker logs for a request. Empty until the worker pod "
        "is running. Default is the last 100 lines plus a cursor. Pass cursor "
        "to get only new lines. full=true still caps size."
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
    description="Read service logs for a request. Same cursor rules as pipeline log."
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
