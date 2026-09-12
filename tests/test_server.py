from typing import Final

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from pipelines_mcp.errors import NotFoundError, SsoLoginRequiredError
from pipelines_mcp.k8s import K8sApiError
from pipelines_mcp.server import mcp
from pipelines_mcp.server_app import tool_boundary

pytestmark = pytest.mark.anyio

_NAMES: Final[tuple[str, ...]] = (
    "list_pipeline_jobs",
    "get_pipeline_status",
    "get_pipeline_job",
    "list_pipeline_pods",
    "get_pipeline_job_config",
    "get_pipeline_pod",
    "get_pipeline_pod_config",
    "get_pipeline_log",
    "get_pipeline_step_log",
    "get_pipeline_service_log",
    "search_pipeline_log",
    "search_pipeline_service_log",
    "get_pipeline_start",
    "get_timescaling_log",
    "list_pipeline_artifacts",
    "list_pipeline_artifact_files",
    "list_pipeline_lifecycle_artifacts",
    "validate_pipeline_config",
)


async def test_lists_exactly_eighteen_tool_names() -> None:
    tools = await mcp.list_tools()
    names = tuple(tool.name for tool in tools)
    assert names == _NAMES


@pytest.mark.parametrize(
    ("tool", "args"),
    [
        ("get_pipeline_log", {"request_id": ""}),
        ("get_pipeline_step_log", {"request_id": "", "step_index": 0, "replica": 0}),
        ("get_pipeline_status", {"request_id": ""}),
        ("get_pipeline_pod", {"pod_name": ""}),
        ("search_pipeline_log", {"request_id": "r1", "query": ""}),
        ("validate_pipeline_config", {"arguments": ""}),
    ],
)
async def test_empty_required_token_raises_tool_error(
    tool: str, args: dict[str, str | int]
) -> None:
    with pytest.raises(ToolError):
        _ = await mcp.call_tool(tool, args)


async def test_tool_boundary_passes_sso_login_url_to_agent() -> None:
    url = "https://device.sso.example.test/?user_code=ABCD-EFGH"
    with pytest.raises(ToolError, match="ABCD-EFGH") as caught:
        async with tool_boundary():
            raise SsoLoginRequiredError(url=url)
    assert url in str(caught.value)


async def test_tool_boundary_hides_url_when_helper_has_it() -> None:
    url = "https://device.sso.example.test/?user_code=ABCD-EFGH"
    with pytest.raises(ToolError, match="Retry the same request") as caught:
        async with tool_boundary():
            raise SsoLoginRequiredError(url=url, helper=True)
    assert url not in str(caught.value)


async def test_tool_boundary_maps_cluster_api_error() -> None:
    with pytest.raises(ToolError, match="cluster api 403"):
        async with tool_boundary():
            raise K8sApiError(status=403)


async def test_tool_boundary_maps_gone_job_to_log_tools() -> None:
    with pytest.raises(ToolError, match="get_pipeline_log") as caught:
        async with tool_boundary():
            raise NotFoundError(entity="job")
    text = str(caught.value)
    assert "job" in text
    assert "cluster api" not in text
