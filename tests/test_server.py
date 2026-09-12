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
    "get_pipeline_job",
    "list_pipeline_pods",
    "get_pipeline_job_config",
    "get_pipeline_pod",
    "get_pipeline_pod_config",
    "get_pipeline_log",
    "get_pipeline_service_log",
    "get_pipeline_start",
    "get_timescaling_log",
)


async def test_lists_exactly_ten_tool_names() -> None:
    tools = await mcp.list_tools()
    names = tuple(tool.name for tool in tools)
    assert names == _NAMES


async def test_get_pipeline_log_empty_request_id_raises_tool_error() -> None:
    with pytest.raises(ToolError):
        _ = await mcp.call_tool("get_pipeline_log", {"request_id": ""})


async def test_get_pipeline_pod_empty_name_raises_tool_error() -> None:
    with pytest.raises(ToolError):
        _ = await mcp.call_tool("get_pipeline_pod", {"pod_name": ""})


async def test_tool_boundary_passes_sso_login_url_to_agent() -> None:
    # Given: AWS login is required
    url = "https://device.sso.example.test/?user_code=ABCD-EFGH"

    # When: the error crosses the tool boundary
    with pytest.raises(ToolError, match="ABCD-EFGH") as caught:
        async with tool_boundary():
            raise SsoLoginRequiredError(url=url)

    # Then: the agent sees the login URL
    assert url in str(caught.value)


async def test_tool_boundary_hides_url_when_helper_has_it() -> None:
    # Given: AWS login was handed to the helper
    url = "https://device.sso.example.test/?user_code=ABCD-EFGH"

    # When: the error crosses the tool boundary
    with pytest.raises(ToolError, match="Retry the same request") as caught:
        async with tool_boundary():
            raise SsoLoginRequiredError(url=url, helper=True)

    # Then: the agent does not see the login URL
    assert url not in str(caught.value)


async def test_tool_boundary_maps_cluster_api_error() -> None:
    # Given: the cluster API rejected the call
    # When: the error crosses the tool boundary
    with pytest.raises(ToolError, match="cluster api 403") as caught:
        async with tool_boundary():
            raise K8sApiError(status=403)

    # Then: the agent sees a stable cluster error
    assert "403" in str(caught.value)


async def test_tool_boundary_maps_gone_job_to_log_tools() -> None:
    # Given: the job is no longer in the cluster
    # When: that not-found crosses the tool boundary
    with pytest.raises(ToolError, match="get_pipeline_log") as caught:
        async with tool_boundary():
            raise NotFoundError(entity="job")

    # Then: the agent is told to use logs, not that the tool crashed
    text = str(caught.value)
    assert "job" in text
    assert "cluster api" not in text
