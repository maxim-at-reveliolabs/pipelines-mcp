from typing import Final

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from pipelines_mcp.errors import SsoLoginRequiredError
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
)

_FORBIDDEN: Final[tuple[str, ...]] = (
    "eks",
    "kubernetes",
    "elasticsearch",
    "argo",
    "kubectl",
    "boto",
    "opensearch",
)


async def test_lists_exactly_eight_tool_names() -> None:
    tools = await mcp.list_tools()
    names = tuple(tool.name for tool in tools)
    assert names == _NAMES


async def test_forbidden_infra_words_absent_from_names_and_descriptions() -> None:
    tools = await mcp.list_tools()
    for tool in tools:
        blob = f"{tool.name} {tool.description}".lower()
        for word in _FORBIDDEN:
            assert word not in blob


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


async def test_tool_boundary_maps_cluster_api_error() -> None:
    # Given: the cluster API rejected the call
    # When: the error crosses the tool boundary
    with pytest.raises(ToolError, match="cluster api 403") as caught:
        async with tool_boundary():
            raise K8sApiError(status=403)

    # Then: the agent sees a stable cluster error
    assert "403" in str(caught.value)
