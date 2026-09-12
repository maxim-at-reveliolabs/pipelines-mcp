from dataclasses import dataclass
from typing import NoReturn

import httpx2
import pytest
from botocore.exceptions import SSOTokenLoadError
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import SecretStr

from pipelines_mcp.errors import SsoLoginRequiredError
from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.logs import create_async_client
from pipelines_mcp.server_app import (
    App,
    get_app,
    get_k8s,
    get_object_store,
    set_builder,
    set_k8s_factory,
    set_object_store_factory,
    set_reauth,
    tool_boundary,
)
from pipelines_mcp.settings import EsAuth

pytestmark = pytest.mark.anyio

_URL = "https://device.sso.example.test/?user_code=ZZZZ-YYYY"


@dataclass(frozen=True, slots=True)
class _Batch:
    def read_namespaced_job(self, name: str, namespace: str) -> NoReturn:
        _ = name, namespace
        raise K8sApiError(status=404)

    def list_namespaced_job(self, namespace: str) -> NoReturn:
        _ = namespace
        raise K8sApiError(status=404)


@dataclass(frozen=True, slots=True)
class _Core:
    def list_namespaced_pod(self, namespace: str, *, label_selector: str) -> NoReturn:
        _ = namespace, label_selector
        raise K8sApiError(status=404)

    def read_namespaced_pod(self, name: str, namespace: str) -> NoReturn:
        _ = name, namespace
        raise K8sApiError(status=404)


def _client() -> httpx2.AsyncClient:
    return create_async_client(
        auth=EsAuth(username="u", password=SecretStr("p")),
        transport=httpx2.MockTransport(lambda req: httpx2.Response(200, request=req)),
    )


def _k8s() -> K8s:
    return K8s(batch=_Batch(), core=_Core())


async def test_get_app_does_not_check_sso() -> None:
    # Given: SSO login would fail
    client = _client()
    set_builder(lambda: App(logs_client=client))

    def reauth() -> None:
        raise SsoLoginRequiredError(url=_URL)

    set_reauth(reauth)
    try:
        # When: a log-only path loads the app
        app = get_app()
        # Then: the log client is returned with no login error
        assert app.logs_client is client
    finally:
        set_reauth(None)
        set_builder(None)
        await client.aclose()


async def test_get_object_store_does_not_check_sso() -> None:
    # Given: SSO login would fail
    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            _ = prefix
            return ()

        def get_bytes(self, key: str) -> bytes:
            _ = key
            return b""

    store = Store()
    set_object_store_factory(lambda: store)

    def reauth() -> None:
        raise SsoLoginRequiredError(url=_URL)

    set_reauth(reauth)
    try:
        # When: the object store is loaded
        got = get_object_store()
        # Then: the store is returned with no login error
        assert got is store
    finally:
        set_reauth(None)
        set_object_store_factory(None)


async def test_get_k8s_rebuilds_after_sso_login_required() -> None:
    # Given: a cluster client was already built, then SSO login is required
    built: list[K8s] = []
    client = _client()
    set_builder(lambda: App(logs_client=client))
    set_k8s_factory(lambda: built.append(_k8s()) or built[-1])
    set_reauth(None)
    first = get_k8s()

    def reauth() -> None:
        raise SsoLoginRequiredError(url=_URL)

    set_reauth(reauth)

    # When: get_k8s runs while login is required
    with pytest.raises(SsoLoginRequiredError):
        _ = get_k8s()

    # Then: a later successful login builds a new cluster client
    set_reauth(None)
    second = get_k8s()
    assert first is not second
    assert len(built) == 2
    set_k8s_factory(None)
    set_builder(None)
    await client.aclose()


async def test_tool_boundary_expired_token_returns_login_url() -> None:
    # Given: cluster auth failed because the SSO token is dead
    def reauth() -> None:
        raise SsoLoginRequiredError(url=_URL)

    set_reauth(reauth)
    try:
        # When: that error crosses the tool boundary
        with pytest.raises(ToolError, match="ZZZZ-YYYY") as caught:
            async with tool_boundary():
                raise SSOTokenLoadError(error_msg="dead")
    finally:
        set_reauth(None)

    # Then: the agent sees the login URL
    assert _URL in str(caught.value)
