from dataclasses import dataclass
from typing import NoReturn

import httpx2
import pytest
from botocore.exceptions import SSOTokenLoadError
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import SecretStr

from pipelines_mcp.errors import SsoLoginRequiredError
from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.logs import create_logs_client
from pipelines_mcp.models import PipelineImages, PipelineQueueItem, PipelineStatus
from pipelines_mcp.server_app import (
    get_images_store,
    get_k8s,
    get_logs_client,
    get_object_store,
    get_queue_store,
    get_status_store,
    set_images_store_factory,
    set_k8s_factory,
    set_logs_factory,
    set_object_store_factory,
    set_queue_store_factory,
    set_sso,
    set_status_store_factory,
    tool_boundary,
)
from pipelines_mcp.settings import BasicAuth

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


def _raise_sso() -> None:
    raise SsoLoginRequiredError(url=_URL)


async def test_get_logs_client_does_not_check_sso() -> None:
    client = create_logs_client(
        auth=BasicAuth(username="u", password=SecretStr("p")),
        transport=httpx2.MockTransport(lambda req: httpx2.Response(200, request=req)),
    )
    set_logs_factory(lambda: client)
    set_sso(_raise_sso)
    try:
        got = get_logs_client()
        assert got is client
    finally:
        set_sso(None)
        set_logs_factory(None)
        await client.aclose()


async def test_get_object_store_does_not_check_sso() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def list_keys(self, prefix: str) -> tuple[str, ...]:
            _ = prefix
            return ()

        def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
            _ = key
            _ = max_bytes
            return b""

    store = Store()
    set_object_store_factory(lambda: store)
    set_sso(_raise_sso)
    try:
        got = get_object_store()
        assert got is store
    finally:
        set_sso(None)
        set_object_store_factory(None)


async def test_get_queue_store_does_not_check_sso() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def get(self) -> tuple[PipelineQueueItem, ...]:
            raise NotImplementedError

    store = Store()
    set_queue_store_factory(lambda: store)
    set_sso(_raise_sso)
    try:
        got = get_queue_store()
        assert got is store
    finally:
        set_sso(None)
        set_queue_store_factory(None)


async def test_get_images_store_does_not_check_sso() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def get(self) -> PipelineImages:
            raise NotImplementedError

    store = Store()
    set_images_store_factory(lambda: store)
    set_sso(_raise_sso)
    try:
        got = get_images_store()
        assert got is store
    finally:
        set_sso(None)
        set_images_store_factory(None)


async def test_get_status_store_does_not_check_sso() -> None:
    @dataclass(frozen=True, slots=True)
    class Store:
        def get(self, request_id: str) -> PipelineStatus:
            _ = request_id
            raise NotImplementedError

    store = Store()
    set_status_store_factory(lambda: store)
    set_sso(_raise_sso)
    try:
        got = get_status_store()
        assert got is store
    finally:
        set_sso(None)
        set_status_store_factory(None)


async def test_get_k8s_rebuilds_after_sso_login_required() -> None:
    built: list[K8s] = []

    def build() -> K8s:
        k8s = K8s(batch=_Batch(), core=_Core())
        built.append(k8s)
        return k8s

    set_k8s_factory(build)
    set_sso(lambda: None)
    try:
        first = get_k8s()
        set_sso(_raise_sso)
        with pytest.raises(SsoLoginRequiredError):
            _ = get_k8s()
        set_sso(lambda: None)
        second = get_k8s()
        assert first is not second
        assert len(built) == 2
    finally:
        set_k8s_factory(None)
        set_sso(None)


async def test_tool_boundary_expired_token_returns_login_url() -> None:
    set_sso(_raise_sso)
    try:
        with pytest.raises(ToolError, match="ZZZZ-YYYY") as caught:
            async with tool_boundary():
                raise SSOTokenLoadError(error_msg="dead")
    finally:
        set_sso(None)

    assert _URL in str(caught.value)
