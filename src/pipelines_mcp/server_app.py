"""Frozen App context and tool error mapping."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final

import httpx2
from mcp.server.mcpserver.exceptions import ToolError

from pipelines_mcp.errors import (
    EmptyQueryError,
    InvalidCursorError,
    NotFoundError,
    SettingsError,
    SsoLoginRequiredError,
)
from pipelines_mcp.k8s import K8s, K8sApiError, live_k8s
from pipelines_mcp.logs import create_async_client
from pipelines_mcp.object_store import LogStore, live_log_store
from pipelines_mcp.redact import redact_text
from pipelines_mcp.settings_sso import request_sso, sso_auth_expired

_NOT_FOUND: Final = "not found"
_RETRY_LOGIN: Final = "AWS login required. Retry the same request."


@dataclass(frozen=True, slots=True)
class App:
    """Injected log client for the tools."""

    logs_client: httpx2.AsyncClient


@dataclass(slots=True)
class _AppSlot:
    """Process-wide App slot. Mutation is the documented purpose."""

    app: App | None = None
    k8s: K8s | None = None
    object_store: LogStore | None = None
    builder: Callable[[], App] | None = None
    k8s_factory: Callable[[], K8s] | None = None
    object_store_factory: Callable[[], LogStore] | None = None
    reauth: Callable[[], None] | None = None


_SLOT = _AppSlot()


def set_builder(builder: Callable[[], App] | None) -> None:
    """Install a lazy App factory used on first tool call."""
    _SLOT.builder = builder
    _SLOT.app = None
    _SLOT.k8s = None
    _SLOT.object_store = None


def set_k8s_factory(factory: Callable[[], K8s] | None) -> None:
    """Install the live cluster factory. Tests leave this unset."""
    _SLOT.k8s_factory = factory
    _SLOT.k8s = None


def set_object_store_factory(factory: Callable[[], LogStore] | None) -> None:
    """Install the live object-store factory."""
    _SLOT.object_store_factory = factory
    _SLOT.object_store = None


def set_reauth(reauth: Callable[[], None] | None) -> None:
    """Install SSO login."""
    _SLOT.reauth = reauth


def install_live() -> None:
    """Install lazy log, cluster, object-store, and SSO factories."""
    set_builder(lambda: App(logs_client=create_async_client()))
    set_k8s_factory(live_k8s)
    set_object_store_factory(live_log_store)
    set_reauth(request_sso)


def _drop_clients() -> None:
    _SLOT.k8s = None


def _ensure_sso() -> None:
    reauth = _SLOT.reauth
    if reauth is None:
        return
    try:
        reauth()
    except SsoLoginRequiredError:
        _drop_clients()
        raise


def get_app() -> App:
    """Return the log client. Does not check SSO."""
    if _SLOT.app is not None:
        return _SLOT.app
    builder = _SLOT.builder
    if builder is None:
        raise SettingsError(reason="app not wired")
    app = builder()
    _SLOT.app = app
    return _SLOT.app


def get_k8s() -> K8s:
    """Return the cluster client. Checks SSO first."""
    _ensure_sso()
    if _SLOT.k8s is not None:
        return _SLOT.k8s
    factory = _SLOT.k8s_factory
    if factory is None:
        raise SettingsError(reason="cluster not wired")
    k8s = factory()
    _SLOT.k8s = k8s
    return k8s


def get_object_store() -> LogStore:
    """Return the object store. Does not check SSO."""
    if _SLOT.object_store is not None:
        return _SLOT.object_store
    factory = _SLOT.object_store_factory
    if factory is None:
        raise SettingsError(reason="timescaling logs not wired")
    store = factory()
    _SLOT.object_store = store
    return store


@asynccontextmanager
async def tool_boundary() -> AsyncGenerator[None]:
    """Convert domain errors to redacted ToolError messages."""
    try:
        yield
    except SsoLoginRequiredError as exc:
        _drop_clients()
        raise ToolError(str(exc)) from exc
    except NotFoundError as exc:
        raise ToolError(_NOT_FOUND) from exc
    except (K8sApiError, InvalidCursorError, SettingsError, EmptyQueryError) as exc:
        raise ToolError(redact_text(str(exc))) from exc
    except Exception as exc:
        if not sso_auth_expired(exc):
            raise
        _drop_clients()
        reauth = _SLOT.reauth
        if reauth is not None:
            try:
                reauth()
            except SsoLoginRequiredError as sso:
                raise ToolError(str(sso)) from exc
        raise ToolError(_RETRY_LOGIN) from exc
