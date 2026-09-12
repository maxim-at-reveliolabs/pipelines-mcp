"""Lazy clients and tool error mapping."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final

import httpx2
from mcp.server.mcpserver.exceptions import ToolError

from pipelines_mcp.errors import (
    NotFoundError,
    SettingsError,
    SsoLoginRequiredError,
)
from pipelines_mcp.k8s import K8s, K8sApiError, live_k8s
from pipelines_mcp.logs import create_async_client
from pipelines_mcp.object_store import LogStore, live_log_store
from pipelines_mcp.pipeline_status import StatusStore, live_status_store
from pipelines_mcp.redact import redact_text
from pipelines_mcp.settings_sso import request_sso, sso_auth_expired

_RETRY_LOGIN: Final = "AWS login required. Retry the same request."


@dataclass(slots=True)
class _Slot:
    """Process-wide clients and factories. Mutation is the documented purpose."""

    logs: httpx2.AsyncClient | None = None
    k8s: K8s | None = None
    object_store: LogStore | None = None
    status_store: StatusStore | None = None
    logs_factory: Callable[[], httpx2.AsyncClient] | None = None
    k8s_factory: Callable[[], K8s] | None = None
    object_store_factory: Callable[[], LogStore] | None = None
    status_store_factory: Callable[[], StatusStore] | None = None
    reauth: Callable[[], None] | None = None


_SLOT = _Slot()


def set_logs_factory(factory: Callable[[], httpx2.AsyncClient] | None) -> None:
    """Install the log client factory."""
    _SLOT.logs_factory = factory
    _SLOT.logs = None


def set_k8s_factory(factory: Callable[[], K8s] | None) -> None:
    """Install the cluster factory."""
    _SLOT.k8s_factory = factory
    _SLOT.k8s = None


def set_object_store_factory(factory: Callable[[], LogStore] | None) -> None:
    """Install the object-store factory."""
    _SLOT.object_store_factory = factory
    _SLOT.object_store = None


def set_status_store_factory(factory: Callable[[], StatusStore] | None) -> None:
    """Install the pipeline-service status factory."""
    _SLOT.status_store_factory = factory
    _SLOT.status_store = None


def set_reauth(reauth: Callable[[], None] | None) -> None:
    """Install SSO login."""
    _SLOT.reauth = reauth


def _drop_k8s() -> None:
    _SLOT.k8s = None


def _ensure_sso() -> None:
    reauth = _SLOT.reauth or request_sso
    try:
        reauth()
    except SsoLoginRequiredError:
        _drop_k8s()
        raise


def get_logs_client() -> httpx2.AsyncClient:
    """Return the log client. Does not check SSO."""
    if _SLOT.logs is None:
        factory = _SLOT.logs_factory or create_async_client
        _SLOT.logs = factory()
    return _SLOT.logs


def get_k8s() -> K8s:
    """Return the cluster client. Checks SSO first."""
    _ensure_sso()
    if _SLOT.k8s is None:
        factory = _SLOT.k8s_factory or live_k8s
        _SLOT.k8s = factory()
    return _SLOT.k8s


def get_object_store() -> LogStore:
    """Return the object store. Does not check SSO."""
    if _SLOT.object_store is None:
        factory = _SLOT.object_store_factory or live_log_store
        _SLOT.object_store = factory()
    return _SLOT.object_store


def get_status_store() -> StatusStore:
    """Return the pipeline service store. Does not check SSO."""
    if _SLOT.status_store is None:
        factory = _SLOT.status_store_factory or live_status_store
        _SLOT.status_store = factory()
    return _SLOT.status_store


@asynccontextmanager
async def tool_boundary() -> AsyncGenerator[None]:
    """Convert domain errors to redacted ToolError messages."""
    try:
        yield
    except SsoLoginRequiredError as exc:
        _drop_k8s()
        raise ToolError(str(exc)) from exc
    except NotFoundError as exc:
        gone = " ".join(
            (
                f"{exc.entity} is gone from the cluster.",
                "Finished jobs and pods are removed.",
                "Use get_pipeline_log instead of job or pod state.",
            )
        )
        raise ToolError(redact_text(gone)) from exc
    except (K8sApiError, SettingsError) as exc:
        raise ToolError(redact_text(str(exc))) from exc
    except Exception as exc:
        if not sso_auth_expired(exc):
            raise
        _drop_k8s()
        try:
            _ensure_sso()
        except SsoLoginRequiredError as sso:
            raise ToolError(str(sso)) from exc
        raise ToolError(_RETRY_LOGIN) from exc
