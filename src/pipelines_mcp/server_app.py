"""Lazy clients and tool error mapping."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final

import httpx2
from mcp.server.mcpserver.exceptions import ToolError

from pipelines_mcp.errors import (
    DomainError,
    NotFoundError,
    SsoLoginRequiredError,
)
from pipelines_mcp.k8s import K8s, K8sApiError, live_k8s
from pipelines_mcp.logs import create_logs_client
from pipelines_mcp.object_store import ObjectStore, live_object_store
from pipelines_mcp.pipeline_images import ImagesStore, live_images_store
from pipelines_mcp.pipeline_queue import QueueStore, live_queue_store
from pipelines_mcp.pipeline_status import StatusStore, live_status_store
from pipelines_mcp.redact import redact_text
from pipelines_mcp.settings_sso import request_sso, sso_needs_login

_RETRY_LOGIN: Final = "AWS login required. Retry the same request."


@dataclass(slots=True)
class _Slot:
    """Process-wide clients and factories. Mutation is the documented purpose."""

    logs: httpx2.AsyncClient | None = None
    k8s: K8s | None = None
    object_store: ObjectStore | None = None
    status_store: StatusStore | None = None
    queue_store: QueueStore | None = None
    images_store: ImagesStore | None = None
    logs_factory: Callable[[], httpx2.AsyncClient] | None = None
    k8s_factory: Callable[[], K8s] | None = None
    object_store_factory: Callable[[], ObjectStore] | None = None
    status_store_factory: Callable[[], StatusStore] | None = None
    queue_store_factory: Callable[[], QueueStore] | None = None
    images_store_factory: Callable[[], ImagesStore] | None = None
    sso: Callable[[], None] | None = None


_SLOT = _Slot()


def set_logs_factory(factory: Callable[[], httpx2.AsyncClient] | None) -> None:
    """Install the log client factory."""
    _SLOT.logs_factory = factory
    _SLOT.logs = None


def set_k8s_factory(factory: Callable[[], K8s] | None) -> None:
    """Install the cluster factory."""
    _SLOT.k8s_factory = factory
    _SLOT.k8s = None


def set_object_store_factory(factory: Callable[[], ObjectStore] | None) -> None:
    """Install the object-store factory."""
    _SLOT.object_store_factory = factory
    _SLOT.object_store = None


def set_status_store_factory(factory: Callable[[], StatusStore] | None) -> None:
    """Install the pipeline-service status factory."""
    _SLOT.status_store_factory = factory
    _SLOT.status_store = None


def set_queue_store_factory(factory: Callable[[], QueueStore] | None) -> None:
    """Install the pipeline-service queue factory."""
    _SLOT.queue_store_factory = factory
    _SLOT.queue_store = None


def set_images_store_factory(factory: Callable[[], ImagesStore] | None) -> None:
    """Install the pipeline-service image factory."""
    _SLOT.images_store_factory = factory
    _SLOT.images_store = None


def set_sso(sso: Callable[[], None] | None) -> None:
    """Install SSO login."""
    _SLOT.sso = sso


def _drop_k8s() -> None:
    _SLOT.k8s = None


def _ensure_sso() -> None:
    sso = _SLOT.sso or request_sso
    try:
        sso()
    except SsoLoginRequiredError:
        _drop_k8s()
        raise


def get_logs_client() -> httpx2.AsyncClient:
    """Return the log client. Does not check SSO."""
    if _SLOT.logs is None:
        factory = _SLOT.logs_factory or create_logs_client
        _SLOT.logs = factory()
    return _SLOT.logs


def get_k8s() -> K8s:
    """Return the cluster client. Checks SSO first."""
    _ensure_sso()
    if _SLOT.k8s is None:
        factory = _SLOT.k8s_factory or live_k8s
        _SLOT.k8s = factory()
    return _SLOT.k8s


def get_object_store() -> ObjectStore:
    """Return the object store. Does not check SSO."""
    if _SLOT.object_store is None:
        factory = _SLOT.object_store_factory or live_object_store
        _SLOT.object_store = factory()
    return _SLOT.object_store


def get_status_store() -> StatusStore:
    """Return the pipeline service store. Does not check SSO."""
    if _SLOT.status_store is None:
        factory = _SLOT.status_store_factory or live_status_store
        _SLOT.status_store = factory()
    return _SLOT.status_store


def get_queue_store() -> QueueStore:
    """Return the pipeline service queue store. Does not check SSO."""
    if _SLOT.queue_store is None:
        factory = _SLOT.queue_store_factory or live_queue_store
        _SLOT.queue_store = factory()
    return _SLOT.queue_store


def get_images_store() -> ImagesStore:
    """Return the pipeline service image store. Does not check SSO."""
    if _SLOT.images_store is None:
        factory = _SLOT.images_store_factory or live_images_store
        _SLOT.images_store = factory()
    return _SLOT.images_store


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
                "Use get_pipeline_log.",
            )
        )
        raise ToolError(redact_text(gone)) from exc
    except (K8sApiError, DomainError) as exc:
        raise ToolError(redact_text(str(exc))) from exc
    except Exception as exc:
        if not sso_needs_login(exc):
            raise
        _drop_k8s()
        try:
            _ensure_sso()
        except SsoLoginRequiredError as sso:
            raise ToolError(str(sso)) from exc
        raise ToolError(_RETRY_LOGIN) from exc
