"""Read-only job and pod access over an injected kubernetes API."""

from __future__ import annotations

import json
from dataclasses import dataclass
from http import HTTPStatus
from importlib import import_module
from typing import TYPE_CHECKING, Protocol, TypeIs, override

import yaml
from anyio.to_thread import run_sync
from pydantic import TypeAdapter

from pipelines_mcp.eks_token import KubeClientConfig, eks_auth_from_settings
from pipelines_mcp.errors import NotFoundError, SettingsError
from pipelines_mcp.models import (
    ContainerState,
    ContainerStatus,
    Job,
    JobName,
    ObjectConfig,
    Pod,
    PodName,
    parse_job_name,
)
from pipelines_mcp.redact import redact_text

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import datetime
    from types import ModuleType


@dataclass(slots=True)
class K8sApiError(Exception):
    """Injected client HTTP error. status 404 means missing.

    Not frozen: Exception must accept __traceback__.
    """

    status: int

    @override
    def __str__(self) -> str:
        """Return a stable cluster-api error."""
        return f"cluster api {self.status}"


class Presence(Protocol):
    """Presence-only k8s sub-object."""


class ObjectMeta(Protocol):
    """Job or pod metadata fields we read."""

    @property
    def name(self) -> str | None: ...
    @property
    def uid(self) -> str | None: ...
    @property
    def labels(self) -> Mapping[str, str] | None: ...


class LabelSelector(Protocol):
    """Job spec selector."""

    @property
    def match_labels(self) -> Mapping[str, str] | None: ...


class JobSpecView(Protocol):
    """Job spec fields we read."""

    @property
    def selector(self) -> LabelSelector | None: ...


class JobConditionView(Protocol):
    """One job status condition."""

    @property
    def type(self) -> str | None: ...
    @property
    def message(self) -> str | None: ...


class JobStatusView(Protocol):
    """Job status fields we read."""

    @property
    def failed(self) -> int | None: ...
    @property
    def succeeded(self) -> int | None: ...
    @property
    def active(self) -> int | None: ...
    @property
    def start_time(self) -> datetime | None: ...
    @property
    def completion_time(self) -> datetime | None: ...
    @property
    def conditions(self) -> Sequence[JobConditionView] | None: ...


class JobView(Protocol):
    """Namespaced Job object."""

    @property
    def metadata(self) -> ObjectMeta | None: ...
    @property
    def spec(self) -> JobSpecView | None: ...
    @property
    def status(self) -> JobStatusView | None: ...


class ContainerStateView(Protocol):
    """Which container state sub-object is set."""

    @property
    def waiting(self) -> Presence | None: ...
    @property
    def running(self) -> Presence | None: ...
    @property
    def terminated(self) -> Presence | None: ...


class ContainerStatusView(Protocol):
    """One container status on a pod."""

    @property
    def name(self) -> str | None: ...
    @property
    def ready(self) -> bool | None: ...
    @property
    def state(self) -> ContainerStateView | None: ...


class PodStatusView(Protocol):
    """Pod status fields we read."""

    @property
    def phase(self) -> str | None: ...
    @property
    def start_time(self) -> datetime | None: ...
    @property
    def container_statuses(self) -> Sequence[ContainerStatusView] | None: ...


class PodView(Protocol):
    """Namespaced Pod object."""

    @property
    def metadata(self) -> ObjectMeta | None: ...
    @property
    def status(self) -> PodStatusView | None: ...


class PodListView(Protocol):
    """Pod list result."""

    @property
    def items(self) -> Sequence[PodView] | None: ...


class JobListView(Protocol):
    """Job list result."""

    @property
    def items(self) -> Sequence[JobView] | None: ...


class BatchApi(Protocol):
    """Injected batch API. Not a live cluster client."""

    def read_namespaced_job(self, name: str, namespace: str) -> JobView: ...

    def list_namespaced_job(self, namespace: str) -> JobListView: ...


class CoreApi(Protocol):
    """Injected core API. Not a live cluster client."""

    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> PodListView: ...

    def read_namespaced_pod(self, name: str, namespace: str) -> PodView: ...


def _found[T](read: Callable[[], T]) -> T | None:
    try:
        return read()
    except K8sApiError as exc:
        if exc.status == HTTPStatus.NOT_FOUND:
            return None
        raise


def _time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return redact_text(value.isoformat())


def _job_status(status: JobStatusView | None) -> str:
    if status is None:
        return "Pending"
    if status.failed:
        return "Failed"
    if status.succeeded:
        return "Complete"
    if status.active:
        return "Active"
    return "Pending"


def _failed_reason(status: JobStatusView | None) -> str | None:
    if status is None or status.conditions is None:
        return None
    for condition in status.conditions:
        if condition.type == "Failed":
            message = condition.message
            return None if message is None else redact_text(message)
    return None


def _meta_name(meta: ObjectMeta | None, fallback: str) -> str:
    if meta is None or meta.name is None:
        return fallback
    return meta.name


def _job_dto(raw: JobView, name: JobName) -> Job:
    meta = raw.metadata
    raw_name = _meta_name(meta, name)
    status = raw.status
    request_id, step_index, replica = parse_job_name(raw_name)
    return Job(
        name=redact_text(raw_name),
        status=redact_text(_job_status(status)),
        start_time=_time(None if status is None else status.start_time),
        completion_time=_time(None if status is None else status.completion_time),
        failed_reason=_failed_reason(status),
        request_id=request_id,
        step_index=step_index,
        replica=replica,
    )


def _container_state(state: ContainerStateView | None) -> ContainerState:
    if state is None or state.waiting is not None:
        return ContainerState.WAITING
    if state.running is not None:
        return ContainerState.RUNNING
    if state.terminated is not None:
        return ContainerState.TERMINATED
    return ContainerState.WAITING


def _container(item: ContainerStatusView) -> ContainerStatus:
    name = "" if item.name is None else item.name
    return ContainerStatus(
        name=redact_text(name),
        state=_container_state(item.state),
        ready=item.ready is True,
    )


def _pod_job_name(meta: ObjectMeta | None, job: JobName | None) -> str:
    if job is not None:
        return job
    labels = None if meta is None else meta.labels
    if not labels:
        return ""
    return labels.get("batch.kubernetes.io/job-name") or labels.get("job-name") or ""


def _pod_dto(raw: PodView, job: JobName | None = None) -> Pod:
    meta = raw.metadata
    name = _meta_name(meta, "")
    status = raw.status
    phase = "" if status is None or status.phase is None else status.phase
    start = None if status is None else status.start_time
    raw_statuses = None if status is None else status.container_statuses
    containers = () if raw_statuses is None else raw_statuses
    return Pod(
        name=redact_text(name),
        job_name=redact_text(_pod_job_name(meta, job)),
        phase=redact_text(phase),
        start_time=_time(start),
        container_statuses=tuple(_container(item) for item in containers),
    )


type _YamlAtom = str | int | float | bool | None
type _YamlValue = _YamlAtom | list[_YamlValue] | dict[str, _YamlValue]
_YAML_MAP: TypeAdapter[dict[str, _YamlValue]] = TypeAdapter(dict[str, _YamlValue])


class _ApiClient(Protocol):
    def sanitize_for_serialization(
        self, obj: JobView | PodView
    ) -> dict[str, _YamlValue]:
        """Turn a cluster object into API-field JSON data."""
        ...


def _is_api_client(value: object) -> TypeIs[_ApiClient]:
    return hasattr(value, "sanitize_for_serialization")


def _config_payload(raw: JobView | PodView) -> dict[str, _YamlValue]:
    if hasattr(raw, "openapi_types") and hasattr(raw, "attribute_map"):
        kube = import_module("kubernetes.client")
        ctor = getattr(kube, "ApiClient", None)
        if callable(ctor):
            client = ctor()
            if _is_api_client(client):
                serialized = client.sanitize_for_serialization(raw)
                return _YAML_MAP.validate_json(json.dumps(serialized, default=str))
    to_dict = getattr(raw, "to_dict", None)
    if not callable(to_dict):
        return {}
    return _YAML_MAP.validate_json(json.dumps(to_dict(), default=str))


def _config_yaml(raw: JobView | PodView) -> str:
    dumped = yaml.safe_dump(
        _config_payload(raw), sort_keys=False, allow_unicode=True
    )
    return redact_text(dumped)


def _object_config(raw: JobView | PodView, fallback: str) -> ObjectConfig:
    return ObjectConfig(
        name=redact_text(_meta_name(raw.metadata, fallback)),
        config=_config_yaml(raw),
    )


def _pod_selector(job: JobView, name: JobName) -> str:
    spec = job.spec
    if spec is not None:
        selector = spec.selector
        if selector is not None:
            labels = selector.match_labels
            if labels:
                return ",".join(f"{key}={value}" for key, value in labels.items())
    meta = job.metadata
    uid = None if meta is None else meta.uid
    if uid:
        return f"batch.kubernetes.io/controller-uid={uid}"
    return f"job-name={_meta_name(meta, name)}"


@dataclass(frozen=True, slots=True)
class K8s:
    """Job and pod readers. API clients are injected."""

    batch: BatchApi
    core: CoreApi
    namespace: str = "pipelines-prd"

    async def get_job(self, name: JobName) -> Job:
        """Read one job or raise NotFoundError."""
        return await run_sync(self._get_job, name)

    async def list_pods(self, name: JobName) -> tuple[Pod, ...]:
        """List pods for a job. Empty if the job or pods are gone."""
        return await run_sync(self._list_pods, name)

    async def get_pod(self, pod_name: PodName) -> Pod:
        """Read one pod or raise NotFoundError."""
        return await run_sync(self._get_pod, pod_name)

    async def list_jobs(
        self,
        status: str | None,
        name_prefix: str | None,
        limit: int,
    ) -> tuple[Job, ...]:
        """List jobs, optionally filtered by status and name prefix."""
        return await run_sync(self._list_jobs, status, name_prefix, limit)

    async def job_config(self, name: JobName) -> ObjectConfig:
        """Read one job's redacted YAML config."""
        return await run_sync(self._job_config, name)

    async def pod_config(self, pod_name: PodName) -> ObjectConfig:
        """Read one pod's redacted YAML config."""
        return await run_sync(self._pod_config, pod_name)

    def _get_job(self, name: JobName) -> Job:
        return _job_dto(self._require_job(name), name)

    def _list_pods(self, name: JobName) -> tuple[Pod, ...]:
        raw = self._read_job(name)
        if raw is None:
            return ()
        listed = self.core.list_namespaced_pod(
            self.namespace, label_selector=_pod_selector(raw, name)
        )
        items = listed.items
        if items is None:
            return ()
        return tuple(_pod_dto(item, name) for item in items)

    def _get_pod(self, pod_name: PodName) -> Pod:
        return _pod_dto(self._require_pod(pod_name))

    def _require_job(self, name: JobName) -> JobView:
        raw = self._read_job(name)
        if raw is None:
            raise NotFoundError(entity="job")
        return raw

    def _require_pod(self, pod_name: PodName) -> PodView:
        raw = _found(lambda: self.core.read_namespaced_pod(pod_name, self.namespace))
        if raw is None:
            raise NotFoundError(entity="pod")
        return raw

    def _read_job(self, name: JobName) -> JobView | None:
        return _found(lambda: self.batch.read_namespaced_job(name, self.namespace))

    def _list_jobs(
        self,
        status: str | None,
        name_prefix: str | None,
        limit: int,
    ) -> tuple[Job, ...]:
        listed = self.batch.list_namespaced_job(self.namespace)
        items = listed.items
        if items is None:
            return ()
        matched: list[Job] = []
        for raw in items:
            meta = raw.metadata
            name = JobName(_meta_name(meta, ""))
            row = _job_dto(raw, name)
            if status is not None and row.status != status:
                continue
            if name_prefix is not None and not row.name.startswith(name_prefix):
                continue
            matched.append(row)
        matched.sort(key=lambda row: row.start_time or "", reverse=True)
        return tuple(matched[:limit])

    def _job_config(self, name: JobName) -> ObjectConfig:
        return _object_config(self._require_job(name), name)

    def _pod_config(self, pod_name: PodName) -> ObjectConfig:
        return _object_config(self._require_pod(pod_name), pod_name)


def _status_of(exc: BaseException) -> int:
    status = getattr(exc, "status", None)
    if isinstance(status, int):
        return status
    return 500


def _call[T](error_type: type[BaseException], read: Callable[[], T]) -> T:
    try:
        return read()
    except error_type as exc:
        raise K8sApiError(status=_status_of(exc)) from exc


@dataclass(frozen=True, slots=True)
class _LiveBatch:
    api: BatchApi
    error_type: type[BaseException]

    def read_namespaced_job(self, name: str, namespace: str) -> JobView:
        return _call(
            self.error_type, lambda: self.api.read_namespaced_job(name, namespace)
        )

    def list_namespaced_job(self, namespace: str) -> JobListView:
        return _call(self.error_type, lambda: self.api.list_namespaced_job(namespace))


@dataclass(frozen=True, slots=True)
class _LiveCore:
    api: CoreApi
    error_type: type[BaseException]

    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> PodListView:
        return _call(
            self.error_type,
            lambda: self.api.list_namespaced_pod(
                namespace, label_selector=label_selector
            ),
        )

    def read_namespaced_pod(self, name: str, namespace: str) -> PodView:
        return _call(
            self.error_type, lambda: self.api.read_namespaced_pod(name, namespace)
        )


class _KubeApiClient(Protocol):
    """Opaque kubernetes API client handle."""


class _KubeMod(Protocol):
    Configuration: Callable[[], KubeClientConfig]
    ApiClient: Callable[..., _KubeApiClient]
    BatchV1Api: Callable[..., BatchApi]
    CoreV1Api: Callable[..., CoreApi]


class _ExcMod(Protocol):
    ApiException: type[BaseException]


def _is_kube(module: ModuleType | _KubeMod) -> TypeIs[_KubeMod]:
    return hasattr(module, "Configuration") and hasattr(module, "BatchV1Api")


def _is_exc(module: ModuleType | _ExcMod) -> TypeIs[_ExcMod]:
    return hasattr(module, "ApiException")


def live_k8s() -> K8s:
    """Build a live cluster client from the baked-in EKS auth."""
    kube = import_module("kubernetes.client")
    if not _is_kube(kube):
        raise SettingsError(reason="kubernetes client is missing")
    exc_mod = import_module("kubernetes.client.exceptions")
    if not _is_exc(exc_mod):
        raise SettingsError(reason="kubernetes ApiException is missing")
    config = kube.Configuration()
    eks_auth_from_settings().bind(config)
    api_client = kube.ApiClient(configuration=config)
    error_type = exc_mod.ApiException
    return K8s(
        batch=_LiveBatch(kube.BatchV1Api(api_client), error_type),
        core=_LiveCore(kube.CoreV1Api(api_client), error_type),
    )
