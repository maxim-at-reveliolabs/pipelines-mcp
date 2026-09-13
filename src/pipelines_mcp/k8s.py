"""Read-only job and pod access. API is injected."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false

from __future__ import annotations

import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Protocol, override

import yaml
from pydantic import TypeAdapter

from pipelines_mcp.errors import NotFoundError
from pipelines_mcp.models import (
    ContainerState,
    ContainerStatus,
    Job,
    ObjectConfig,
    Pod,
    parse_job_name,
)
from pipelines_mcp.redact import redact_text

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from kubernetes.client import (
        V1ContainerState,
        V1ContainerStatus,
        V1Job,
        V1JobList,
        V1JobStatus,
        V1ObjectMeta,
        V1Pod,
        V1PodList,
    )


@dataclass(slots=True)
class K8sApiError(Exception):
    """Cluster HTTP error. status 404 means missing.

    Not frozen: Exception must accept __traceback__.
    """

    status: int

    @override
    def __str__(self) -> str:
        return f"cluster api {self.status}"


class BatchApi(Protocol):
    """Batch job reads."""

    def read_namespaced_job(self, name: str, namespace: str) -> V1Job: ...

    def list_namespaced_job(self, namespace: str) -> V1JobList: ...


class CoreApi(Protocol):
    """Pod reads."""

    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> V1PodList: ...

    def read_namespaced_pod(self, name: str, namespace: str) -> V1Pod: ...


def _api[T](read: Callable[[], T]) -> T:
    """Run a cluster call. HTTP errors become K8sApiError."""
    from kubernetes.client.exceptions import (  # noqa: PLC0415  # load on use
        ApiException,
    )

    try:
        return read()
    except ApiException as exc:
        status = exc.status
        if not isinstance(status, int):
            raise
        raise K8sApiError(status=status) from exc


def _require[T](raw: T | None, entity: str) -> T:
    if raw is None:
        raise NotFoundError(entity=entity)
    return raw


def _found[T](read: Callable[[], T]) -> T | None:
    try:
        return _api(read)
    except K8sApiError as exc:
        if exc.status == HTTPStatus.NOT_FOUND:
            return None
        raise


def _time(value: datetime | None) -> str | None:
    if value is None:
        return None
    return redact_text(value.isoformat())


def _job_status(status: V1JobStatus | None) -> str:
    if status is None:
        return "Pending"
    if status.failed:
        return "Failed"
    if status.succeeded:
        return "Complete"
    if status.active:
        return "Active"
    return "Pending"


def _failed_reason(status: V1JobStatus | None) -> str | None:
    if status is None or status.conditions is None:
        return None
    for condition in status.conditions:
        if condition.type == "Failed":
            message = condition.message
            return None if message is None else redact_text(message)
    return None


def _meta_name(meta: V1ObjectMeta | None, fallback: str) -> str:
    if meta is None or meta.name is None:
        return fallback
    return meta.name


def _job_dto(raw: V1Job, name: str) -> Job:
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


def _container_state(state: V1ContainerState | None) -> ContainerState:
    if state is None or state.waiting is not None:
        return ContainerState.WAITING
    if state.running is not None:
        return ContainerState.RUNNING
    if state.terminated is not None:
        return ContainerState.TERMINATED
    return ContainerState.WAITING


def _container(item: V1ContainerStatus) -> ContainerStatus:
    name = "" if item.name is None else item.name
    raw = item.state
    state = _container_state(raw)
    match state:
        case ContainerState.WAITING:
            waiting = None if raw is None else raw.waiting
            reason = None if waiting is None else waiting.reason
            exit_code = None
        case ContainerState.RUNNING:
            reason = None
            exit_code = None
        case ContainerState.TERMINATED:
            terminated = None if raw is None else raw.terminated
            reason = None if terminated is None else terminated.reason
            exit_code = None if terminated is None else terminated.exit_code
    return ContainerStatus(
        name=redact_text(name),
        state=state,
        ready=item.ready is True,
        reason=None if reason is None else redact_text(reason),
        exit_code=exit_code,
        restart_count=0 if item.restart_count is None else item.restart_count,
    )


def _pod_job_name(meta: V1ObjectMeta | None, job: str | None) -> str:
    if job is not None:
        return job
    labels = None if meta is None else meta.labels
    if not labels:
        return ""
    return labels.get("batch.kubernetes.io/job-name") or labels.get("job-name") or ""


def _pod_dto(raw: V1Pod, job: str | None = None) -> Pod:
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


def _object_config(raw: V1Job | V1Pod, fallback: str) -> ObjectConfig:
    from kubernetes.client import ApiClient  # noqa: PLC0415  # load on use

    payload = _YAML_MAP.validate_json(
        json.dumps(ApiClient().sanitize_for_serialization(raw), default=str)
    )
    return ObjectConfig(
        name=redact_text(_meta_name(raw.metadata, fallback)),
        config=redact_text(
            yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
        ),
    )


def _pod_selector(job: V1Job, name: str) -> str:
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

    def get_job(self, name: str) -> Job:
        """Read one job or raise NotFoundError."""
        return _job_dto(_require(self._read_job(name), "job"), name)

    def list_pods(self, job_name: str) -> tuple[Pod, ...]:
        """List pods for a job. Empty if the job or pods are gone."""
        raw = self._read_job(job_name)
        if raw is None:
            return ()
        listed = _api(
            lambda: self.core.list_namespaced_pod(
                self.namespace, label_selector=_pod_selector(raw, job_name)
            )
        )
        items = listed.items
        if items is None:
            return ()
        return tuple(_pod_dto(item, job_name) for item in items)

    def get_pod(self, pod_name: str) -> Pod:
        """Read one pod or raise NotFoundError."""
        return _pod_dto(_require(self._read_pod(pod_name), "pod"))

    def _read_job(self, name: str) -> V1Job | None:
        return _found(lambda: self.batch.read_namespaced_job(name, self.namespace))

    def _read_pod(self, pod_name: str) -> V1Pod | None:
        return _found(lambda: self.core.read_namespaced_pod(pod_name, self.namespace))

    def list_jobs(
        self,
        status: str | None,
        name_prefix: str | None,
        limit: int,
    ) -> tuple[Job, ...]:
        """List jobs, optionally filtered by status and name prefix."""
        listed = _api(lambda: self.batch.list_namespaced_job(self.namespace))
        items = listed.items
        if items is None:
            return ()
        matched: list[Job] = []
        for raw in items:
            meta = raw.metadata
            name = _meta_name(meta, "")
            row = _job_dto(raw, name)
            if status is not None and row.status != status:
                continue
            if name_prefix is not None and not row.name.startswith(name_prefix):
                continue
            matched.append(row)
        matched.sort(key=lambda row: row.start_time or "", reverse=True)
        return tuple(matched[:limit])

    def job_config(self, name: str) -> ObjectConfig:
        """Read one job's redacted YAML config."""
        return _object_config(_require(self._read_job(name), "job"), name)

    def pod_config(self, pod_name: str) -> ObjectConfig:
        """Read one pod's redacted YAML config."""
        return _object_config(_require(self._read_pod(pod_name), "pod"), pod_name)


def live_k8s() -> K8s:
    """Build a live cluster client from in-process EKS auth."""
    from kubernetes.client import (  # noqa: PLC0415  # load on use
        ApiClient,
        BatchV1Api,
        Configuration,
        CoreV1Api,
    )

    from pipelines_mcp.eks_token import live_eks_auth  # noqa: PLC0415

    config = Configuration()
    live_eks_auth().bind(config)
    api_client = ApiClient(configuration=config)
    return K8s(batch=BatchV1Api(api_client), core=CoreV1Api(api_client))
