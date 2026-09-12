"""Frozen pipeline DTOs returned by tools."""

from enum import StrEnum, unique
from typing import ClassVar

from pydantic import BaseModel, ConfigDict


def job_name(request_id: str, step_index: int, replica: int) -> str:
    """Build `pipelines-{request_id}-{step_index}-{replica}` with no padding."""
    return f"pipelines-{request_id}-{step_index}-{replica}"


def parse_job_name(name: str) -> tuple[str | None, int | None, int | None]:
    """Split a pipeline job name into request_id, step_index, replica."""
    prefix = "pipelines-"
    if not name.startswith(prefix):
        return None, None, None
    rest = name[len(prefix) :]
    request_part, sep, replica_raw = rest.rpartition("-")
    if sep == "" or not replica_raw.isdigit():
        return None, None, None
    request_id, sep, step_raw = request_part.rpartition("-")
    if sep == "" or not step_raw.isdigit() or request_id == "":
        return None, None, None
    return request_id, int(step_raw), int(replica_raw)


@unique
class LogKind(StrEnum):
    """Kind of log query."""

    PIPELINE = "pipeline"
    SERVICE = "service"


@unique
class ContainerState(StrEnum):
    """Container state taken from which k8s sub-object is set."""

    WAITING = "waiting"
    RUNNING = "running"
    TERMINATED = "terminated"


class Job(BaseModel):
    """One cluster job. Spec is never a field."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    name: str
    status: str
    start_time: str | None
    completion_time: str | None
    failed_reason: str | None
    request_id: str | None
    step_index: int | None
    replica: int | None


class ContainerStatus(BaseModel):
    """One container on a pod."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    name: str
    state: ContainerState
    ready: bool


class Pod(BaseModel):
    """One cluster pod."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    name: str
    job_name: str
    phase: str
    start_time: str | None
    container_statuses: tuple[ContainerStatus, ...]


class LogPage(BaseModel):
    """Capped log page with an opaque cursor."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    lines: tuple[str, ...]
    cursor: str
    truncated: bool
    note: str | None = None


class ObjectConfig(BaseModel):
    """Redacted YAML config for one job or pod."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    name: str
    config: str


class PipelineStart(BaseModel):
    """Keys from the service line that starts the job."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    timestamp: str
    job_name: str
    image_name: str
    container_name: str
    namespace: str
    arguments: str
    pipeline_id: str


class PipelineStepStatus(BaseModel):
    """One pipeline service step. Spec is never a field."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    step_index: int
    name: str
    arguments: str


class PipelineStatus(BaseModel):
    """Pipeline service status for one request."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    request_id: str
    name: str
    status: str
    start_time: str
    end_time: str
    created_at: str
    steps: tuple[PipelineStepStatus, ...]
