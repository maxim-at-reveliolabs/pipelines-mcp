"""Frozen pipeline DTOs returned by tools."""

from enum import StrEnum, unique
from typing import ClassVar, NewType

from pydantic import BaseModel, ConfigDict

RequestId = NewType("RequestId", str)
StepIndex = NewType("StepIndex", int)
Replica = NewType("Replica", int)
PodName = NewType("PodName", str)
JobName = NewType("JobName", str)


def job_name(request_id: RequestId, step_index: StepIndex, replica: Replica) -> JobName:
    """Build `pipelines-{request_id}-{step_index}-{replica}` with no padding."""
    return JobName(f"pipelines-{request_id}-{step_index}-{replica}")


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
