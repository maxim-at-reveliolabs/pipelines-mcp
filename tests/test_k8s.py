from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import ClassVar

import pytest

from pipelines_mcp.errors import NotFoundError
from pipelines_mcp.k8s import (
    ContainerStateView,
    ContainerStatusView,
    JobConditionView,
    JobSpecView,
    JobStatusView,
    K8s,
    K8sApiError,
    LabelSelector,
    ObjectMeta,
    PodStatusView,
    PodView,
    Presence,
)
from pipelines_mcp.models import (
    ContainerState,
    PodName,
    Replica,
    RequestId,
    StepIndex,
    job_name,
)

pytestmark = pytest.mark.anyio

_NS = "pipelines-ns"
_SECRET = "AKIAIOSFODNN7EXAMPLE"  # noqa: S105
_JOB = job_name(RequestId("r1"), StepIndex(0), Replica(0))
_POD = PodName("r1-0-0-abc")
_START = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class Present:
    """Non-none k8s container-state sub-object."""


@dataclass(frozen=True, slots=True)
class FakeMeta:
    name: str | None = None
    uid: str | None = None
    labels: Mapping[str, str] | None = None


@dataclass(frozen=True, slots=True)
class FakeSelector:
    match_labels: Mapping[str, str] | None = None


@dataclass(frozen=True, slots=True)
class FakeJobSpec:
    selector: LabelSelector | None = None


@dataclass(frozen=True, slots=True)
class FakeCondition:
    type: str | None = None
    message: str | None = None


@dataclass(frozen=True, slots=True)
class FakeJobStatus:
    failed: int | None = None
    succeeded: int | None = None
    active: int | None = None
    start_time: datetime | None = None
    completion_time: datetime | None = None
    conditions: Sequence[JobConditionView] | None = None


@dataclass(frozen=True, slots=True)
class FakeJob:
    metadata: ObjectMeta | None = None
    spec: JobSpecView | None = None
    status: JobStatusView | None = None

    def to_dict(self) -> dict[str, str]:
        name = ""
        if self.metadata is not None and self.metadata.name is not None:
            name = self.metadata.name
        return {"name": name, "secret": _SECRET}


@dataclass(frozen=True, slots=True)
class FakeOpenApiJob(FakeJob):
    openapi_types: ClassVar[dict[str, str]] = {
        "api_version": "str",
        "kind": "str",
    }
    attribute_map: ClassVar[dict[str, str]] = {
        "api_version": "apiVersion",
        "kind": "kind",
    }
    api_version: str = "batch/v1"
    kind: str = "Job"


@dataclass(frozen=True, slots=True)
class FakeContainerState:
    waiting: Presence | None = None
    running: Presence | None = None
    terminated: Presence | None = None


@dataclass(frozen=True, slots=True)
class FakeContainerStatus:
    name: str | None = None
    ready: bool | None = None
    state: ContainerStateView | None = None


@dataclass(frozen=True, slots=True)
class FakePodStatus:
    phase: str | None = None
    start_time: datetime | None = None
    container_statuses: Sequence[ContainerStatusView] | None = None


@dataclass(frozen=True, slots=True)
class FakePod:
    metadata: ObjectMeta | None = None
    status: PodStatusView | None = None

    def to_dict(self) -> dict[str, str]:
        name = ""
        if self.metadata is not None and self.metadata.name is not None:
            name = self.metadata.name
        return {"name": name, "secret": _SECRET}


@dataclass(frozen=True, slots=True)
class FakePodList:
    items: Sequence[PodView] | None = None


@dataclass(frozen=True, slots=True)
class FakeJobList:
    items: Sequence[FakeJob] | None = None


@dataclass(frozen=True, slots=True)
class FakeBatch:
    """In-memory BatchV1Api. calls grows because this is a recorder."""

    jobs: dict[str, FakeJob]
    calls: list[tuple[str, str]] = field(default_factory=list)

    def read_namespaced_job(self, name: str, namespace: str) -> FakeJob:
        self.calls.append((name, namespace))
        job = self.jobs.get(name)
        if job is None:
            raise K8sApiError(status=404)
        return job

    def list_namespaced_job(self, namespace: str) -> FakeJobList:
        _ = namespace
        return FakeJobList(items=tuple(self.jobs.values()))


@dataclass(frozen=True, slots=True)
class FakeCore:
    """In-memory CoreV1Api. selectors grows because this is a recorder."""

    pods: dict[str, FakePod]
    by_selector: dict[str, tuple[FakePod, ...]]
    selectors: list[str] = field(default_factory=list)

    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> FakePodList:
        _ = namespace
        self.selectors.append(label_selector)
        return FakePodList(items=self.by_selector.get(label_selector, ()))

    def read_namespaced_pod(self, name: str, namespace: str) -> FakePod:
        _ = namespace
        pod = self.pods.get(name)
        if pod is None:
            raise K8sApiError(status=404)
        return pod


def _status(
    *,
    failed: int | None = None,
    succeeded: int | None = None,
    active: int | None = None,
    conditions: tuple[FakeCondition, ...] | None = None,
    start_time: datetime | None = _START,
) -> FakeJobStatus:
    return FakeJobStatus(
        failed=failed,
        succeeded=succeeded,
        active=active,
        start_time=start_time,
        conditions=conditions,
    )


def _job(
    *,
    name: str = "r1-0-0",
    uid: str | None = None,
    labels: dict[str, str] | None = None,
    status: FakeJobStatus | None = None,
) -> FakeJob:
    selector = None if labels is None else FakeSelector(match_labels=labels)
    return FakeJob(
        metadata=FakeMeta(name=name, uid=uid),
        spec=FakeJobSpec(selector=selector),
        status=_status() if status is None else status,
    )


def _pod(
    *,
    state: FakeContainerState,
    phase: str = "Running",
    labels: Mapping[str, str] | None = None,
) -> FakePod:
    return FakePod(
        metadata=FakeMeta(name=_POD, labels=labels),
        status=FakePodStatus(
            phase=phase,
            start_time=_START,
            container_statuses=(
                FakeContainerStatus(name="main", ready=True, state=state),
            ),
        ),
    )


def _k8s(
    *,
    jobs: dict[str, FakeJob] | None = None,
    pods: dict[str, FakePod] | None = None,
    by_selector: dict[str, tuple[FakePod, ...]] | None = None,
) -> tuple[K8s, FakeBatch, FakeCore]:
    batch = FakeBatch(jobs={} if jobs is None else jobs)
    core = FakeCore(
        pods={} if pods is None else pods,
        by_selector={} if by_selector is None else by_selector,
    )
    return K8s(namespace=_NS, batch=batch, core=core), batch, core


@pytest.mark.parametrize(
    "jobs",
    [
        {_JOB: _job(status=_status(active=1))},
        {},
    ],
)
async def test_list_pods_empty(jobs: dict[str, FakeJob]) -> None:
    # Given: a job with no matching pods, or no job at all
    client, _batch, _core = _k8s(jobs=jobs)

    # When: listing pods
    pods = await client.list_pods(_JOB)

    # Then: empty list, not an error
    assert pods == ()


async def test_get_job_raises_not_found_when_404() -> None:
    # Given: empty cluster
    client, _batch, _core = _k8s()

    # When: getting a missing job
    # Then: typed not-found
    with pytest.raises(NotFoundError) as caught:
        _ = await client.get_job(_JOB)
    assert caught.value.entity == "job"


async def test_get_pod_raises_not_found_when_404() -> None:
    # Given: empty cluster
    client, _batch, _core = _k8s()

    # When: getting a missing pod
    # Then: typed not-found
    with pytest.raises(NotFoundError) as caught:
        _ = await client.get_pod(_POD)
    assert caught.value.entity == "pod"


async def test_job_status_active_when_active_is_1() -> None:
    # Given: the job has an active count of one
    client, _batch, _core = _k8s(jobs={_JOB: _job(status=_status(active=1))})

    # When: reading the job
    job = await client.get_job(_JOB)

    # Then: mapped status is Active
    assert job.status == "Active"


async def test_job_status_failed_when_failed_is_1() -> None:
    # Given: the job has a failed count of one
    client, _batch, _core = _k8s(
        jobs={_JOB: _job(status=_status(failed=1, succeeded=1, active=1))}
    )

    # When: reading the job
    job = await client.get_job(_JOB)

    # Then: Failed wins
    assert job.status == "Failed"


async def test_container_waiting_maps_state() -> None:
    # Given: container state.waiting is set
    pod = _pod(state=FakeContainerState(waiting=Present()))
    client, _batch, core = _k8s(
        jobs={_JOB: _job(labels={"job-name": "r1-0-0"}, status=_status(active=1))},
        by_selector={"job-name=r1-0-0": (pod,)},
    )

    # When: listing pods
    pods = await client.list_pods(_JOB)

    # Then: waiting container maps
    assert pods[0].container_statuses[0].state is ContainerState.WAITING
    assert core.selectors == ["job-name=r1-0-0"]


async def test_container_running_maps_state() -> None:
    # Given: container state.running is set
    pod = _pod(state=FakeContainerState(running=Present()))
    client, _batch, _core = _k8s(pods={_POD: pod})

    # When: getting the pod
    result = await client.get_pod(_POD)

    # Then: running container maps
    assert result.container_statuses[0].state is ContainerState.RUNNING
    assert result.phase == "Running"


async def test_container_terminated_maps_state() -> None:
    # Given: container state.terminated is set
    pod = _pod(state=FakeContainerState(terminated=Present()), phase="Succeeded")
    client, _batch, _core = _k8s(pods={_POD: pod})

    # When: getting the pod
    result = await client.get_pod(_POD)

    # Then: terminated container maps
    assert result.container_statuses[0].state is ContainerState.TERMINATED


async def test_get_pod_job_name_from_labels() -> None:
    # Given: a pod with the job-name label
    pod = _pod(
        state=FakeContainerState(running=Present()),
        labels={"batch.kubernetes.io/job-name": "pipelines-r1-0-0"},
    )
    client, _batch, _core = _k8s(pods={_POD: pod})

    # When: getting the pod
    result = await client.get_pod(_POD)

    # Then: job_name comes from the label
    assert result.job_name == "pipelines-r1-0-0"


async def test_failed_reason_is_first_failed_condition_and_redacted() -> None:
    # Given: two Failed conditions, first message is credential-shaped
    client, batch, _core = _k8s(
        jobs={
            _JOB: _job(
                status=_status(
                    failed=1,
                    conditions=(
                        FakeCondition(type="Failed", message=_SECRET),
                        FakeCondition(type="Failed", message="later"),
                    ),
                ),
            )
        }
    )

    # When: reading the job
    job = await client.get_job(_JOB)

    # Then: first Failed message, redacted; namespace used
    assert job.failed_reason == "[redacted]"
    assert batch.calls == [(_JOB, _NS)]


async def test_pod_selector_falls_back_to_controller_uid() -> None:
    # Given: job with uid and no match_labels
    client, _batch, core = _k8s(
        jobs={_JOB: _job(uid="uid-1", status=_status(active=1))},
        by_selector={"batch.kubernetes.io/controller-uid=uid-1": ()},
    )

    # When: listing pods
    _ = await client.list_pods(_JOB)

    # Then: controller-uid selector
    assert core.selectors == ["batch.kubernetes.io/controller-uid=uid-1"]


async def test_pod_selector_falls_back_to_job_name() -> None:
    # Given: job with no match_labels and no uid
    client, _batch, core = _k8s(jobs={_JOB: _job(status=_status(active=1))})

    # When: listing pods
    _ = await client.list_pods(_JOB)

    # Then: job-name selector
    assert core.selectors == ["job-name=r1-0-0"]


async def test_list_jobs_filters_by_status_and_prefix() -> None:
    # Given: one active job and one failed job
    active = _job(name="pipelines-r1-0-0", status=_status(active=1))
    failed = _job(name="pipelines-r2-0-0", status=_status(failed=1))
    client, _batch, _core = _k8s(
        jobs={"pipelines-r1-0-0": active, "pipelines-r2-0-0": failed}
    )

    # When: listing Active jobs whose name starts with pipelines-r1
    rows = await client.list_jobs("Active", "pipelines-r1", 50)

    # Then: only the matching job
    assert len(rows) == 1
    assert rows[0].name == "pipelines-r1-0-0"
    assert rows[0].status == "Active"
    assert rows[0].request_id == "r1"
    assert rows[0].step_index == 0
    assert rows[0].replica == 0


async def test_list_jobs_newest_first() -> None:
    # Given: two active jobs with different start times
    older = _job(
        name="pipelines-old-0-0",
        status=_status(active=1, start_time=datetime(2026, 1, 1, tzinfo=UTC)),
    )
    newer = _job(
        name="pipelines-new-0-0",
        status=_status(active=1, start_time=datetime(2026, 8, 29, tzinfo=UTC)),
    )
    client, _batch, _core = _k8s(
        jobs={"pipelines-old-0-0": older, "pipelines-new-0-0": newer}
    )

    # When: listing without a prefix filter beyond pipelines-
    rows = await client.list_jobs(None, "pipelines-", 50)

    # Then: the newer job is first
    assert [row.name for row in rows] == ["pipelines-new-0-0", "pipelines-old-0-0"]


async def test_job_config_redacts_secrets() -> None:
    # Given: a job whose dumped config contains a credential
    client, _batch, _core = _k8s(jobs={_JOB: _job()})

    # When: reading the job config
    row = await client.job_config(_JOB)

    # Then: the credential is redacted
    assert _SECRET not in row.config
    assert "[redacted]" in row.config
    assert not row.config.lstrip().startswith("{")


async def test_job_config_uses_api_field_names() -> None:
    # Given: a job object with OpenAPI attribute_map
    job = FakeOpenApiJob(metadata=FakeMeta(name=_JOB))
    client, _batch, _core = _k8s(jobs={_JOB: job})

    # When: reading the job config
    row = await client.job_config(_JOB)

    # Then: keys match the API names, not Python snake_case
    assert "apiVersion" in row.config
    assert "api_version" not in row.config


async def test_pod_config_redacts_secrets() -> None:
    # Given: a pod whose dumped config contains a credential
    client, _batch, _core = _k8s(
        pods={_POD: _pod(state=FakeContainerState(running=Present()))}
    )

    # When: reading the pod config
    row = await client.pod_config(_POD)

    # Then: the credential is redacted and the body is YAML
    assert _SECRET not in row.config
    assert "[redacted]" in row.config
    assert not row.config.lstrip().startswith("{")
