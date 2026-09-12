from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest
from kubernetes.client import (
    V1ContainerState,
    V1ContainerStateRunning,
    V1ContainerStateTerminated,
    V1ContainerStateWaiting,
    V1ContainerStatus,
    V1Job,
    V1JobCondition,
    V1JobList,
    V1JobSpec,
    V1JobStatus,
    V1LabelSelector,
    V1ObjectMeta,
    V1Pod,
    V1PodList,
    V1PodStatus,
    V1PodTemplateSpec,
)

from pipelines_mcp.errors import NotFoundError
from pipelines_mcp.k8s import K8s, K8sApiError
from pipelines_mcp.models import ContainerState, job_name

_NS = "pipelines-ns"
_SECRET = "AKIAIOSFODNN7EXAMPLE"  # noqa: S105
_JOB = job_name("r1", 0, 0)
_POD = "r1-0-0-abc"
_START = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class FakeBatch:
    jobs: dict[str, V1Job]
    calls: list[tuple[str, str]] = field(default_factory=list)

    def read_namespaced_job(self, name: str, namespace: str) -> V1Job:
        self.calls.append((name, namespace))
        job = self.jobs.get(name)
        if job is None:
            raise K8sApiError(status=404)
        return job

    def list_namespaced_job(self, namespace: str) -> V1JobList:
        _ = namespace
        return V1JobList(items=list(self.jobs.values()))


@dataclass(frozen=True, slots=True)
class FakeCore:
    pods: dict[str, V1Pod]
    by_selector: dict[str, tuple[V1Pod, ...]]
    selectors: list[str] = field(default_factory=list)

    def list_namespaced_pod(
        self, namespace: str, *, label_selector: str
    ) -> V1PodList:
        _ = namespace
        self.selectors.append(label_selector)
        return V1PodList(items=list(self.by_selector.get(label_selector, ())))

    def read_namespaced_pod(self, name: str, namespace: str) -> V1Pod:
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
    conditions: list[V1JobCondition] | None = None,
    start_time: datetime | None = _START,
) -> V1JobStatus:
    return V1JobStatus(
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
    status: V1JobStatus | None = None,
) -> V1Job:
    selector = None if labels is None else V1LabelSelector(match_labels=labels)
    return V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=V1ObjectMeta(name=name, uid=uid, annotations={"x": _SECRET}),
        spec=V1JobSpec(selector=selector, template=V1PodTemplateSpec()),
        status=_status() if status is None else status,
    )


def _pod(
    *,
    state: V1ContainerState,
    phase: str = "Running",
    labels: Mapping[str, str] | None = None,
    restart_count: int = 0,
) -> V1Pod:
    return V1Pod(
        metadata=V1ObjectMeta(
            name=_POD,
            labels=None if labels is None else dict(labels),
            annotations={"x": _SECRET},
        ),
        status=V1PodStatus(
            phase=phase,
            start_time=_START,
            container_statuses=[
                V1ContainerStatus(
                    name="main",
                    ready=True,
                    restart_count=restart_count,
                    image="img",
                    image_id="id",
                    state=state,
                )
            ],
        ),
    )


def _k8s(
    *,
    jobs: dict[str, V1Job] | None = None,
    pods: dict[str, V1Pod] | None = None,
    by_selector: dict[str, tuple[V1Pod, ...]] | None = None,
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
def test_list_pods_empty(jobs: dict[str, V1Job]) -> None:
    client, _batch, _core = _k8s(jobs=jobs)
    assert client.list_pods(_JOB) == ()


def test_get_job_raises_not_found_when_404() -> None:
    client, _batch, _core = _k8s()
    with pytest.raises(NotFoundError) as caught:
        _ = client.get_job(_JOB)
    assert caught.value.entity == "job"


def test_get_pod_raises_not_found_when_404() -> None:
    client, _batch, _core = _k8s()
    with pytest.raises(NotFoundError) as caught:
        _ = client.get_pod(_POD)
    assert caught.value.entity == "pod"


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (_status(active=1), "Active"),
        (_status(failed=1, succeeded=1, active=1), "Failed"),
        (_status(succeeded=1), "Complete"),
        (_status(), "Pending"),
    ],
)
def test_job_status_mapping(status: V1JobStatus, expected: str) -> None:
    client, _batch, _core = _k8s(jobs={_JOB: _job(status=status)})
    assert client.get_job(_JOB).status == expected


def test_container_waiting_maps_state() -> None:
    pod = _pod(state=V1ContainerState(waiting=V1ContainerStateWaiting()))
    client, _batch, core = _k8s(
        jobs={_JOB: _job(labels={"job-name": "r1-0-0"}, status=_status(active=1))},
        by_selector={"job-name=r1-0-0": (pod,)},
    )
    pods = client.list_pods(_JOB)
    assert pods[0].container_statuses[0].state is ContainerState.WAITING
    assert core.selectors == ["job-name=r1-0-0"]


@pytest.mark.parametrize(
    ("state", "phase", "expected"),
    [
        (
            V1ContainerState(running=V1ContainerStateRunning()),
            "Running",
            ContainerState.RUNNING,
        ),
        (
            V1ContainerState(terminated=V1ContainerStateTerminated(exit_code=0)),
            "Succeeded",
            ContainerState.TERMINATED,
        ),
    ],
)
def test_container_state_from_pod(
    state: V1ContainerState, phase: str, expected: ContainerState
) -> None:
    client, _batch, _core = _k8s(pods={_POD: _pod(state=state, phase=phase)})
    result = client.get_pod(_POD)
    assert result.container_statuses[0].state is expected
    assert result.phase == phase


@pytest.mark.parametrize(
    ("state", "reason", "exit_code", "restart_count"),
    [
        (
            V1ContainerState(
                waiting=V1ContainerStateWaiting(reason="ImagePullBackOff")
            ),
            "ImagePullBackOff",
            None,
            0,
        ),
        (
            V1ContainerState(running=V1ContainerStateRunning()),
            None,
            None,
            0,
        ),
        (
            V1ContainerState(
                terminated=V1ContainerStateTerminated(
                    exit_code=137, reason="OOMKilled"
                )
            ),
            "OOMKilled",
            137,
            3,
        ),
        (
            V1ContainerState(terminated=V1ContainerStateTerminated(exit_code=0)),
            None,
            0,
            0,
        ),
        (
            V1ContainerState(waiting=V1ContainerStateWaiting(reason=_SECRET)),
            "[redacted]",
            None,
            0,
        ),
    ],
)
def test_container_reason_exit_code_and_restarts(
    state: V1ContainerState,
    reason: str | None,
    exit_code: int | None,
    restart_count: int,
) -> None:
    client, _batch, _core = _k8s(
        pods={_POD: _pod(state=state, restart_count=restart_count)}
    )
    status = client.get_pod(_POD).container_statuses[0]
    assert status.reason == reason
    assert status.exit_code == exit_code
    assert status.restart_count == restart_count


def test_get_pod_job_name_from_labels() -> None:
    pod = _pod(
        state=V1ContainerState(running=V1ContainerStateRunning()),
        labels={"batch.kubernetes.io/job-name": "pipelines-r1-0-0"},
    )
    client, _batch, _core = _k8s(pods={_POD: pod})
    assert client.get_pod(_POD).job_name == "pipelines-r1-0-0"


def test_failed_reason_is_first_failed_condition_and_redacted() -> None:
    client, batch, _core = _k8s(
        jobs={
            _JOB: _job(
                status=_status(
                    failed=1,
                    conditions=[
                        V1JobCondition(
                            type="Failed", message=_SECRET, status="True"
                        ),
                        V1JobCondition(
                            type="Failed", message="later", status="True"
                        ),
                    ],
                ),
            )
        }
    )
    job = client.get_job(_JOB)
    assert job.failed_reason == "[redacted]"
    assert batch.calls == [(_JOB, _NS)]


def test_pod_selector_falls_back_to_controller_uid() -> None:
    client, _batch, core = _k8s(
        jobs={_JOB: _job(uid="uid-1", status=_status(active=1))},
        by_selector={"batch.kubernetes.io/controller-uid=uid-1": ()},
    )
    _ = client.list_pods(_JOB)
    assert core.selectors == ["batch.kubernetes.io/controller-uid=uid-1"]


def test_pod_selector_falls_back_to_job_name() -> None:
    client, _batch, core = _k8s(jobs={_JOB: _job(status=_status(active=1))})
    _ = client.list_pods(_JOB)
    assert core.selectors == ["job-name=r1-0-0"]


def test_list_jobs_filters_by_status_and_prefix() -> None:
    active = _job(name="pipelines-r1-0-0", status=_status(active=1))
    failed = _job(name="pipelines-r2-0-0", status=_status(failed=1))
    client, _batch, _core = _k8s(
        jobs={"pipelines-r1-0-0": active, "pipelines-r2-0-0": failed}
    )
    rows = client.list_jobs("Active", "pipelines-r1", 50)
    assert len(rows) == 1
    assert rows[0].name == "pipelines-r1-0-0"
    assert rows[0].status == "Active"
    assert rows[0].request_id == "r1"
    assert rows[0].step_index == 0
    assert rows[0].replica == 0


def test_list_jobs_newest_first() -> None:
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
    rows = client.list_jobs(None, "pipelines-", 50)
    assert [row.name for row in rows] == ["pipelines-new-0-0", "pipelines-old-0-0"]


def test_job_config_is_redacted_yaml_with_api_names() -> None:
    client, _batch, _core = _k8s(jobs={_JOB: _job()})
    row = client.job_config(_JOB)
    assert _SECRET not in row.config
    assert "[redacted]" in row.config
    assert not row.config.lstrip().startswith("{")
    assert "apiVersion" in row.config
    assert "api_version" not in row.config


def test_pod_config_redacts_secrets() -> None:
    client, _batch, _core = _k8s(
        pods={_POD: _pod(state=V1ContainerState(running=V1ContainerStateRunning()))}
    )
    row = client.pod_config(_POD)
    assert _SECRET not in row.config
    assert "[redacted]" in row.config
    assert not row.config.lstrip().startswith("{")
