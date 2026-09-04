from dataclasses import is_dataclass

import pytest

from pipelines_mcp.errors import (
    EmptyQueryError,
    InvalidCursorError,
    NotFoundError,
    SettingsError,
    SsoLoginRequiredError,
)
from pipelines_mcp.models import (
    ContainerState,
    ContainerStatus,
    Job,
    LogKind,
    LogPage,
    ObjectConfig,
    Pod,
    Replica,
    RequestId,
    StepIndex,
    job_name,
    parse_job_name,
)

type _DtoClass = type[Job | Pod | LogPage | ContainerStatus | ObjectConfig]


@pytest.mark.parametrize(
    ("cls", "names"),
    [
        (
            Job,
            (
                "name",
                "status",
                "start_time",
                "completion_time",
                "failed_reason",
                "request_id",
                "step_index",
                "replica",
            ),
        ),
        (Pod, ("name", "job_name", "phase", "start_time", "container_statuses")),
        (LogPage, ("lines", "cursor", "truncated", "note")),
        (ContainerStatus, ("name", "state", "ready")),
        (ObjectConfig, ("name", "config")),
    ],
)
def test_dto_field_names(cls: _DtoClass, names: tuple[str, ...]) -> None:
    assert tuple(cls.model_fields) == names
    assert "spec" not in names
    for item in cls.model_fields.values():
        assert "dict" not in str(item.annotation)


@pytest.mark.parametrize(
    ("enum_cls", "values"),
    [
        (ContainerState, ("waiting", "running", "terminated")),
        (LogKind, ("pipeline", "service")),
    ],
)
def test_enum_members(
    enum_cls: type[ContainerState | LogKind],
    values: tuple[str, ...],
) -> None:
    assert tuple(member.value for member in enum_cls) == values


@pytest.mark.parametrize(
    ("error", "text"),
    [
        (SettingsError(reason="missing field"), "missing field"),
        (NotFoundError(entity="request"), "request not found"),
        (InvalidCursorError(), "invalid cursor"),
        (EmptyQueryError(field="q"), "empty q"),
        (
            SsoLoginRequiredError(url="https://example.test/login"),
            "https://example.test/login",
        ),
        (
            SsoLoginRequiredError(url="https://example.test/login", helper=True),
            "AWS login started. Retry the same request.",
        ),
    ],
)
def test_domain_error_is_typed_dataclass_not_value_error(
    error: Exception, text: str
) -> None:
    assert is_dataclass(error)
    assert isinstance(error, Exception)
    assert not isinstance(error, ValueError)
    assert text in str(error)


@pytest.mark.parametrize(
    ("step", "replica", "expected"),
    [
        (0, 0, "pipelines-r1-0-0"),
        (12, 3, "pipelines-r1-12-3"),
    ],
)
def test_job_name_is_unpadded(step: int, replica: int, expected: str) -> None:
    assert job_name(RequestId("r1"), StepIndex(step), Replica(replica)) == expected


def test_parse_job_name_splits_pipeline_job() -> None:
    # Given: a pipeline job name with a uuid request id
    name = "pipelines-5b9ba6a8-6869-431a-ab22-4e068749cf43-0-0"

    # When: parsing it
    request_id, step_index, replica = parse_job_name(name)

    # Then: the uuid and indexes are recovered
    assert request_id == "5b9ba6a8-6869-431a-ab22-4e068749cf43"
    assert step_index == 0
    assert replica == 0


def test_parse_job_name_rejects_non_pipeline_name() -> None:
    assert parse_job_name("nvme-warm-1ecab4de-e7f5-41fc-bf31-3689cea2dfce") == (
        None,
        None,
        None,
    )
