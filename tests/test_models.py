from dataclasses import is_dataclass

import pytest

from pipelines_mcp.errors import (
    DomainError,
    NotFoundError,
    SsoLoginRequiredError,
)
from pipelines_mcp.models import (
    job_name,
    parse_job_name,
)


@pytest.mark.parametrize(
    ("error", "text"),
    [
        (DomainError(reason="missing field"), "missing field"),
        (NotFoundError(entity="request"), "request not found"),
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
    assert job_name("r1", step, replica) == expected


def test_parse_job_name_splits_pipeline_job() -> None:
    name = "pipelines-5b9ba6a8-6869-431a-ab22-4e068749cf43-0-0"
    request_id, step_index, replica = parse_job_name(name)
    assert request_id == "5b9ba6a8-6869-431a-ab22-4e068749cf43"
    assert step_index == 0
    assert replica == 0


@pytest.mark.parametrize(
    "name",
    [
        "nvme-warm-1ecab4de-e7f5-41fc-bf31-3689cea2dfce",
        "pipelines-r1-0-x",
        "pipelines-r1-x-0",
        "pipelines--0-0",
    ],
)
def test_parse_job_name_rejects_bad_name(name: str) -> None:
    assert parse_job_name(name) == (None, None, None)
