from typing import ClassVar

import pytest
from mcp.types import CallToolResult, InputRequiredResult
from pydantic import BaseModel, ConfigDict

from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import PipelineConfigCheck
from pipelines_mcp.pipeline_config import check_pipeline_config
from pipelines_mcp.server import mcp


class _CallDump(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)
    structured_content: PipelineConfigCheck | None = None


def _from_tool(result: CallToolResult | InputRequiredResult) -> PipelineConfigCheck:
    match result:
        case CallToolResult() as call:
            dump = _CallDump.model_validate_json(call.model_dump_json())
        case InputRequiredResult():
            pytest.fail("tool asked for input")
    match dump.structured_content:
        case PipelineConfigCheck() as check:
            return check
        case None:
            pytest.fail("tool did not return a config check")


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        ("{", "arguments is not valid JSON"),
        ("[]", "arguments is not a JSON object"),
        ("null", "arguments is not a JSON object"),
        ("1", "arguments is not a JSON object"),
        ('"x"', "arguments is not a JSON object"),
    ],
)
def test_bad_arguments_json_returns_valid_false(arguments: str, error: str) -> None:
    result = check_pipeline_config(arguments)
    assert result == PipelineConfigCheck(valid=False, error=error)


def test_real_validator_rejects_empty_object() -> None:
    result = check_pipeline_config("{}")
    assert result.valid is False
    assert result.error is not None


def test_validator_reject_keeps_error_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_reject(_config: dict[str, str]) -> dict[str, bool | str]:
        return {"valid": False, "error": "missing dataset"}

    monkeypatch.setattr("pipelines_mcp.pipeline_config.validate", fake_reject)
    result = check_pipeline_config("{}")
    assert result == PipelineConfigCheck(valid=False, error="missing dataset")


def test_validator_accept_clears_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_accept(_config: dict[str, str]) -> dict[str, bool | str]:
        return {"valid": True, "error": "stale"}

    monkeypatch.setattr("pipelines_mcp.pipeline_config.validate", fake_accept)
    result = check_pipeline_config('{"dataset": "positions"}')
    assert result == PipelineConfigCheck(valid=True, error=None)


def test_validator_crash_is_domain_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(_config: dict[str, str]) -> dict[str, str]:
        raise RuntimeError

    monkeypatch.setattr("pipelines_mcp.pipeline_config.validate", boom)
    with pytest.raises(DomainError, match="pipeline config check failed"):
        _ = check_pipeline_config("{}")


@pytest.mark.anyio
async def test_validate_pipeline_config_through_tool() -> None:
    result = await mcp.call_tool(
        "validate_pipeline_config",
        {"arguments": "{"},
    )
    check = _from_tool(result)
    assert check == PipelineConfigCheck(
        valid=False, error="arguments is not valid JSON"
    )
