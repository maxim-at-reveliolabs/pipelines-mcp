"""Check pipeline arguments JSON."""

# pyright: reportUnknownVariableType=false

from collections.abc import Mapping, Sequence
from json import JSONDecodeError

from pipelines_validator import validate
from pydantic import TypeAdapter, ValidationError

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.models import PipelineConfigCheck

type JsonValue = (
    str | int | float | bool | Sequence[JsonValue] | Mapping[str, JsonValue] | None
)


def check_pipeline_config(arguments: str) -> PipelineConfigCheck:
    """Parse arguments JSON and check it. Does not start a pipeline."""
    try:
        config = TypeAdapter(dict[str, JsonValue]).validate_json(arguments)
    except ValidationError as exc:
        error = (
            "arguments is not valid JSON"
            if any(item["type"] == "json_invalid" for item in exc.errors())
            else "arguments is not a JSON object"
        )
        return PipelineConfigCheck(valid=False, error=error)
    try:
        outcome = PipelineConfigCheck.model_validate(validate(config))
    except (RuntimeError, JSONDecodeError, ValidationError) as exc:
        raise SettingsError(reason="pipeline config check failed") from exc
    if outcome.valid:
        return PipelineConfigCheck(valid=True, error=None)
    return outcome
