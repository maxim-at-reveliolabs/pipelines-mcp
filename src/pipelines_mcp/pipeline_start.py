"""Parse the service line that starts a pipeline job."""

from collections.abc import Sequence
from typing import Final

from pydantic import TypeAdapter, ValidationError

from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import PipelineStart

_START_FIELDS: Final = TypeAdapter(dict[str, str])


def parse_start(lines: Sequence[str]) -> PipelineStart:
    """Start-line keys. arguments stays the original JSON."""
    for line in lines:
        if "[k8s-client] starting pipeline" not in line:
            continue
        try:
            payload = _START_FIELDS.validate_json(line)
            return PipelineStart(
                timestamp=payload["@timestamp"],
                job_name=payload["job-name"],
                image_name=payload["image-name"],
                container_name=payload["container-name"],
                namespace=payload["namespace"],
                arguments=payload["arguments"],
                pipeline_id=payload["pipeline-id"],
            )
        except (ValidationError, KeyError) as err:
            raise DomainError(
                reason="starting pipeline line is not valid"
            ) from err
    raise DomainError(
        reason="No starting pipeline line yet. Retry after the service queues the job."
    )
