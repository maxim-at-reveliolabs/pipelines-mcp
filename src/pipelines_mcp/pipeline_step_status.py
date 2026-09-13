"""Parse per-step run status from pipeline service logs."""

import re
from collections.abc import Sequence
from typing import Final

from pydantic import TypeAdapter, ValidationError

from pipelines_mcp.models import (
    PipelineStepRun,
    PipelineStepRuns,
    job_name,
    parse_job_name,
)

_INDEX_STATUS: Final = re.compile(r"Index: (\d+), replica: (\d+), status: (\S+)")


def parse_step_status(request_id: str, lines: Sequence[str]) -> PipelineStepRuns:
    """Per-replica status from service log lines.

    Empty logs return no steps. A starting-pipeline line is running. An
    Index/replica/status line wins over start.
    """
    runs: dict[tuple[int, int], PipelineStepRun] = {}
    for line in lines:
        if "[k8s-client] starting pipeline" in line:
            try:
                payload = TypeAdapter(dict[str, str]).validate_json(line)
                _, step_index, replica = parse_job_name(payload["job-name"])
            except (ValidationError, KeyError):
                pass
            else:
                if step_index is not None and replica is not None:
                    key = (step_index, replica)
                    prior = runs.get(key)
                    runs[key] = PipelineStepRun(
                        step_index=step_index,
                        replica=replica,
                        status="running" if prior is None else prior.status,
                        job_name=payload["job-name"],
                    )
        for match in _INDEX_STATUS.finditer(line):
            step_index = int(match.group(1))
            replica = int(match.group(2))
            key = (step_index, replica)
            prior = runs.get(key)
            runs[key] = PipelineStepRun(
                step_index=step_index,
                replica=replica,
                status=match.group(3),
                job_name=(
                    prior.job_name
                    if prior is not None and prior.job_name is not None
                    else job_name(request_id, step_index, replica)
                ),
            )
    return PipelineStepRuns(
        request_id=request_id,
        steps=tuple(runs[key] for key in sorted(runs)),
    )
