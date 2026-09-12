"""Pipeline service status from GetPipeline."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

from google.protobuf import json_format
from pipeline_service_client.client import PipelineServiceClient
from pipeline_service_client.errors import PipelineServiceClientError

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.models import PipelineStatus, PipelineStepStatus
from pipelines_mcp.settings import load_pipeline_auth


class StatusStore(Protocol):
    """Read pipeline service status. Injected."""

    def get(self, request_id: str) -> PipelineStatus: ...


@dataclass(frozen=True, slots=True)
class _ClientStatusStore:
    client: PipelineServiceClient

    def get(self, request_id: str) -> PipelineStatus:
        try:
            info = self.client.get_pipeline(pipeline_id=request_id)
        except PipelineServiceClientError as exc:
            text = f"{exc} {exc.__cause__}"
            reason = (
                "pipeline not found"
                if "PERMISSION_DENIED" in text or "NOT_FOUND" in text
                else "pipeline status failed"
            )
            raise SettingsError(reason=reason) from exc
        steps: list[PipelineStepStatus] = []
        for index, step in enumerate(info.steps):
            config = step.config
            raw = None if config is None else config.arguments
            steps.append(
                PipelineStepStatus(
                    step_index=index,
                    name=step.name,
                    arguments="{}"
                    if raw is None
                    else json.dumps(
                        json_format.MessageToDict(raw),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
            )
        return PipelineStatus(
            request_id=info.id,
            name=info.name,
            status=info.runStatus,
            start_time=info.start_time,
            end_time=info.end_time,
            created_at=info.created_at,
            steps=tuple(steps),
        )


def live_status_store() -> StatusStore:
    """Build the live pipeline service store."""
    auth = load_pipeline_auth()
    try:
        client = PipelineServiceClient(
            username=auth.username,
            password=auth.password.get_secret_value(),
        )
    except PipelineServiceClientError as exc:
        raise SettingsError(reason="pipeline status failed") from exc
    return _ClientStatusStore(client=client)
