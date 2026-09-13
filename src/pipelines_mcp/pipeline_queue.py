"""Service queue from GetPipelineQueue."""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAttributeAccessIssue=false, reportPrivateUsage=false, reportUnknownLambdaType=false

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pipeline_service_client.client import PipelineServiceClient
from pipeline_service_client.errors import PipelineServiceClientError
from pipeline_service_client.generated_code.pipeline_util_pb2 import EmptyMessage

from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import PipelineQueueItem
from pipelines_mcp.settings import load_pipeline_auth


class QueueStore(Protocol):
    """Read the pipeline service queue. Injected."""

    def get(self) -> tuple[PipelineQueueItem, ...]: ...


@dataclass(frozen=True, slots=True)
class _ClientQueueStore:
    client: PipelineServiceClient

    def get(self) -> tuple[PipelineQueueItem, ...]:
        try:
            info = self.client._call_with_retries(  # noqa: SLF001  # client retry helper
                "GetPipelineQueue",
                lambda: self.client.stub.GetPipelineQueue.with_call(
                    EmptyMessage(),
                    timeout=self.client.timeout,
                    metadata=(("sessiontoken", self.client.session_token),),
                ),
            )
        except PipelineServiceClientError as exc:
            raise DomainError(reason="pipeline queue failed") from exc
        return tuple(
            PipelineQueueItem(
                request_id=item.id,
                status=item.runStatus,
                queue_tag=item.queue_tag,
            )
            for item in info.queue
        )


def live_queue_store() -> QueueStore:
    """Build the live pipeline service queue store."""
    auth = load_pipeline_auth()
    try:
        client = PipelineServiceClient(
            username=auth.username,
            password=auth.password.get_secret_value(),
        )
    except PipelineServiceClientError as exc:
        raise DomainError(reason="pipeline queue failed") from exc
    return _ClientQueueStore(client=client)
