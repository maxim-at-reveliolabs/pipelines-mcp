"""Current rust and lifecycle image versions from service metadata."""

# pyright: reportArgumentType=false

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from pipeline_service_client.client import PIPELINE_METADATA_NAME, PipelineServiceClient
from pipeline_service_client.errors import PipelineServiceClientError

from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import PipelineImage, PipelineImages
from pipelines_mcp.settings import load_pipeline_auth

if TYPE_CHECKING:
    from collections.abc import Sequence


class PipelineMetadata(Protocol):
    """Pipeline service metadata fields we read."""

    @property
    def name(self) -> str: ...

    @property
    def container_name(self) -> str: ...

    @property
    def container_version(self) -> str: ...


class PipelineMetadataList(Protocol):
    """List payload from pipeline service metadata."""

    @property
    def pipelines(self) -> Sequence[PipelineMetadata]: ...


class ImagesClient(Protocol):
    """Read pipeline service metadata. Injected."""

    def get_pipeline_metadata(self, *, pipeline_name: str) -> PipelineMetadata: ...

    def get_pipelines_metadata(self) -> PipelineMetadataList: ...


class ImagesStore(Protocol):
    """Read rust and lifecycle image versions. Injected."""

    def get(self) -> PipelineImages: ...


@dataclass(frozen=True, slots=True)
class ClientImagesStore:
    """Read rust and lifecycle image versions from a metadata client."""

    client: ImagesClient

    def get(self) -> PipelineImages:
        try:
            rust_meta = self.client.get_pipeline_metadata(
                pipeline_name=PIPELINE_METADATA_NAME
            )
        except PipelineServiceClientError as exc:
            text = f"{exc} {exc.__cause__}"
            reason = (
                "pipeline not found"
                if "PERMISSION_DENIED" in text or "NOT_FOUND" in text
                else "pipeline images failed"
            )
            raise DomainError(reason=reason) from exc
        try:
            lifecycle_meta: PipelineMetadata | None = (
                self.client.get_pipeline_metadata(
                    pipeline_name="pipelines-rust-lifecycle"
                )
            )
        except PipelineServiceClientError:
            lifecycle_meta = None
            try:
                listing = self.client.get_pipelines_metadata()
            except PipelineServiceClientError:
                pass
            else:
                for item in listing.pipelines:
                    if "lifecycle" in item.name:
                        lifecycle_meta = item
                        break
        return PipelineImages(
            rust=PipelineImage(
                name=rust_meta.name,
                image=rust_meta.container_name,
                version=rust_meta.container_version,
            ),
            lifecycle=None
            if lifecycle_meta is None
            else PipelineImage(
                name=lifecycle_meta.name,
                image=lifecycle_meta.container_name,
                version=lifecycle_meta.container_version,
            ),
        )


def live_images_store() -> ImagesStore:
    """Build the live pipeline service image store."""
    auth = load_pipeline_auth()
    try:
        client = PipelineServiceClient(
            username=auth.username,
            password=auth.password.get_secret_value(),
        )
    except PipelineServiceClientError as exc:
        raise DomainError(reason="pipeline images failed") from exc
    return ClientImagesStore(client)
