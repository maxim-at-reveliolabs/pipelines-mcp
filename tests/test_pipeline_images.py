from dataclasses import dataclass

import pytest
from pipeline_service_client.client import PIPELINE_METADATA_NAME
from pipeline_service_client.errors import PipelineServiceClientError

from pipelines_mcp.errors import DomainError
from pipelines_mcp.models import PipelineImage, PipelineImages
from pipelines_mcp.pipeline_images import ClientImagesStore


@dataclass(frozen=True, slots=True)
class FakeMeta:
    name: str
    container_name: str
    container_version: str


@dataclass(frozen=True, slots=True)
class FakeList:
    pipelines: tuple[FakeMeta, ...]


@dataclass(frozen=True, slots=True)
class FakeClient:
    records: dict[str, FakeMeta]
    listing: tuple[FakeMeta, ...] = ()
    list_error: PipelineServiceClientError | None = None

    def get_pipeline_metadata(self, *, pipeline_name: str) -> FakeMeta:
        try:
            return self.records[pipeline_name]
        except KeyError:
            msg = "NOT_FOUND"
            raise PipelineServiceClientError(msg) from None

    def get_pipelines_metadata(self) -> FakeList:
        if self.list_error is not None:
            raise self.list_error
        return FakeList(pipelines=self.listing)


_RUST = FakeMeta(
    name=PIPELINE_METADATA_NAME,
    container_name="pipelines-rust",
    container_version="v1.2.3",
)
_LIFECYCLE = FakeMeta(
    name="pipelines-rust-lifecycle",
    container_name="pipelines-rust-lifecycle",
    container_version="v9.9.9",
)


def test_returns_rust_and_lifecycle_when_both_names_exist() -> None:
    client = FakeClient(
        records={
            PIPELINE_METADATA_NAME: _RUST,
            "pipelines-rust-lifecycle": _LIFECYCLE,
        },
        listing=(
            FakeMeta(
                name="pipelines-rust-lifecycle",
                container_name="pipelines-rust-lifecycle",
                container_version="stale",
            ),
        ),
    )
    images = ClientImagesStore(client).get()
    assert images == PipelineImages(
        rust=PipelineImage(
            name=PIPELINE_METADATA_NAME,
            image="pipelines-rust",
            version="v1.2.3",
        ),
        lifecycle=PipelineImage(
            name="pipelines-rust-lifecycle",
            image="pipelines-rust-lifecycle",
            version="v9.9.9",
        ),
    )


def test_uses_list_entry_when_lifecycle_name_is_missing() -> None:
    listed = FakeMeta(
        name="custom-lifecycle",
        container_name="pipelines-rust-lifecycle",
        container_version="v4.0.0",
    )
    client = FakeClient(
        records={PIPELINE_METADATA_NAME: _RUST},
        listing=(
            _RUST,
            FakeMeta(name="other", container_name="other", container_version="x"),
            listed,
        ),
    )
    images = ClientImagesStore(client).get()
    assert images.rust.version == "v1.2.3"
    assert images.lifecycle == PipelineImage(
        name="custom-lifecycle",
        image="pipelines-rust-lifecycle",
        version="v4.0.0",
    )


def test_returns_rust_when_lifecycle_is_missing() -> None:
    client = FakeClient(
        records={PIPELINE_METADATA_NAME: _RUST},
        listing=(
            FakeMeta(name="other", container_name="other", container_version="x"),
        ),
    )
    images = ClientImagesStore(client).get()
    assert images == PipelineImages(
        rust=PipelineImage(
            name=PIPELINE_METADATA_NAME,
            image="pipelines-rust",
            version="v1.2.3",
        ),
        lifecycle=None,
    )


def test_returns_rust_when_lifecycle_list_fails() -> None:
    client = FakeClient(
        records={PIPELINE_METADATA_NAME: _RUST},
        list_error=PipelineServiceClientError("list failed"),
    )
    images = ClientImagesStore(client).get()
    assert images.rust.version == "v1.2.3"
    assert images.lifecycle is None


def test_maps_missing_rust_to_not_found() -> None:
    client = FakeClient(records={})
    with pytest.raises(DomainError, match="pipeline not found"):
        _ = ClientImagesStore(client).get()


def test_maps_generic_rust_error_to_images_failed() -> None:
    @dataclass(frozen=True, slots=True)
    class BoomClient:
        def get_pipeline_metadata(self, *, pipeline_name: str) -> FakeMeta:
            _ = pipeline_name
            msg = "unavailable"
            raise PipelineServiceClientError(msg)

        def get_pipelines_metadata(self) -> FakeList:
            pytest.fail("list")

    with pytest.raises(DomainError, match="pipeline images failed"):
        _ = ClientImagesStore(BoomClient()).get()

