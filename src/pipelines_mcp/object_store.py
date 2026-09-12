"""Injected S3 object store. List keys and get bytes."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from pipelines_mcp.errors import DomainError
from pipelines_mcp.settings import AWS_REGION

if TYPE_CHECKING:
    from collections.abc import Callable

    from types_boto3_s3.client import S3Client
    from types_boto3_s3.type_defs import (
        GetObjectRequestTypeDef,
        ListObjectsV2RequestTypeDef,
    )

_BUCKET: Final = "revelio-automated-pipelines"


class ObjectStore(Protocol):
    """List and read objects. Injected."""

    def list_keys(self, prefix: str) -> tuple[str, ...]: ...
    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes: ...


def _s3[T](read: Callable[[], T]) -> T:
    from botocore.exceptions import ClientError  # noqa: PLC0415  # load on use

    try:
        return read()
    except ClientError as exc:
        raise DomainError(reason="object store unreachable") from exc


@dataclass(frozen=True, slots=True)
class _BotoObjectStore:
    client: S3Client

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        def collect() -> tuple[str, ...]:
            keys: list[str] = []
            token: str | None = None
            while True:
                request: ListObjectsV2RequestTypeDef = {
                    "Bucket": _BUCKET,
                    "Prefix": prefix,
                }
                if token is not None:
                    request["ContinuationToken"] = token
                response = self.client.list_objects_v2(**request)
                for item in response.get("Contents") or []:
                    key = item.get("Key")
                    if key:
                        keys.append(key)
                nxt = response.get("NextContinuationToken")
                if response.get("IsTruncated") is not True or not nxt:
                    return tuple(keys)
                token = nxt

        return _s3(collect)

    def get_bytes(self, key: str, max_bytes: int | None = None) -> bytes:
        def read() -> bytes:
            request: GetObjectRequestTypeDef = {"Bucket": _BUCKET, "Key": key}
            if max_bytes is not None:
                request["Range"] = f"bytes=0-{max_bytes - 1}"
            response = self.client.get_object(**request)
            return response["Body"].read()

        return _s3(read)


def live_object_store() -> ObjectStore:
    """Build the live object store. No network until the first list or read."""
    import boto3  # noqa: PLC0415  # load on use

    session = boto3.Session(region_name=AWS_REGION)
    client: S3Client = session.client("s3")
    return _BotoObjectStore(client=client)
