"""Injected object store for timescaling model logs."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.settings import AWS_REGION

if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client

_BUCKET: Final = "revelio-automated-pipelines"


class LogStore(Protocol):
    """List and read log objects. Injected."""

    def list_keys(self, prefix: str) -> tuple[str, ...]: ...
    def get_bytes(self, key: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class _BotoLogStore:
    client: S3Client

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        from botocore.exceptions import ClientError  # noqa: PLC0415  # load on use

        keys: list[str] = []
        token: str | None = None
        try:
            while True:
                if token is None:
                    response = self.client.list_objects_v2(
                        Bucket=_BUCKET, Prefix=prefix
                    )
                else:
                    response = self.client.list_objects_v2(
                        Bucket=_BUCKET, Prefix=prefix, ContinuationToken=token
                    )
                for item in response.get("Contents") or []:
                    key = item.get("Key")
                    if key:
                        keys.append(key)
                nxt = response.get("NextContinuationToken")
                if response.get("IsTruncated") is not True or not nxt:
                    return tuple(keys)
                token = nxt
        except ClientError as exc:
            raise SettingsError(reason="log store unreachable") from exc

    def get_bytes(self, key: str) -> bytes:
        from botocore.exceptions import ClientError  # noqa: PLC0415  # load on use

        try:
            response = self.client.get_object(Bucket=_BUCKET, Key=key)
        except ClientError as exc:
            raise SettingsError(reason="log store unreachable") from exc
        return response["Body"].read()


def live_log_store() -> LogStore:
    """Build the live object store. No network until the first list or read."""
    import boto3  # noqa: PLC0415  # load on use

    session = boto3.Session(region_name=AWS_REGION)
    client: S3Client = session.client("s3")
    return _BotoLogStore(client=client)
