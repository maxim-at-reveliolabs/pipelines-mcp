"""Injected object store for timescaling model logs."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, TypeIs

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.settings import AWS_REGION

if TYPE_CHECKING:
    from types import ModuleType

type JsonValue = (
    str | int | float | bool | Sequence[JsonValue] | Mapping[str, JsonValue] | None
)

_BUCKET: Final = "revelio-automated-pipelines"


class LogStore(Protocol):
    """List and read log objects. Injected."""

    def list_keys(self, prefix: str) -> tuple[str, ...]: ...
    def get_bytes(self, key: str) -> bytes: ...


class _S3Client(Protocol):
    def list_objects_v2(self, **kwargs: str) -> Mapping[str, JsonValue]: ...
    def get_object(self, **kwargs: str) -> Mapping[str, JsonValue]: ...


class _AwsSession(Protocol):
    def client(self, service_name: str) -> _S3Client: ...


class _Boto3Mod(Protocol):
    Session: Callable[..., _AwsSession]


class _ExcMod(Protocol):
    ClientError: type[BaseException]


def _is_boto3(module: ModuleType | _Boto3Mod) -> TypeIs[_Boto3Mod]:
    return hasattr(module, "Session")


def _is_exc(module: ModuleType | _ExcMod) -> TypeIs[_ExcMod]:
    return hasattr(module, "ClientError")


@dataclass(frozen=True, slots=True)
class _BotoLogStore:
    """Blocking S3 list and get."""

    client: _S3Client
    error_type: type[BaseException]

    def list_keys(self, prefix: str) -> tuple[str, ...]:
        keys: list[str] = []
        token: str | None = None
        try:
            while True:
                kwargs: dict[str, str] = {"Bucket": _BUCKET, "Prefix": prefix}
                if token is not None:
                    kwargs["ContinuationToken"] = token
                response = self.client.list_objects_v2(**kwargs)
                contents = response.get("Contents")
                if isinstance(contents, list):
                    for item in contents:
                        if isinstance(item, dict):
                            key = item.get("Key")
                            if isinstance(key, str) and key != "":
                                keys.append(key)
                nxt = response.get("NextContinuationToken")
                if response.get("IsTruncated") is not True or not isinstance(nxt, str):
                    return tuple(keys)
                token = nxt
        except self.error_type as exc:
            raise SettingsError(reason="log store unreachable") from exc

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=_BUCKET, Key=key)
        except self.error_type as exc:
            raise SettingsError(reason="log store unreachable") from exc
        body = response["Body"]
        read = getattr(body, "read", None)
        if not callable(read):
            raise SettingsError(reason="log store unreachable")
        raw = read()
        if not isinstance(raw, bytes):
            raise SettingsError(reason="log store unreachable")
        return raw


def live_log_store() -> LogStore:
    """Build the live object store. No network until the first list or read."""
    boto3 = importlib.import_module("boto3")
    if not _is_boto3(boto3):
        raise SettingsError(reason="boto3 Session is missing")
    exc_mod = importlib.import_module("botocore.exceptions")
    if not _is_exc(exc_mod):
        raise SettingsError(reason="botocore ClientError is missing")
    session = boto3.Session(region_name=AWS_REGION)
    return _BotoLogStore(client=session.client("s3"), error_type=exc_mod.ClientError)
