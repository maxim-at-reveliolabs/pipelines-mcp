"""EKS bearer token via botocore RequestSigner. No AWS CLI."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

import base64
import binascii
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, TYPE_CHECKING, Final, Protocol, TypedDict

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.settings import AWS_PROFILE, AWS_REGION

if TYPE_CHECKING:
    from boto3.session import Session
    from types_boto3_sts.client import STSClient

type Clock = Callable[[], float]

_EKS_CLUSTER: Final = "dev"
_CLUSTER_ENDPOINT: Final = (
    "https://C9E48999FC8319AC0A1A8EC23AE236FF.gr7.us-east-2.eks.amazonaws.com"
)
_CA_B64: Final = (
    "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJZmthWmF3"
    "UXNaMzh3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhS"
    "bGN6QWVGdzB5TXpBNU1qQXdNekV4TVRoYUZ3MHpNekE1TVRjd016RXhNVGhhTUJVeApFekFS"
    "QmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3"
    "QXdnZ0VLCkFvSUJBUURCWkQ5TzNxNzVXS3VjZ3ZQckp5SnIyRWJZNnhYTU1BVFI3eDlRVWJu"
    "T3N5b3d3NTVoWUh2MFpyRGgKNUMwYTBMNjJqeW9ybG5JVVJVNGFlUm9USVNPYzByTC8xd2I4"
    "cXg2Si84WmRQZXFUQ0d0R0RyZTQvbHZDRDJsVgpJUkZzaXIvZEVSTSt5YUhaM0czSGUwV1pE"
    "cCtDZzRVNXRGR0ZCdCtXL2dUZDlqbGY4V3hhdnovRXZZZ0xjOUpUCjFtVVFINWhodXFHQkFK"
    "ZXVZSzhoTy8wK0xNNTFRK0dwb0hiZTUyWE1XdmdoYWZZMzlQUVVWU0hjaVQ4OXpqQjgKRThI"
    "dlpIUGtRZFJUNFRoVThVdmtiVjErejhKUks3TVQ1UjJNRnpoeU84NFZMVlJKSkJOT3plZk1z"
    "NVczeC9ENAp6NEozN3ZyS3FZVDg2M3NKZEpBa2srbHFQbGY3QWdNQkFBR2pXVEJYTUE0R0Ex"
    "VWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJS"
    "WVhjR0xpN3NIREtpTk1LcGFoSjlodWYxdXJUQVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVs"
    "ZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQ1BBQmVyZlgrTApHVGlGK0ZJSFlGY0hh"
    "YzFvSW90bzhXMkUvWGdZUFVqV1NmK2ExVGZqaVIrMEhiaTIzVWZ5Nk9lMUdsK1dpdVBZClJZ"
    "KzJJc2pzZHR0ZGhaNVRLTGZkYzdlazRWUUVvaWhnczlBbWlrMFBnclRCOGpON0lhTXFORmc4"
    "S0ZKRTcvdVcKdzVFUDZCeCtjMXEzT1A4OXEwV3lJTk44bmVzaTd3N2tvUGd2QnVtSlBmcVBK"
    "VVVaYXZqTmJYbWJONHhZTERYaQpBMVJKNHhlRGlNS1hLQUlvektRcDNFY1NDSWR3ZWdhTmZK"
    "dm0yUUNMeWs4d3VITGk3QzVUMlphbGJFUjhhMzlLCnNBT1I5MHdNVk15Y2pRbzdMekpMNWRo"
    "RklSMnQyZVl4U2JQOGJwR1JLTWFCTzg2bGZEWnZhN2FGVjJxN0h0RkYKNTlXN3p6UnZ0RWEw"
    "Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K"
)


class PresignRequest(TypedDict):
    """botocore request dict for a presigned STS GET."""

    method: str
    url: str
    body: dict[str, str]
    headers: dict[str, str]
    context: dict[str, str]


class PresignSigner(Protocol):
    """Creates a presigned STS URL."""

    def generate_presigned_url(
        self,
        request_dict: PresignRequest,
        operation_name: str,
        expires_in: int = 3600,
        region_name: str | None = None,
        signing_name: str | None = None,
    ) -> str: ...


class KubeClientConfig(Protocol):
    """kubernetes Configuration fields this module sets."""

    host: str
    api_key: dict[str, str]
    api_key_prefix: dict[str, str]
    ssl_ca_cert: str | None
    refresh_api_key_hook: Callable[[KubeClientConfig], None] | None


@dataclass(frozen=True, slots=True)
class CachedCluster:
    """Cluster API endpoint and decoded CA PEM."""

    endpoint: str
    ca_pem: str

    @classmethod
    def from_ca_data(cls, *, endpoint: str, ca_data: str) -> CachedCluster:
        """Decode a cluster endpoint and base64 CA."""
        try:
            pem = base64.b64decode(ca_data, validate=True).decode("utf-8")
        except (ValueError, UnicodeError, binascii.Error) as exc:
            raise SettingsError(reason="cluster lookup failed") from exc
        stripped = endpoint.strip()
        if stripped == "" or pem.strip() == "":
            raise SettingsError(reason="cluster lookup failed")
        return cls(endpoint=stripped, ca_pem=pem)


_CLUSTER: Final = CachedCluster.from_ca_data(
    endpoint=_CLUSTER_ENDPOINT,
    ca_data=_CA_B64,
)


@dataclass(frozen=True, slots=True)
class TokenMint:
    """Inputs needed to mint one EKS token."""

    cluster_name: str
    region: str
    signer: PresignSigner
    # botocore RequestSigner weakrefs the STS client; keep session and client alive.
    session: Session | None = None
    sts: STSClient | None = None


def mint_token(mint: TokenMint) -> str:
    """Presign STS GetCallerIdentity and encode a k8s-aws-v1 token."""
    request: PresignRequest = {
        "method": "GET",
        "url": (
            f"https://sts.{mint.region}.amazonaws.com/"
            "?Action=GetCallerIdentity&Version=2011-06-15"
        ),
        "body": {},
        "headers": {"x-k8s-aws-id": mint.cluster_name},
        "context": {},
    }
    presigned = mint.signer.generate_presigned_url(
        request,
        operation_name="",
        expires_in=900,
        region_name=mint.region,
    )
    encoded = base64.urlsafe_b64encode(presigned.encode("utf-8")).decode("ascii")
    return f"k8s-aws-v1.{encoded.rstrip('=')}"


class EksAuth:
    """Caches cluster CA and remints the bearer token.

    Mutation is required: token timestamp and the CA tempfile stay on this
    object for process lifetime.
    """

    def __init__(self, mint: TokenMint, cluster: CachedCluster, clock: Clock) -> None:
        """Keep mint state, cluster, and clock on this object."""
        self._mint: TokenMint = mint
        self._cluster: CachedCluster = cluster
        self._clock: Clock = clock
        self._minted_at: float | None = None
        self._ca_file: IO[str] | None = None

    def bind(self, config: KubeClientConfig) -> None:
        """Set host, CA, bearer token, and the 14-minute refresh hook."""
        config.host = self._cluster.endpoint
        config.api_key_prefix["BearerToken"] = "Bearer"
        self._apply_ca(config)
        config.refresh_api_key_hook = self.refresh_api_key
        self.refresh_api_key(config)

    def refresh_api_key(self, config: KubeClientConfig) -> None:
        """Remint the bearer token when 14 minutes have passed."""
        now = self._clock()
        minted_at = self._minted_at
        if minted_at is not None and now - minted_at < 14 * 60:
            return
        config.api_key["BearerToken"] = mint_token(self._mint)
        self._minted_at = now

    def _apply_ca(self, config: KubeClientConfig) -> None:
        if self._ca_file is None:
            # Must stay open for process lifetime; closing deletes the PEM.
            ca_file = tempfile.NamedTemporaryFile(  # noqa: SIM115
                mode="w",
                encoding="utf-8",
                prefix="pipelines-mcp-eks-ca-",
                suffix=".pem",
            )
            _ = Path(ca_file.name).chmod(0o600)
            _ = ca_file.write(self._cluster.ca_pem)
            ca_file.flush()
            self._ca_file = ca_file
        config.ssl_ca_cert = self._ca_file.name


def eks_auth_from_settings(
    *,
    signer: PresignSigner | None = None,
    cluster: CachedCluster | None = None,
    clock: Clock | None = None,
) -> EksAuth:
    """Build EksAuth from the baked-in cluster and optional signer."""
    resolved_clock = time.monotonic if clock is None else clock
    resolved_cluster = _CLUSTER if cluster is None else cluster
    resolved_signer = signer
    session: Session | None = None
    sts: STSClient | None = None
    if resolved_signer is None:
        session = _boto_session()
        resolved_signer, sts = _sts_signer(session)
    return EksAuth(
        TokenMint(
            cluster_name=_EKS_CLUSTER,
            region=AWS_REGION,
            signer=resolved_signer,
            session=session,
            sts=sts,
        ),
        resolved_cluster,
        resolved_clock,
    )


def _boto_session() -> Session:
    import boto3  # noqa: PLC0415  # load on use

    return boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)


def _sts_signer(session: Session) -> tuple[PresignSigner, STSClient]:
    from botocore.signers import RequestSigner  # noqa: PLC0415  # load on use

    credentials = session.get_credentials()
    if credentials is None:
        raise SettingsError(reason="AWS credentials are missing")
    sts: STSClient = session.client("sts", region_name=AWS_REGION)
    signer = RequestSigner(
        sts.meta.service_model.service_id,
        AWS_REGION,
        "sts",
        "v4",
        credentials,
        sts.meta.events,
    )
    return signer, sts
