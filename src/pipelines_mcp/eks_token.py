"""EKS bearer token via botocore RequestSigner. No AWS CLI."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

import base64
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Protocol,
    TypedDict,
    TypeIs,
    final,
    override,
)

from pipelines_mcp.settings import AWS_PROFILE, AWS_REGION

if TYPE_CHECKING:
    from boto3.session import Session
    from types_boto3_sts.client import STSClient

type Clock = Callable[[], float]


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


class KubeClientConfigWithCaData(KubeClientConfig, Protocol):
    """Configuration that accepts PEM in memory."""

    ca_cert_data: str


@dataclass(frozen=True, slots=True)
class CachedCluster:
    """Cluster API endpoint and decoded CA PEM."""

    endpoint: str
    ca_pem: str


@dataclass(frozen=True, slots=True)
class TokenMint:
    """Inputs needed to mint one EKS token."""

    cluster_name: str
    region: str
    signer: PresignSigner
    # botocore RequestSigner weakrefs the STS client; keep session and client alive.
    session: Session | None = None
    sts: STSClient | None = None


@dataclass(frozen=True, slots=True)
class EksAuthError(Exception):
    """EKS token or cluster lookup failed."""

    reason: str

    @override
    def __str__(self) -> str:
        """Return the failure reason."""
        return self.reason


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


@final
class EksAuth:
    """Caches cluster CA and remints the bearer token.

    Mutation is required: token timestamp and the CA tempfile stay on this
    object for process lifetime.
    """

    def __init__(self, mint: TokenMint, cluster: CachedCluster, clock: Clock) -> None:
        """Store mint inputs, cached cluster, and clock."""
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
        pem = self._cluster.ca_pem
        if _has_ca_cert_data(config):
            config.ca_cert_data = pem
            return
        if self._ca_file is None:
            # Must stay open for process lifetime; closing deletes the PEM.
            ca_file = tempfile.NamedTemporaryFile(  # noqa: SIM115  # must stay open
                mode="w",
                encoding="utf-8",
                prefix="pipelines-mcp-eks-ca-",
                suffix=".pem",
            )
            _ = Path(ca_file.name).chmod(0o600)
            _ = ca_file.write(pem)
            ca_file.flush()
            self._ca_file = ca_file
        config.ssl_ca_cert = self._ca_file.name


def eks_auth_from_settings(
    *,
    signer: PresignSigner | None = None,
    cluster: CachedCluster,
    clock: Clock | None = None,
) -> EksAuth:
    """Build EksAuth from a cluster and optional signer."""
    cluster_name = "dev"
    resolved_clock = time.monotonic if clock is None else clock
    resolved_signer = signer
    session: Session | None = None
    sts: STSClient | None = None
    if resolved_signer is None:
        session = _boto_session()
        resolved_signer, sts = _sts_signer(session)
    return EksAuth(
        TokenMint(
            cluster_name=cluster_name,
            region=AWS_REGION,
            signer=resolved_signer,
            session=session,
            sts=sts,
        ),
        cluster,
        resolved_clock,
    )


def _has_ca_cert_data(config: KubeClientConfig) -> TypeIs[KubeClientConfigWithCaData]:
    return hasattr(config, "ca_cert_data")


def _boto_session() -> Session:
    import boto3  # noqa: PLC0415  # load on use

    return boto3.Session(region_name=AWS_REGION, profile_name=AWS_PROFILE)


def _sts_signer(session: Session) -> tuple[PresignSigner, STSClient]:
    from botocore.signers import RequestSigner  # noqa: PLC0415  # load on use

    credentials = session.get_credentials()
    if credentials is None:
        raise EksAuthError(reason="AWS credentials are missing")
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
