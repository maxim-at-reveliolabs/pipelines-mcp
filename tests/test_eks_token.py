from __future__ import annotations

import base64
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from pipelines_mcp.eks_token import (
    CachedCluster,
    EksAuth,
    PresignRequest,
    TokenMint,
    eks_auth_from_settings,
    mint_token,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_PEM: str = "-----BEGIN CERTIFICATE-----\nMIIBtest\n-----END CERTIFICATE-----\n"
_PRESIGNED: str = (
    "https://sts.ap-south-1.amazonaws.com/?Action=GetCallerIdentity"
    "&Version=2011-06-15&X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-SignedHeaders=host%3Bx-k8s-aws-id"
)
_CLUSTER = CachedCluster(
    endpoint="https://eks.ap-south-1.example.test",
    ca_pem=_PEM,
)


@dataclass(frozen=True, slots=True)
class FakeSigner:
    """Records presign URL and headers. Mutation is the recorded calls."""

    url: str = _PRESIGNED
    calls: list[PresignRequest] = field(default_factory=list)
    expires: list[int] = field(default_factory=list)
    regions: list[str | None] = field(default_factory=list)

    def generate_presigned_url(
        self,
        request_dict: PresignRequest,
        operation_name: str,
        expires_in: int = 3600,
        region_name: str | None = None,
        signing_name: str | None = None,
    ) -> str:
        del operation_name, signing_name
        self.calls.append(request_dict)
        self.expires.append(expires_in)
        self.regions.append(region_name)
        return f"{self.url}&n={len(self.calls)}"


@dataclass(slots=True)
class FakeClock:
    """Injected clock. Mutation is the current time."""

    now_value: float = 0.0

    def now(self) -> float:
        return self.now_value

    def advance(self, seconds: float) -> None:
        self.now_value += seconds


@dataclass(slots=True)
class FakeKubeConfig:
    """kubernetes Configuration stand-in. Mutation is the client contract."""

    host: str = ""
    api_key: dict[str, str] = field(default_factory=dict)
    api_key_prefix: dict[str, str] = field(default_factory=dict)
    ssl_ca_cert: str | None = None
    refresh_api_key_hook: Callable[..., None] | None = None


@dataclass(slots=True)
class FakeKubeConfigWithCaData(FakeKubeConfig):
    """Configuration that accepts in-memory PEM."""

    ca_cert_data: str | None = None


def _auth(
    signer: FakeSigner,
    clock: FakeClock,
    cluster: CachedCluster = _CLUSTER,
) -> EksAuth:
    return eks_auth_from_settings(
        signer=signer,
        cluster=cluster,
        clock=clock.now,
    )


def test_mint_token_presigns_sts_with_cluster_header() -> None:
    signer = FakeSigner()

    token = mint_token(
        TokenMint(cluster_name="prod-cluster", region="ap-south-1", signer=signer)
    )

    expected_url = f"{_PRESIGNED}&n=1"
    expected = "k8s-aws-v1." + base64.urlsafe_b64encode(
        expected_url.encode("utf-8")
    ).decode("ascii").rstrip("=")
    assert token == expected
    assert not token.removeprefix("k8s-aws-v1.").endswith("=")
    assert signer.calls[0]["headers"]["x-k8s-aws-id"] == "prod-cluster"
    assert signer.calls[0]["method"] == "GET"
    assert (
        signer.calls[0]["url"]
        == "https://sts.ap-south-1.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
    )
    assert signer.expires == [900]
    assert signer.regions == ["ap-south-1"]


def test_refresh_keeps_token_when_under_14_minutes() -> None:
    signer = FakeSigner()
    clock = FakeClock()
    auth = _auth(signer, clock)
    config = FakeKubeConfig()
    auth.bind(config)
    first = config.api_key["BearerToken"]

    clock.advance(14 * 60 - 1)
    hook = config.refresh_api_key_hook
    assert hook is not None
    hook(config)

    assert config.api_key["BearerToken"] == first
    assert len(signer.calls) == 1


def test_refresh_remints_when_14_minutes_elapsed() -> None:
    signer = FakeSigner()
    clock = FakeClock()
    auth = _auth(signer, clock)
    config = FakeKubeConfig()
    auth.bind(config)
    first = config.api_key["BearerToken"]

    clock.advance(14 * 60)
    hook = config.refresh_api_key_hook
    assert hook is not None
    hook(config)

    assert config.api_key["BearerToken"] != first
    assert len(signer.calls) == 2
    assert config.api_key["BearerToken"].startswith("k8s-aws-v1.")


def test_bind_prefers_ca_cert_data_when_present() -> None:
    signer = FakeSigner()
    clock = FakeClock()
    auth = _auth(signer, clock)
    config = FakeKubeConfigWithCaData()

    auth.bind(config)

    assert config.ca_cert_data == _PEM
    assert config.ssl_ca_cert is None


def test_bind_uses_0600_tempfile_when_no_ca_cert_data() -> None:
    signer = FakeSigner()
    clock = FakeClock()
    auth = _auth(signer, clock)
    config = FakeKubeConfig()

    auth.bind(config)

    assert config.ssl_ca_cert is not None
    path = Path(config.ssl_ca_cert)
    assert path.read_text(encoding="utf-8") == _PEM
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_bind_sets_host_bearer_prefix_and_refresh_hook() -> None:
    signer = FakeSigner()
    clock = FakeClock()
    auth = _auth(signer, clock)
    config = FakeKubeConfig()

    auth.bind(config)

    assert config.host == _CLUSTER.endpoint
    assert config.api_key_prefix["BearerToken"] == "Bearer"
    assert config.refresh_api_key_hook is not None
    assert config.api_key["BearerToken"].startswith("k8s-aws-v1.")


def test_from_settings_uses_hardcoded_cluster_name_and_region() -> None:
    signer = FakeSigner()
    clock = FakeClock()

    auth = eks_auth_from_settings(
        signer=signer,
        cluster=_CLUSTER,
        clock=clock.now,
    )
    auth.bind(FakeKubeConfig())

    assert signer.calls[0]["headers"]["x-k8s-aws-id"] == "dev"
    assert (
        signer.calls[0]["url"]
        == "https://sts.us-east-2.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15"
    )
