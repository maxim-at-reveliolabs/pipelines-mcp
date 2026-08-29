"""AWS IAM Identity Center device login. No AWS CLI."""

from __future__ import annotations

import importlib
import logging
import threading
import time
from collections.abc import Callable, Mapping
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, TypeIs

from pipelines_mcp.errors import SettingsError, SsoLoginRequiredError
from pipelines_mcp.settings import AWS_PROFILE

if TYPE_CHECKING:
    from types import ModuleType

type Sleep = Callable[[float], None]
type Announce = Callable[[str], None]
type SaveToken = Callable[[str, dict[str, str | int]], None]
type CredsOk = Callable[[], bool]
type LoadPortal = Callable[[str], SsoPortal]
type IsPending = Callable[[BaseException], bool]
type Spawn = Callable[[Callable[[], None]], None]


@dataclass(slots=True)
class _PendingLogin:
    """Holds the in-progress login URL. Mutation is the documented purpose."""

    url: str | None = None


_PENDING = _PendingLogin()


@dataclass(frozen=True, slots=True)
class SsoPortal:
    """IAM Identity Center start URL, region, and optional session name."""

    start_url: str
    region: str
    session_name: str | None


type JsonMap = Mapping[str, str | int]


class SsoOidc(Protocol):
    """Unsigned sso-oidc client used for the device grant."""

    def register_client(
        self,
        clientName: str,
        clientType: str,
        scopes: list[str],
    ) -> JsonMap:
        """Register a public OIDC client."""
        ...

    def start_device_authorization(
        self,
        clientId: str,
        clientSecret: str,
        startUrl: str,
    ) -> JsonMap:
        """Start a device authorization."""
        ...

    def create_token(
        self,
        clientId: str,
        clientSecret: str,
        grantType: str,
        deviceCode: str,
    ) -> JsonMap:
        """Exchange a device code for an access token."""
        ...


def _default_spawn(work: Callable[[], None]) -> None:
    threading.Thread(target=work, daemon=True, name="pipelines-mcp-sso").start()


def _device_login(
    profile: str,
    *,
    load_portal: LoadPortal,
    oidc: SsoOidc | None,
    sleep: Sleep,
    announce: Announce,
    save_token: SaveToken,
    is_pending: IsPending,
) -> tuple[str, Callable[[], None]]:
    portal = load_portal(profile)
    client = _live_oidc(portal.region) if oidc is None else oidc
    registered = client.register_client(
        clientName="pipelines-mcp",
        clientType="public",
        scopes=["sso:account:access"],
    )
    device = client.start_device_authorization(
        clientId=str(registered["clientId"]),
        clientSecret=str(registered["clientSecret"]),
        startUrl=portal.start_url,
    )
    url = device.get("verificationUriComplete")
    if not isinstance(url, str) or url == "":
        raise SettingsError(reason="SSO login URL is missing")

    def wait() -> None:
        interval = float(device.get("interval", 5))
        wait_seconds = 5 * 60
        deadline = time.monotonic() + wait_seconds
        token: JsonMap | None = None
        while token is None:
            try:
                token = client.create_token(
                    clientId=str(registered["clientId"]),
                    clientSecret=str(registered["clientSecret"]),
                    grantType="urn:ietf:params:oauth:grant-type:device_code",
                    deviceCode=str(device.get("deviceCode", "")),
                )
            except Exception as exc:  # boto3 pending is not a stable type
                if is_pending(exc):
                    if time.monotonic() >= deadline:
                        announce(url)
                        deadline = time.monotonic() + wait_seconds
                    sleep(interval)
                    continue
                raise SettingsError(reason="SSO login failed") from exc
        cache_key = (
            portal.session_name if portal.session_name is not None else portal.start_url
        )
        expires_in = int(token.get("expiresIn", 3600))
        payload: dict[str, str | int] = {
            "startUrl": portal.start_url,
            "region": portal.region,
            "accessToken": str(token["accessToken"]),
            "expiresAt": _iso_utc(time.time() + float(expires_in)),
            "clientId": str(registered["clientId"]),
            "clientSecret": str(registered["clientSecret"]),
            "registrationExpiresAt": _iso_utc(int(registered["clientSecretExpiresAt"])),
        }
        refresh = token.get("refreshToken")
        if isinstance(refresh, str) and refresh != "":
            payload["refreshToken"] = refresh
        save_token(cache_key, payload)
        _PENDING.url = None

    return url, wait


def request_sso(
    profile: str = AWS_PROFILE,
    *,
    creds_ok: CredsOk | None = None,
    load_portal: LoadPortal | None = None,
    oidc: SsoOidc | None = None,
    sleep: Sleep | None = None,
    announce: Announce | None = None,
    save_token: SaveToken | None = None,
    is_pending: IsPending | None = None,
    spawn: Spawn | None = None,
) -> None:
    """Start SSO login in the background and raise with the URL for the agent."""
    ok = _live_creds_ok(profile) if creds_ok is None else creds_ok()
    if ok:
        _PENDING.url = None
        return
    if _PENDING.url is not None:
        raise SsoLoginRequiredError(url=_PENDING.url)
    tell = _default_announce if announce is None else announce
    url, wait = _device_login(
        profile,
        load_portal=_live_load_portal if load_portal is None else load_portal,
        oidc=oidc,
        sleep=time.sleep if sleep is None else sleep,
        announce=tell,
        save_token=_live_save_token if save_token is None else save_token,
        is_pending=_live_is_pending if is_pending is None else is_pending,
    )
    tell(url)
    _PENDING.url = url
    starter = _default_spawn if spawn is None else spawn
    starter(wait)
    raise SsoLoginRequiredError(url=url)


def _default_announce(url: str) -> None:
    logging.getLogger("pipelines_mcp").warning(
        "Open this AWS login URL. Waiting up to five minutes: %s", url
    )


def _iso_utc(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


class _AwsHandle(Protocol):
    """Opaque AWS SDK value."""


class _CredSession(Protocol):
    def get_credentials(self) -> _AwsHandle | None:
        """Return cached AWS credentials if any."""
        ...


class _Boto3Mod(Protocol):
    def Session(self, profile_name: str) -> _CredSession:
        """Build a profile session."""
        ...

    def client(
        self, service_name: str, region_name: str, config: _AwsHandle
    ) -> SsoOidc:
        """Build a service client."""
        ...


def _is_boto3(module: ModuleType | _Boto3Mod) -> TypeIs[_Boto3Mod]:
    return hasattr(module, "Session") and hasattr(module, "client")


def sso_auth_expired(exc: BaseException) -> bool:
    """True when boto3 failed because the SSO token is missing or dead."""
    match type(exc).__name__:
        case "UnauthorizedSSOTokenError" | "TokenRetrievalError" | "SSOTokenLoadError":
            return True
        case _:
            return False


def _credentials_ready(credentials: _AwsHandle | None) -> bool:
    if credentials is None:
        return False
    freeze = getattr(credentials, "get_frozen_credentials", None)
    if not callable(freeze):
        return True
    try:
        _ = freeze()
    except Exception as exc:  # boto3 SSO errors are not a stable type
        if sso_auth_expired(exc):
            return False
        raise
    return True


def _live_creds_ok(profile: str) -> bool:
    module = importlib.import_module("boto3")
    if not _is_boto3(module):
        raise SettingsError(reason="boto3 Session is missing")
    try:
        credentials = module.Session(profile_name=profile).get_credentials()
    except Exception as exc:  # boto3 SSO errors are not a stable type
        match type(exc).__name__:
            case "ProfileNotFound":
                raise SettingsError(reason=f"AWS profile {profile} is missing") from exc
            case _:
                if sso_auth_expired(exc):
                    return False
                raise
    return _credentials_ready(credentials)


def _live_load_portal(profile: str) -> SsoPortal:
    parser = ConfigParser()
    read = parser.read(Path.home() / ".aws" / "config")
    if not read:
        raise SettingsError(reason=f"AWS profile {profile} is missing")
    section = "default" if profile == "default" else f"profile {profile}"
    if not parser.has_section(section):
        raise SettingsError(reason=f"AWS profile {profile} is missing")
    session_name = parser.get(section, "sso_session", fallback="")
    if session_name != "":
        sso_section = f"sso-session {session_name}"
        if not parser.has_section(sso_section):
            raise SettingsError(reason="SSO session is missing")
        start = parser.get(sso_section, "sso_start_url", fallback="")
        region = parser.get(sso_section, "sso_region", fallback="")
        if start == "" or region == "":
            raise SettingsError(reason="SSO session is incomplete")
        return SsoPortal(start_url=start, region=region, session_name=session_name)
    start = parser.get(section, "sso_start_url", fallback="")
    region = parser.get(section, "sso_region", fallback="")
    if start == "" or region == "":
        raise SettingsError(reason=f"AWS profile {profile} has no SSO start URL")
    return SsoPortal(start_url=start, region=region, session_name=None)


def _live_oidc(region: str) -> SsoOidc:
    boto3 = importlib.import_module("boto3")
    botocore = importlib.import_module("botocore")
    config_mod = importlib.import_module("botocore.config")
    if not _is_boto3(boto3):
        raise SettingsError(reason="boto3 client is missing")
    unsigned = getattr(botocore, "UNSIGNED", None)
    config_cls = getattr(config_mod, "Config", None)
    if unsigned is None or not callable(config_cls):
        raise SettingsError(reason="botocore Config is missing")
    return boto3.client(
        "sso-oidc",
        region_name=region,
        config=config_cls(signature_version=unsigned),
    )


def _live_is_pending(exc: BaseException) -> bool:
    text = str(exc)
    return "AuthorizationPendingException" in text or "SlowDownException" in text


def _live_save_token(key: str, token: dict[str, str | int]) -> None:
    utils = importlib.import_module("botocore.utils")
    cache_cls = getattr(utils, "JSONFileCache", None)
    loader_cls = getattr(utils, "SSOTokenLoader", None)
    if not callable(cache_cls) or not callable(loader_cls):
        raise SettingsError(reason="SSO token cache is missing")
    start_url = str(token["startUrl"])
    session_name = key if key != start_url else None
    loaded = loader_cls(cache=cache_cls(str(Path.home() / ".aws" / "sso" / "cache")))
    save = getattr(loaded, "save_token", None)
    if not callable(save):
        raise SettingsError(reason="SSO token cache is missing")
    _ = save(start_url, token, session_name=session_name)
