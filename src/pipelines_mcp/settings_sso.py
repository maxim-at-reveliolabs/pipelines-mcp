"""AWS IAM Identity Center device login. Hands the URL to the local helper."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from types_boto3_sso_oidc.client import SSOOIDCClient

from pipelines_mcp.errors import SettingsError, SsoLoginRequiredError
from pipelines_mcp.settings import AWS_PROFILE
from pipelines_mcp.sso_url import send_sso_url

type Sleep = Callable[[float], None]
type Announce = Callable[[str], None]
type SaveToken = Callable[[str, dict[str, str | int]], None]
type CredsOk = Callable[[], bool]
type LoadPortal = Callable[[str], SsoPortal]
type IsPending = Callable[[BaseException], bool]
type Spawn = Callable[[Callable[[], None]], None]
type SendUrl = Callable[[str], bool]


@dataclass(slots=True)
class _PendingLogin:
    """Holds the in-progress login URL. Mutation is the documented purpose."""

    url: str | None = None
    helper: bool = False


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
    ) -> JsonMap: ...

    def start_device_authorization(
        self,
        clientId: str,
        clientSecret: str,
        startUrl: str,
    ) -> JsonMap: ...

    def create_token(
        self,
        clientId: str,
        clientSecret: str,
        grantType: str,
        deviceCode: str,
    ) -> JsonMap: ...


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
    client: SsoOidc | SSOOIDCClient = (
        oidc if oidc is not None else _live_oidc(portal.region)
    )
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
        token = None
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
        _PENDING.helper = False

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
    send_url: SendUrl | None = None,
) -> None:
    """Start SSO login in the background. The helper gets the URL when it is up."""
    ok = _live_creds_ok(profile) if creds_ok is None else creds_ok()
    if ok:
        _PENDING.url = None
        _PENDING.helper = False
        return
    if _PENDING.url is not None:
        raise SsoLoginRequiredError(url=_PENDING.url, helper=_PENDING.helper)
    tell = _default_announce if announce is None else announce
    send = send_sso_url if send_url is None else send_url
    handed_off = False

    def deliver(page: str) -> None:
        nonlocal handed_off
        if send(page):
            handed_off = True
            return
        handed_off = False
        tell(page)

    url, wait = _device_login(
        profile,
        load_portal=_live_load_portal if load_portal is None else load_portal,
        oidc=oidc,
        sleep=time.sleep if sleep is None else sleep,
        announce=deliver,
        save_token=_live_save_token if save_token is None else save_token,
        is_pending=_live_is_pending if is_pending is None else is_pending,
    )
    deliver(url)
    _PENDING.url = url
    _PENDING.helper = handed_off
    starter = _default_spawn if spawn is None else spawn
    starter(wait)
    raise SsoLoginRequiredError(url=url, helper=handed_off)


def _default_announce(url: str) -> None:
    logging.getLogger("pipelines_mcp").warning(
        "Open this AWS login URL. Waiting up to five minutes: %s", url
    )


def _iso_utc(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(epoch))


def sso_auth_expired(exc: BaseException) -> bool:
    """True when boto3 failed because the SSO token is missing or dead."""
    from botocore.exceptions import (  # noqa: PLC0415  # load on use
        SSOTokenLoadError,
        TokenRetrievalError,
        UnauthorizedSSOTokenError,
    )

    match exc:
        case UnauthorizedSSOTokenError() | TokenRetrievalError() | SSOTokenLoadError():
            return True
        case _:
            return False


def _live_creds_ok(profile: str) -> bool:
    import boto3  # noqa: PLC0415  # load on use
    from botocore.exceptions import ProfileNotFound  # noqa: PLC0415  # load on use

    try:
        credentials = boto3.Session(profile_name=profile).get_credentials()
    except ProfileNotFound as exc:
        raise SettingsError(reason=f"AWS profile {profile} is missing") from exc
    except Exception as exc:  # boto3 SSO errors are not a stable type
        if sso_auth_expired(exc):
            return False
        raise
    if credentials is None:
        return False
    try:
        _ = credentials.get_frozen_credentials()
    except Exception as exc:  # boto3 SSO errors are not a stable type
        if sso_auth_expired(exc):
            return False
        raise
    return True


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


def _live_oidc(region: str) -> SSOOIDCClient:
    import boto3  # noqa: PLC0415  # load on use
    from botocore.config import Config  # noqa: PLC0415  # load on use

    client: SSOOIDCClient = boto3.client(
        "sso-oidc",
        region_name=region,
        config=Config(signature_version="unsigned"),
    )
    return client


def _live_is_pending(exc: BaseException) -> bool:
    text = str(exc)
    return "AuthorizationPendingException" in text or "SlowDownException" in text


def _live_save_token(key: str, token: dict[str, str | int]) -> None:
    import hashlib  # noqa: PLC0415  # load on use

    from botocore.utils import JSONFileCache  # noqa: PLC0415  # load on use

    start_url = str(token["startUrl"])
    session_name = key if key != start_url else None
    material = start_url if session_name is None else session_name
    cache_key = hashlib.sha1(
        material.encode("utf-8"), usedforsecurity=False
    ).hexdigest()
    cache = JSONFileCache(str(Path.home() / ".aws" / "sso" / "cache"))
    cache[cache_key] = token
