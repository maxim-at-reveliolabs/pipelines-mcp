from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Final

import pytest

from pipelines_mcp.errors import DomainError, SsoLoginRequiredError
from pipelines_mcp.settings_sso import SsoPortal, request_sso

_URL: Final = "https://device.sso.example.test/?user_code=ABCD-EFGH"
_START: Final = "https://sso.example.test/start"
_PORTAL: Final = SsoPortal(
    start_url=_START,
    region="us-east-2",
    session_name="revelio-sso",
)


@pytest.fixture(autouse=True)
def reset_pending_login() -> Iterator[None]:
    request_sso(creds_ok=lambda: True)
    yield
    request_sso(creds_ok=lambda: True)


@dataclass(slots=True)
class FakeOidc:
    pending: int = 1
    saved: dict[str, dict[str, str | int]] = field(default_factory=dict)

    def register_client(
        self,
        clientName: str,
        clientType: str,
        scopes: list[str],
    ) -> dict[str, str | int]:
        _ = clientName, clientType, scopes
        return {
            "clientId": "cid",
            "clientSecret": "csecret",
            "clientSecretExpiresAt": 9_999_999_999,
        }

    def start_device_authorization(
        self,
        clientId: str,
        clientSecret: str,
        startUrl: str,
    ) -> dict[str, str | int]:
        _ = clientId, clientSecret, startUrl
        return {
            "deviceCode": "dev",
            "userCode": "ABCD-EFGH",
            "verificationUri": "https://device.sso.example.test/",
            "verificationUriComplete": _URL,
            "interval": 1,
            "expiresIn": 600,
        }

    def create_token(
        self,
        clientId: str,
        clientSecret: str,
        grantType: str,
        deviceCode: str,
    ) -> dict[str, str | int]:
        _ = clientId, clientSecret, grantType, deviceCode
        if self.pending > 0:
            self.pending -= 1
            raise FakePendingError
        return {
            "accessToken": "access-token",
            "expiresIn": 3600,
            "refreshToken": "refresh-token",
        }


class FakePendingError(Exception):
    pass


def test_request_sso_skips_oidc_when_credentials_work() -> None:
    oidc = FakeOidc()
    announced: list[str] = []
    request_sso(
        creds_ok=lambda: True,
        load_portal=lambda: _PORTAL,
        oidc=oidc,
        announce=announced.append,
        save_token=oidc.saved.__setitem__,
    )
    assert announced == []
    assert oidc.saved == {}


def test_request_sso_waits_then_saves_token() -> None:
    oidc = FakeOidc(pending=2)
    announced: list[str] = []
    sleeps: list[float] = []
    with pytest.raises(SsoLoginRequiredError):
        request_sso(
            creds_ok=lambda: False,
            load_portal=lambda: _PORTAL,
            oidc=oidc,
            sleep=sleeps.append,
            announce=announced.append,
            save_token=oidc.saved.__setitem__,
            is_pending=lambda exc: isinstance(exc, FakePendingError),
            spawn=lambda work: work(),
        )
    assert announced == [_URL]
    assert sleeps == [1, 1]
    assert oidc.saved["revelio-sso"]["accessToken"] == "access-token"


def test_request_sso_raises_when_profile_portal_missing() -> None:
    def load_portal() -> SsoPortal:
        raise DomainError(reason="AWS profile reveliolabs is missing")

    with pytest.raises(DomainError, match="missing"):
        request_sso(
            creds_ok=lambda: False,
            load_portal=load_portal,
            oidc=FakeOidc(),
        )


def test_request_sso_raises_url_and_starts_wait_in_background() -> None:
    oidc = FakeOidc(pending=0)
    announced: list[str] = []
    started: list[Callable[[], None]] = []

    def spawn(work: Callable[[], None]) -> None:
        started.append(work)

    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso(
            creds_ok=lambda: False,
            load_portal=lambda: _PORTAL,
            oidc=oidc,
            announce=announced.append,
            save_token=oidc.saved.__setitem__,
            is_pending=lambda exc: isinstance(exc, FakePendingError),
            spawn=spawn,
        )
    assert caught.value.url == _URL
    assert _URL in str(caught.value)
    assert announced == [_URL]
    assert len(started) == 1


def test_request_sso_reuses_url_while_login_still_running() -> None:
    oidc = FakeOidc(pending=0)
    with pytest.raises(SsoLoginRequiredError):
        request_sso(
            creds_ok=lambda: False,
            load_portal=lambda: _PORTAL,
            oidc=oidc,
            announce=lambda _url: None,
            save_token=oidc.saved.__setitem__,
            spawn=lambda _work: None,
        )
    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso(creds_ok=lambda: False)
    assert caught.value.url == _URL


def test_request_sso_sends_url_to_helper_without_announcing() -> None:
    oidc = FakeOidc(pending=0)
    announced: list[str] = []
    sent: list[str] = []

    def send(url: str) -> bool:
        sent.append(url)
        return True

    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso(
            creds_ok=lambda: False,
            load_portal=lambda: _PORTAL,
            oidc=oidc,
            announce=announced.append,
            send_url=send,
            save_token=oidc.saved.__setitem__,
            spawn=lambda _work: None,
        )
    assert sent == [_URL]
    assert announced == []
    assert caught.value.helper is True
    assert _URL not in str(caught.value)


def test_request_sso_reuses_helper_handoff_without_url() -> None:
    oidc = FakeOidc(pending=0)
    with pytest.raises(SsoLoginRequiredError):
        request_sso(
            creds_ok=lambda: False,
            load_portal=lambda: _PORTAL,
            oidc=oidc,
            announce=lambda _url: None,
            send_url=lambda _url: True,
            save_token=oidc.saved.__setitem__,
            spawn=lambda _work: None,
        )
    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso(creds_ok=lambda: False)
    assert caught.value.helper is True
    assert _URL not in str(caught.value)
