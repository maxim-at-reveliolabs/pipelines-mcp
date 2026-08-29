from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Final

import pytest

from pipelines_mcp.errors import SettingsError, SsoLoginRequiredError
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
    request_sso("reveliolabs", creds_ok=lambda: True)
    yield
    request_sso("reveliolabs", creds_ok=lambda: True)


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


def _run_wait(work: Callable[[], None]) -> None:
    work()


def test_request_sso_skips_oidc_when_credentials_work() -> None:
    # Given: AWS credentials for the profile already work
    oidc = FakeOidc()
    announced: list[str] = []

    # When: requesting SSO
    request_sso(
        "reveliolabs",
        creds_ok=lambda: True,
        load_portal=lambda _profile: _PORTAL,
        oidc=oidc,
        announce=announced.append,
        save_token=oidc.saved.__setitem__,
    )

    # Then: no login URL and no device token
    assert announced == []
    assert oidc.saved == {}


@pytest.mark.parametrize(
    ("pending", "expected_sleeps"),
    [
        (1, [1]),
        (2, [1, 1]),
    ],
)
def test_request_sso_waits_then_saves_token(
    pending: int, expected_sleeps: list[int]
) -> None:
    # Given: credentials do not work and the device grant succeeds after pending polls
    oidc = FakeOidc(pending=pending)
    announced: list[str] = []
    sleeps: list[float] = []

    # When: requesting SSO and running the waiter immediately
    with pytest.raises(SsoLoginRequiredError):
        request_sso(
            "reveliolabs",
            creds_ok=lambda: False,
            load_portal=lambda _profile: _PORTAL,
            oidc=oidc,
            sleep=sleeps.append,
            announce=announced.append,
            save_token=oidc.saved.__setitem__,
            is_pending=lambda exc: isinstance(exc, FakePendingError),
            spawn=_run_wait,
        )

    # Then: the login URL is announced, it waited, then a token is stored
    assert announced == [_URL]
    assert sleeps == expected_sleeps
    assert oidc.saved["revelio-sso"]["accessToken"] == "access-token"


def test_request_sso_raises_when_profile_portal_missing() -> None:
    # Given: the AWS profile has no SSO portal
    def load_portal(profile: str) -> SsoPortal:
        raise SettingsError(reason=f"AWS profile {profile} is missing")

    # When: requesting SSO
    # Then: a typed settings error is raised
    with pytest.raises(SettingsError, match="missing"):
        request_sso(
            "reveliolabs",
            creds_ok=lambda: False,
            load_portal=load_portal,
            oidc=FakeOidc(),
        )


def test_request_sso_raises_url_and_starts_wait_in_background() -> None:
    # Given: credentials do not work
    oidc = FakeOidc(pending=0)
    announced: list[str] = []
    started: list[Callable[[], None]] = []

    def spawn(work: Callable[[], None]) -> None:
        started.append(work)

    # When: requesting SSO for the agent to show the human
    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso(
            "reveliolabs",
            creds_ok=lambda: False,
            load_portal=lambda _profile: _PORTAL,
            oidc=oidc,
            announce=announced.append,
            save_token=oidc.saved.__setitem__,
            is_pending=lambda exc: isinstance(exc, FakePendingError),
            spawn=spawn,
        )

    # Then: the URL is on the error and a waiter was started
    assert caught.value.url == _URL
    assert _URL in str(caught.value)
    assert announced == [_URL]
    assert len(started) == 1


def test_request_sso_reuses_url_while_login_still_running() -> None:
    # Given: a login was already started
    oidc = FakeOidc(pending=0)
    with pytest.raises(SsoLoginRequiredError):
        request_sso(
            "reveliolabs",
            creds_ok=lambda: False,
            load_portal=lambda _profile: _PORTAL,
            oidc=oidc,
            announce=lambda _url: None,
            save_token=oidc.saved.__setitem__,
            spawn=lambda _work: None,
        )

    # When: requesting SSO again
    with pytest.raises(SsoLoginRequiredError) as caught:
        request_sso("reveliolabs", creds_ok=lambda: False)

    # Then: the same URL is returned without starting a new login
    assert caught.value.url == _URL
