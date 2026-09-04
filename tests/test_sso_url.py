import socket
import threading
from queue import Queue

import pytest

from pipelines_mcp.sso_url import HELPER_PORT, login_url_allowed, send_sso_url


@pytest.mark.parametrize(
    "url",
    [
        "https://device.sso.us-east-2.amazonaws.com/?user_code=ABCD-EFGH",
        "https://device.sso.us-east-2.api.aws/?user_code=ABCD-EFGH",
        "https://d-1234567890.awsapps.com/start#/device?user_code=ABCD-EFGH",
        "https://client.awsapps.com/start/#/device?user_code=ABCD-EFGH",
    ],
)
def test_login_url_allowed(url: str) -> None:
    assert login_url_allowed(url) is True


@pytest.mark.parametrize(
    "url",
    [
        "https://oidc.us-east-2.amazonaws.com/authorize?x=1",
        "http://device.sso.us-east-2.amazonaws.com/?user_code=ABCD-EFGH",
        "https://device.sso.example.test/?user_code=ABCD-EFGH",
        "https://evil.example/phishing",
        "https://device.sso.us-east-2.amazonaws.com.evil.example/",
        "https://device.sso.us-east-2.amazonaws.com@evil.example/",
        "https://d-1234567890.awsapps.com/start",
        "file:///etc/passwd",
        "",
    ],
)
def test_login_url_rejected(url: str) -> None:
    assert login_url_allowed(url) is False


def test_send_sso_url_false_when_helper_down() -> None:
    url = "https://device.sso.us-east-2.amazonaws.com/?user_code=ABCD-EFGH"
    if _port_open():
        pytest.skip("helper is listening")
    assert send_sso_url(url) is False


def test_send_sso_url_writes_one_line() -> None:
    if _port_open():
        pytest.skip("helper is listening")
    received: Queue[bytes] = Queue()
    url = "https://device.sso.us-east-2.amazonaws.com/?user_code=ABCD-EFGH"
    with socket.create_server(("127.0.0.1", HELPER_PORT)) as server:

        def accept() -> None:
            conn = server.accept()[0]
            with conn:
                received.put(conn.recv(2048))

        worker = threading.Thread(target=accept, daemon=True)
        worker.start()

        sent = send_sso_url(url)
        worker.join(timeout=2)

    assert sent is True
    assert received.get_nowait() == f"{url}\n".encode()


def test_send_sso_url_skips_disallowed_without_connecting() -> None:
    assert send_sso_url("https://evil.example/phishing") is False


def _port_open() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", HELPER_PORT), timeout=0.05):
            return True
    except OSError:
        return False
