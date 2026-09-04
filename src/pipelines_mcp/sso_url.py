"""Hand an AWS SSO login URL to the laptop helper over TCP."""

from __future__ import annotations

import socket
from typing import Final
from urllib.parse import urlsplit

HELPER_PORT: Final = 18201
_MAX_URL_BYTES: Final = 2000
_SEND_TIMEOUT: Final = 15.0


def login_url_allowed(url: str) -> bool:
    """True when the URL is an AWS device or portal login page with no userinfo."""
    parsed = urlsplit(url)
    if parsed.scheme != "https" or "@" in parsed.netloc:
        return False
    host = parsed.hostname
    if host is None:
        return False
    host = host.casefold()
    device = host.startswith("device.sso.") and host.endswith(
        (".amazonaws.com", ".api.aws")
    )
    portal = (
        host.endswith(".awsapps.com")
        and parsed.path.rstrip("/") == "/start"
        and parsed.fragment.startswith("/device?user_code=")
    )
    return device or portal


def send_sso_url(url: str) -> bool:
    """Send one URL line to 127.0.0.1:18201. False if skipped or the helper is down."""
    text = url.strip()
    if len(text.encode()) > _MAX_URL_BYTES or not login_url_allowed(text):
        return False
    try:
        with socket.create_connection(
            ("127.0.0.1", HELPER_PORT), timeout=_SEND_TIMEOUT
        ) as sock:
            sock.sendall(f"{text}\n".encode())
    except OSError:
        return False
    return True
