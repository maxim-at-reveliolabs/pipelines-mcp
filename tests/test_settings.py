import json
from collections.abc import Callable, Mapping
from typing import Final

import pytest

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.settings import AWS_PROFILE, AWS_REGION, load_es_auth


def test_cluster_constants_are_baked_in() -> None:
    # Given: shared AWS constants
    # When: they are read
    # Then: the baked-in values are unchanged
    assert AWS_REGION == "us-east-2"
    assert AWS_PROFILE == "reveliolabs"


_ES_SECRET: Final[dict[str, str]] = {
    "username": "elastic",
    "password": "es-secret-pass",
}


def _reader(secrets: Mapping[str, Mapping[str, str]]) -> Callable[[str], str]:
    def read(secret_id: str) -> str:
        try:
            return json.dumps(secrets[secret_id])
        except KeyError:
            raise SettingsError(reason="secret is missing") from None

    return read


def test_load_es_auth_reads_username_and_password() -> None:
    # Given: a secret with elasticsearch basic auth
    reader = _reader({"elasticsearch/elastic": _ES_SECRET})

    # When: the secret is read
    auth = load_es_auth(read_secret=reader)

    # Then: username and password come from the secret
    assert auth.username == "elastic"
    assert auth.password.get_secret_value() == "es-secret-pass"


def test_load_es_auth_raises_when_secret_json_is_bad() -> None:
    # Given: the elasticsearch secret is not JSON
    def read(secret_id: str) -> str:
        _ = secret_id
        return "not-json"

    # When: the secret is read
    # Then: a typed settings error is raised
    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=read)


def test_load_es_auth_raises_when_password_is_blank() -> None:
    # Given: the elasticsearch secret has a whitespace password
    reader = _reader(
        {"elasticsearch/elastic": {"username": "elastic", "password": "  "}}
    )

    # When: the secret is read
    # Then: a typed settings error is raised
    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=reader)


def test_load_es_auth_raises_when_secret_keys_missing() -> None:
    # Given: the elasticsearch secret is JSON but missing password
    reader = _reader({"elasticsearch/elastic": {"username": "elastic"}})

    # When: the secret is read
    # Then: a typed settings error is raised
    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=reader)


def test_repr_hides_loaded_secret_values() -> None:
    # Given: a distinctive secret value pulled from AWS JSON
    reader = _reader(
        {
            "elasticsearch/elastic": {
                "username": "elastic",
                "password": "super-secret-es-pass",
            },
        }
    )

    # When: the secret is read and turned into a repr
    auth = load_es_auth(read_secret=reader)
    text = repr(auth)

    # Then: the secret value does not appear in the repr
    assert "super-secret-es-pass" not in text
