import json
from collections.abc import Callable, Mapping
from typing import Final

import pytest

from pipelines_mcp.errors import SettingsError
from pipelines_mcp.settings import (
    AWS_PROFILE,
    AWS_REGION,
    load_es_auth,
    load_pipeline_auth,
)


def test_cluster_constants_are_baked_in() -> None:
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
    reader = _reader({"elasticsearch/elastic": _ES_SECRET})
    auth = load_es_auth(read_secret=reader)
    assert auth.username == "elastic"
    assert auth.password.get_secret_value() == "es-secret-pass"


def test_load_es_auth_raises_when_secret_json_is_bad() -> None:
    def read(secret_id: str) -> str:
        _ = secret_id
        return "not-json"

    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=read)


def test_load_es_auth_raises_when_password_is_blank() -> None:
    reader = _reader(
        {"elasticsearch/elastic": {"username": "elastic", "password": "  "}}
    )
    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=reader)


def test_load_es_auth_raises_when_secret_keys_missing() -> None:
    reader = _reader({"elasticsearch/elastic": {"username": "elastic"}})
    with pytest.raises(SettingsError):
        _ = load_es_auth(read_secret=reader)


def test_repr_hides_loaded_secret_values() -> None:
    reader = _reader(
        {
            "elasticsearch/elastic": {
                "username": "elastic",
                "password": "super-secret-es-pass",
            },
        }
    )
    auth = load_es_auth(read_secret=reader)
    assert "super-secret-es-pass" not in repr(auth)


_PIPELINE_AUTH_KEY: Final = (
    "pipelines/prod/service_pipelines_user_prod@reveliolabs.com"
)


def test_load_pipeline_auth_reads_user_and_password() -> None:
    reader = _reader(
        {
            _PIPELINE_AUTH_KEY: {
                "user": "service_pipelines_user_prod@reveliolabs.com",
                "password": "pipeline-secret-pass",
            }
        }
    )
    auth = load_pipeline_auth(read_secret=reader)
    assert auth.username == "service_pipelines_user_prod@reveliolabs.com"
    assert auth.password.get_secret_value() == "pipeline-secret-pass"
