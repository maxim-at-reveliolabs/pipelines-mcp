"""AWS deploy values and Elasticsearch auth from AWS Secrets Manager."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Annotated,
    ClassVar,
    Final,
    Protocol,
    TypedDict,
    TypeIs,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationError,
    field_validator,
)

from pipelines_mcp.errors import SettingsError

AWS_REGION: Final = "us-east-2"
AWS_PROFILE: Final = "reveliolabs"

if TYPE_CHECKING:
    from types import ModuleType

type SecretReader = Callable[[str], str]


class EsAuth(BaseModel):
    """Username and password for the log store."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    username: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
    password: SecretStr = Field(min_length=1)

    @field_validator("password", mode="before")
    @classmethod
    def strip_password(cls, value: str | SecretStr) -> str:
        """Strip surrounding whitespace from the password."""
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        return raw.strip()


class _SecretValue(TypedDict, total=False):
    SecretString: str


class _SecretsClient(Protocol):
    def get_secret_value(self, SecretId: str) -> _SecretValue:
        """Return one Secrets Manager payload."""
        ...


class _AwsSession(Protocol):
    def client(self, service_name: str) -> _SecretsClient:
        """Build a service client."""
        ...


class _Boto3Module(Protocol):
    Session: Callable[..., _AwsSession]


class _ExcMod(Protocol):
    ClientError: type[BaseException]


def _is_boto3(module: ModuleType | _Boto3Module) -> TypeIs[_Boto3Module]:
    return hasattr(module, "Session")


def _is_exc(module: ModuleType | _ExcMod) -> TypeIs[_ExcMod]:
    return hasattr(module, "ClientError")


def _aws_read(secret_id: str) -> str:
    module = importlib.import_module("boto3")
    if not _is_boto3(module):
        raise SettingsError(reason="boto3 Session is missing")
    exc_mod = importlib.import_module("botocore.exceptions")
    if not _is_exc(exc_mod):
        raise SettingsError(reason="botocore ClientError is missing")
    session = module.Session(region_name=AWS_REGION)
    try:
        response = session.client("secretsmanager").get_secret_value(
            SecretId=secret_id
        )
    except exc_mod.ClientError as exc:
        raise SettingsError(reason="secret read failed") from exc
    secret = response.get("SecretString")
    if secret is None or secret == "":
        raise SettingsError(reason="secret is empty")
    return secret


def load_es_auth(*, read_secret: SecretReader | None = None) -> EsAuth:
    """Read Elasticsearch basic auth from AWS Secrets Manager."""
    reader = _aws_read if read_secret is None else read_secret
    try:
        return EsAuth.model_validate_json(reader("elasticsearch/elastic"))
    except (ValidationError, ValueError) as exc:
        raise SettingsError(reason="secret is invalid") from exc
