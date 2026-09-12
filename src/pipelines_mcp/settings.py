"""AWS deploy values and username/password secrets."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, ClassVar, Final

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StringConstraints,
    ValidationError,
    field_validator,
)

from pipelines_mcp.errors import DomainError

if TYPE_CHECKING:
    from types_boto3_secretsmanager.client import SecretsManagerClient

AWS_REGION: Final = "us-east-2"
AWS_PROFILE: Final = "reveliolabs"

type SecretReader = Callable[[str], str]


class BasicAuth(BaseModel):
    """Username and password."""

    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, extra="ignore")
    username: Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)] = (
        Field(validation_alias=AliasChoices("username", "user"))
    )
    password: SecretStr = Field(min_length=1)

    @field_validator("password", mode="before")
    @classmethod
    def strip_password(cls, value: str | SecretStr) -> str:
        """Strip surrounding whitespace from the password."""
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        return raw.strip()


def _aws_read(secret_id: str) -> str:
    import boto3  # noqa: PLC0415  # load on use
    from botocore.exceptions import ClientError  # noqa: PLC0415  # load on use

    session = boto3.Session(region_name=AWS_REGION)
    client: SecretsManagerClient = session.client("secretsmanager")
    try:
        response = client.get_secret_value(SecretId=secret_id)
    except ClientError as exc:
        raise DomainError(reason="secret read failed") from exc
    secret = response.get("SecretString", "")
    if secret == "":
        raise DomainError(reason="secret is empty")
    return secret


def _load_auth(secret_id: str, *, read_secret: SecretReader | None) -> BasicAuth:
    reader = _aws_read if read_secret is None else read_secret
    try:
        return BasicAuth.model_validate_json(reader(secret_id))
    except (ValidationError, ValueError) as exc:
        raise DomainError(reason="secret is invalid") from exc


def load_es_auth(*, read_secret: SecretReader | None = None) -> BasicAuth:
    """Read Elasticsearch basic auth from AWS Secrets Manager."""
    return _load_auth("elasticsearch/elastic", read_secret=read_secret)


def load_pipeline_auth(*, read_secret: SecretReader | None = None) -> BasicAuth:
    """Read pipeline service login from AWS Secrets Manager."""
    return _load_auth(
        "pipelines/prod/service_pipelines_user_prod@reveliolabs.com",
        read_secret=read_secret,
    )
