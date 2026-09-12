"""Typed domain errors."""

from dataclasses import dataclass
from typing import override


@dataclass(frozen=True, slots=True)
class SettingsError(Exception):
    """A tool call failed with a stable message."""

    reason: str

    @override
    def __str__(self) -> str:
        """Return the failure reason."""
        return self.reason


@dataclass(frozen=True, slots=True)
class NotFoundError(Exception):
    """A requested pipeline object does not exist."""

    entity: str

    @override
    def __str__(self) -> str:
        """Return a stable not-found message."""
        return f"{self.entity} not found"


@dataclass(frozen=True, slots=True)
class SsoLoginRequiredError(Exception):
    """AWS SSO device login is in progress."""

    url: str
    helper: bool = False

    @override
    def __str__(self) -> str:
        """Retry. Omit the URL when the helper has it."""
        if self.helper:
            return "AWS login started. Retry the same request."
        return (
            "AWS login required. Open this URL, then retry the same request: "
            f"{self.url}"
        )
