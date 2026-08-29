"""Typed domain errors."""

from dataclasses import dataclass
from typing import override


@dataclass(frozen=True, slots=True)
class SettingsError(Exception):
    """Settings failed to load or failed a parse check."""

    reason: str

    @override
    def __str__(self) -> str:
        """Return the parse failure reason."""
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
    """AWS SSO device login is in progress. The URL is for the human."""

    url: str

    @override
    def __str__(self) -> str:
        """Ask the human to open the login URL, then retry."""
        return (
            "AWS login required. Open this URL, then retry the same request: "
            f"{self.url}"
        )


@dataclass(frozen=True, slots=True)
class InvalidCursorError(Exception):
    """A log cursor is malformed or does not match this call."""

    @override
    def __str__(self) -> str:
        """Return a stable invalid-cursor message."""
        return "invalid cursor"


@dataclass(frozen=True, slots=True)
class EmptyQueryError(Exception):
    """A required query field was empty after strip."""

    field: str

    @override
    def __str__(self) -> str:
        """Return a stable empty-field message."""
        return f"empty {self.field}"
