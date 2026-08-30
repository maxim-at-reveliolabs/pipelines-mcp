"""Replace credential-shaped substrings in tool output."""

import re
from typing import Final

_PLACEHOLDER: Final = "[redacted]"
_RULES: Final[tuple[tuple[re.Pattern[str], str], ...]] = (
    (re.compile(r"AKIA[0-9A-Z]{16}"), _PLACEHOLDER),
    (re.compile(r"ASIA[0-9A-Z]{16}"), _PLACEHOLDER),
    (
        re.compile(r"-----BEGIN [\s\S]*?(?:-----END [^\n-]*-----|\Z)"),
        _PLACEHOLDER,
    ),
    (re.compile(r"://[^/\s:@]+:[^/\s@]+@"), _PLACEHOLDER),
    (re.compile(r"k8s-aws-v1\.\S*"), _PLACEHOLDER),
    (re.compile(r"aws_secret_access_key(?:\s*[=:]\s*\S+)?"), _PLACEHOLDER),
    (re.compile(r"eyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}"), _PLACEHOLDER),
    (
        re.compile(
            r"""
            (
                name:\s*\S*(?:password|token|secret|passwd|credential)\S*
                \s+value:\s*
            )
            \S+
            """,
            re.IGNORECASE | re.VERBOSE,
        ),
        rf"\1{_PLACEHOLDER}",
    ),
)


def redact_text(text: str) -> str:
    """Return text with credential-shaped substrings replaced by [redacted]."""
    redacted = text
    for pattern, replacement in _RULES:
        redacted = pattern.sub(replacement, redacted)
    return redacted
