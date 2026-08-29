"""Replace credential-shaped substrings in tool output."""

import re
from typing import Final

_PATTERNS: Final = (
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"ASIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN [\s\S]*?(?:-----END [^\n-]*-----|\Z)"),
    re.compile(r"://[^/\s:@]+:[^/\s@]+@"),
    re.compile(r"k8s-aws-v1\.\S*"),
    re.compile(r"aws_secret_access_key(?:\s*[=:]\s*\S+)?"),
    re.compile(r"eyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2}"),
)


def redact_text(text: str) -> str:
    """Return text with credential-shaped substrings replaced by [redacted]."""
    redacted = text
    for pattern in _PATTERNS:
        redacted = pattern.sub("[redacted]", redacted)
    return redacted
