import pytest

from pipelines_mcp.redact import redact_text

_PLACEHOLDER = "[redacted]"
_AKIA = "AKIA" + "A" * 16
_ASIA = "ASIA" + "B" * 16
_PEM = (
    "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEA\n-----END RSA PRIVATE KEY-----"
)
_JWT = "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc"
_EKS = "k8s-aws-v1." + "abcDEF-_123"


@pytest.mark.parametrize(
    ("text", "hidden"),
    [
        (f"id={_AKIA}", (_AKIA,)),
        (f"id={_ASIA}", (_ASIA,)),
        (f"cert={_PEM}", ("BEGIN RSA PRIVATE KEY", "MIIEowIBAAKCAQEA")),
        ("https://" + "alice:s3cret@example.com/x", ("alice:s3cret",)),
        (f"token={_EKS}", (_EKS,)),
        (
            "aws_secret_access_key=" + "wJalrXUtnFEMI",
            ("aws_secret_access_key", "wJalrXUtnFEMI"),
        ),
        (f"auth={_JWT}", (_JWT,)),
    ],
)
def test_redacts_credential_shaped_text(text: str, hidden: tuple[str, ...]) -> None:
    result = redact_text(text)
    assert _PLACEHOLDER in result
    for part in hidden:
        assert part not in result


def test_leaves_ordinary_text_unchanged() -> None:
    assert redact_text("request-abc") == "request-abc"
    assert redact_text("pipeline step 0 completed") == "pipeline step 0 completed"
    assert redact_text("") == ""


def test_redacts_each_match_when_patterns_overlap_in_one_string() -> None:
    access_key = "AKIA" + "C" * 16
    jwt = "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.xyz"
    result = redact_text(f"{access_key} and {jwt}")
    assert access_key not in result
    assert jwt not in result
    assert result.count(_PLACEHOLDER) == 2
