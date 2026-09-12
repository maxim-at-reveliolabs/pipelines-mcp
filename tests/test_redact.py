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


@pytest.mark.parametrize(
    ("text", "hidden", "kept"),
    [
        (
            "- name: HARBOR_PASSWORD\n  value: harbor-secret-value\n",
            "harbor-secret-value",
            "HARBOR_PASSWORD",
        ),
        (
            "- name: M2M_TOKEN\n  value: m2m-secret-value\n",
            "m2m-secret-value",
            "M2M_TOKEN",
        ),
    ],
)
def test_redacts_secret_env_yaml_value(text: str, hidden: str, kept: str) -> None:
    result = redact_text(text)
    assert hidden not in result
    assert _PLACEHOLDER in result
    assert kept in result


def test_leaves_ordinary_text_unchanged() -> None:
    assert redact_text("request-abc") == "request-abc"
    assert redact_text("pipeline step 0 completed") == "pipeline step 0 completed"
    assert redact_text("") == ""
    assert redact_text("- name: CLIENT\n  value: acme\n") == (
        "- name: CLIENT\n  value: acme\n"
    )


def test_redacts_each_match_when_patterns_overlap_in_one_string() -> None:
    access_key = "AKIA" + "C" * 16
    jwt = "eyJ" + "hbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.xyz"
    result = redact_text(f"{access_key} and {jwt}")
    assert access_key not in result
    assert jwt not in result
    assert result.count(_PLACEHOLDER) == 2
