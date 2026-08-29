import subprocess
import sys
from pathlib import Path

_MAIN = Path(__file__).resolve().parents[1] / "src" / "pipelines_mcp" / "__main__.py"


def test_main_does_not_import_module_constants() -> None:
    # Given: the console entry
    # When: its source is read
    text = _MAIN.read_text(encoding="utf-8")

    # Then: it does not import baked-in constants or build live clients itself
    assert "AWS_PROFILE" not in text
    assert "ES_CONTENT_HEADERS" not in text
    assert "b64encode" not in text
    assert "load_es_auth" not in text
    assert "eks_auth_from_settings" not in text
    assert "JobView" not in text
    assert "live_k8s" not in text
    assert "create_async_client" not in text
    assert "request_sso" not in text
    assert "install_live" in text


def test_process_starts_without_aws_login(tmp_path: Path) -> None:
    # Given: no tool call, stdin already closed
    # When: the module is started
    result = subprocess.run(
        [sys.executable, "-m", "pipelines_mcp"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    # Then: the server started cleanly and stdout is not JSON-RPC
    assert result.returncode == 0
    stdout = result.stdout
    assert stdout == "" or "{" not in stdout
