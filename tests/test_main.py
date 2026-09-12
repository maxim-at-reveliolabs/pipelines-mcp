import subprocess
import sys
from pathlib import Path


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
