# pipelines-mcp

Read-only MCP server for pipeline jobs, pods, configs, and logs.

It talks to the `dev` EKS cluster (`pipelines-prd` namespace) and the log store. Region `us-east-2`. SSO profile `reveliolabs` is only for EKS.

## Run

Needs Python 3.13 and [uv](https://docs.astral.sh/uv/). Cluster tools need AWS SSO for the `reveliolabs` profile.

```bash
uv sync
uv run pipelines-mcp
```

The process starts without talking to AWS. Cluster tools log you in if needed. A helper on `127.0.0.1:18201` gets the login; retry. If none, they return a URL. Over SSH: `RemoteForward 18201 127.0.0.1:18201`.

## Use in OpenCode

Add this to `~/.config/opencode/opencode.json`, or to an `opencode.json` in a project. Use the real absolute path to this repo.

```json
{
  "mcp": {
    "pipelines": {
      "type": "local",
      "command": [
        "uv",
        "run",
        "--directory",
        "/absolute/path/to/pipelines-mcp",
        "pipelines-mcp"
      ],
      "enabled": true
    }
  }
}
```

`uv` must be on PATH. Restart OpenCode after saving.

If the user gives a GitHub or Jenkins run, read that CI log first for the request_id UUID (not the run id), then read pipeline logs. If a cluster tool says AWS login started, retry the same request. Do not ask the user to open a URL unless the tool returned one.

## Dev

```bash
uv sync --group dev
uv run pytest
uv run ruff check src tests
uv run basedpyright
```
