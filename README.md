# pipelines-mcp

Read-only MCP server for pipeline jobs, pods, configs, and logs.

It talks to the `dev` EKS cluster (`pipelines-prd` namespace) and the log store. AWS profile is `reveliolabs` in `us-east-2`.

## Tools

| Tool | What it does |
|------|----------------|
| `list_pipeline_jobs` | List jobs, newest first. Filter by status, request id, limit. |
| `get_pipeline_job` | One job by request id, step index, replica. |
| `list_pipeline_pods` | Pods for that job. Empty if they are already gone. |
| `get_pipeline_job_config` | Full job YAML. Secrets stripped. |
| `get_pipeline_pod` | One pod by name. |
| `get_pipeline_pod_config` | Full pod YAML. Secrets stripped. |
| `get_pipeline_log` | Worker logs for a request. Last 100 lines plus a cursor. |
| `get_pipeline_service_log` | Service logs. Same cursor rules. |

Job names look like `pipelines-{request_id}-{step_index}-{replica}`.

## Run

Needs Python 3.13, [uv](https://docs.astral.sh/uv/), and AWS SSO for the `reveliolabs` profile.

```bash
uv sync
uv run pipelines-mcp
```

The process starts without talking to AWS. The first tool call logs you in if needed: it returns a URL. Open that URL, finish login, then retry the same request.

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

Then ask OpenCode to list jobs or read logs. If it returns an AWS login URL, open it in a browser, finish login, and ask again.

## Dev

```bash
uv sync --group dev
uv run pytest
uv run ruff check src tests
uv run basedpyright
```
