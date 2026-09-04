# pipelines-mcp

Read-only MCP server for pipeline jobs, pods, configs, and logs.

It talks to the `dev` EKS cluster (`pipelines-prd` namespace) and the log store. Region `us-east-2`. SSO profile `reveliolabs` is only for EKS.

## Tools

| Tool | What it does |
|------|----------------|
| `list_pipeline_jobs` | Browse running or recent jobs. Skip if the user gave a request_id, GitHub URL, or Jenkins run. |
| `get_pipeline_job` | One job by request id, step index, replica. |
| `list_pipeline_pods` | Pods for that job. Empty if they are already gone. |
| `get_pipeline_job_config` | Full job YAML. Secrets stripped. |
| `get_pipeline_pod` | One pod by name. |
| `get_pipeline_pod_config` | Full pod YAML. Secrets stripped. |
| `get_pipeline_log` | Worker logs for a request. Last 100 lines plus a cursor. |
| `get_pipeline_service_log` | Service logs. Same cursor rules. |
| `get_timescaling_log` | Timescaling logs when the worker says the cluster failed. Pass client, batchtime, comptype from the job config. |

Job names look like `pipelines-{request_id}-{step_index}-{replica}`.

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
