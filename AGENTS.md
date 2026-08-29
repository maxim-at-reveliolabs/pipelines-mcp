# pipelines-mcp

Read-only MCP for pipeline jobs, pods, configs, and logs. Python 3.13, pydantic v2, basedpyright `all`, ruff `ALL`.

## Layout

`src/pipelines_mcp/` is the whole product. `tests/` follows the modules.

| Task | File |
|------|------|
| New tool | `server.py` (decorator) + `server_ops.py` (call) |
| Job/pod I/O | `k8s.py` |
| Logs | `logs.py` |
| SSO device login | `settings_sso.py` |
| EKS token | `eks_token.py` |
| ES auth | `settings.py` |
| Process wiring | `__main__.py`, `server_app.py` |
| DTOs | `models.py` |
| Output scrub | `redact.py` |

`server.py` defines tools and must not call `run()`. `main()` installs a lazy App builder, then `mcp.run()`.

## Live targets (baked in)

- AWS profile `reveliolabs`, region `us-east-2`
- EKS cluster `dev`, namespace `pipelines-prd`
- ES secret `elasticsearch/elastic`
- Job name `pipelines-{request_id}-{step_index}-{replica}`

## Conventions

- Frozen pydantic DTOs. Protocols for k8s clients. Inject fakes at the boundary.
- No live clients at import. First tool call: `request_sso()`, then build `App`.
- Domain errors go through `tool_boundary` to a redacted `ToolError`. The SSO login URL is for the human — do not redact it.
- Read-only. Do not add mutate/delete cluster tools.

## Commands

```bash
uv sync --group dev
uv run pytest
uv run ruff check src tests
uv run basedpyright
```
