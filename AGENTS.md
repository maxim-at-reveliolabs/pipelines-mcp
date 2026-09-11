# pipelines-mcp

Read-only MCP for pipeline jobs, pods, configs, and logs. Python 3.13, pydantic v2, basedpyright `all`, ruff `ALL`.

## Layout

`src/pipelines_mcp/` is the whole product. `tests/` follows the modules.

| Task | File |
|------|------|
| New tool | `server.py` (decorator) + `server_ops.py` (call) |
| Job/pod I/O | `k8s.py` |
| Logs | `logs.py` |
| Start line keys | `pipeline_start.py` |
| Timescaling logs | `timescaling_logs.py`, `object_store.py` |
| SSO device login | `settings_sso.py`, `sso_url.py` |
| EKS token | `eks_token.py` |
| ES auth | `settings.py` |
| Process wiring | `__main__.py`, `server_app.py` |
| DTOs | `models.py` |
| Output scrub | `redact.py` |

`server.py` defines tools and must not call `run()`. `main()` installs a lazy App builder, then `mcp.run()`.

## Live targets (baked in)

- AWS profile `reveliolabs`, region `us-east-2`
- EKS cluster `dev`, namespace `pipelines-prd`
- Live cluster client uses local kube config.
- ES secret `elasticsearch/elastic`
- Job name `pipelines-{request_id}-{step_index}-{replica}`
- Timescaling logs: `{batchtime}/{client}/{comptype}/timescaling/logs/`, else `{batchtime}/logs/{client}_{batchtime}_{comptype}_replica_0/`, else without `_replica_0`

## Conventions

- Frozen pydantic DTOs. Protocols for k8s clients. Inject fakes at the boundary.
- No live clients at import. SSO (`reveliolabs` profile) is only for EKS. Other AWS calls use the default credential chain.
- Domain errors go through `tool_boundary` to a redacted `ToolError`. If the helper on `127.0.0.1:18201` got the login, the error has no URL. If it did not, the URL is for the human — do not redact it.
- Read-only. Do not add mutate/delete cluster tools.

## Commands

```bash
uv sync --group dev
uv run pytest
uv run ruff check src tests
uv run basedpyright
```
