"""Console entry for pipelines-mcp."""

from pipelines_mcp.server import mcp
from pipelines_mcp.server_app import install_live


def main() -> None:
    """Start the MCP server. Cluster tools log in to AWS if needed."""
    install_live()
    mcp.run()


if __name__ == "__main__":
    main()
