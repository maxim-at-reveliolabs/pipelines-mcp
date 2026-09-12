"""Console entry for pipelines-mcp."""

from pipelines_mcp.server import mcp


def main() -> None:
    """Start the MCP server. Cluster tools log in to AWS if needed."""
    mcp.run()


if __name__ == "__main__":
    main()
