"""Main entry point for NATS Firehose Ingest service."""

import asyncio
import sys

# Import the async service entrypoint
from src.service import service_main


def main():
    """Entry point that runs the async service."""
    try:
        asyncio.run(service_main())
    except Exception as e:
        print(f"Service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
