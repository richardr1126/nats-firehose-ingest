"""Main entry point for NATS Firehose Ingest service."""

import sys

# Import the main service
from src.service import main as service_main


def main():
    """Entry point that runs the synchronous service."""
    try:
        service_main()
    except KeyboardInterrupt:
        print("\nService interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"Service failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()