"""Container healthchecks. Standard library only, so each probe starts fast."""

import argparse
import sys
import time
import urllib.request
from collections.abc import Sequence
from pathlib import Path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m switch_pipeline.healthcheck")
    checks = parser.add_subparsers(dest="check", required=True)
    heartbeat = checks.add_parser("heartbeat", help="A worker loop touched its file recently.")
    heartbeat.add_argument("path", type=Path)
    heartbeat.add_argument("--max-age", type=float, required=True, help="Seconds.")
    http = checks.add_parser("http", help="An HTTP endpoint answers 200.")
    http.add_argument("url")
    http.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args(argv)

    if args.check == "heartbeat":
        try:
            age = time.time() - args.path.stat().st_mtime
        except FileNotFoundError:
            print("no heartbeat yet")
            return 1
        if age > args.max_age:
            print(f"heartbeat is {age:.0f}s old")
            return 1
        return 0
    try:
        with urllib.request.urlopen(args.url, timeout=args.timeout) as response:
            return 0 if response.status == 200 else 1
    except OSError as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
