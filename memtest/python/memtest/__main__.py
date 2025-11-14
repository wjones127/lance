"""CLI for lance-memtest."""

import argparse
import os
import subprocess
import sys

from memtest import get_library_path, print_stats


def cmd_path(args):
    """Print the path to the memtest shared library."""
    lib_path = get_library_path()
    print(lib_path)
    return 0


def cmd_run(args):
    """Run a command with LD_PRELOAD set to track memory allocations."""
    lib_path = get_library_path()

    # Set up environment
    env = os.environ.copy()

    # Prepend to LD_PRELOAD if it already exists
    existing_preload = env.get("LD_PRELOAD", "")
    if existing_preload:
        env["LD_PRELOAD"] = f"{lib_path}:{existing_preload}"
    else:
        env["LD_PRELOAD"] = str(lib_path)

    # Run the command
    try:
        result = subprocess.run(args.command, env=env, shell=False)
        return result.returncode
    except FileNotFoundError:
        print(f"Error: Command not found: {args.command[0]}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def cmd_stats(args):
    """Print current allocation statistics."""
    print_stats()
    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="lance-memtest",
        description="Memory allocation testing utilities for Python",
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # path command
    path_parser = subparsers.add_parser(
        "path", help="Print path to the memtest shared library"
    )
    path_parser.set_defaults(func=cmd_path)

    # run command
    run_parser = subparsers.add_parser(
        "run", help="Run a command with memory tracking enabled"
    )
    run_parser.add_argument("command", nargs="+", help="Command and arguments to run")
    run_parser.set_defaults(func=cmd_run)

    # stats command
    stats_parser = subparsers.add_parser(
        "stats", help="Print current allocation statistics"
    )
    stats_parser.set_defaults(func=cmd_stats)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
