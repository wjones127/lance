"""CLI for lance-memtest."""

import sys
from memtest import get_library_path, print_stats, is_supported


def main():
    """Main CLI entry point."""
    args = sys.argv[1:]

    if not args or args[0] == "path":
        lib_path = get_library_path()
        if lib_path is None:
            print(
                "lance-memtest is not supported on this platform",
                file=sys.stderr,
            )
            return 1
        print(lib_path)
        return 0
    elif args[0] == "stats":
        if not is_supported():
            print(
                "lance-memtest is not supported on this platform",
                file=sys.stderr,
            )
            return 1
        print_stats()
        return 0
    else:
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        print("Usage: lance-memtest [path|stats]", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
