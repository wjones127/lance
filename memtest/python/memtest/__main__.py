"""CLI for lance-memtest."""

import sys
from memtest import get_library_path


def main():
    """Main CLI entry point - print path to shared library."""
    lib_path = get_library_path()
    print(lib_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
