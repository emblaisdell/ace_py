"""
python -m ace_py <user_module>

Imports <user_module>, which registers its @calculation functions,
then reads one ACE request from stdin and writes results to stdout.
"""

import importlib
import sys


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m ace_py <module>", file=sys.stderr)
        sys.exit(1)

    module_name = sys.argv[1]
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        print(f"ace_py: could not import module {module_name!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    from ace_py import run
    run()


if __name__ == "__main__":
    main()
