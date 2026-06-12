"""Placeholder CLI for hoglah.

Installed as the `hoglah` console script via pyproject.toml.
Will be expanded with real commands (submit, status, list, cancel, models, etc.)
once the core library is implemented.
"""

import sys


def main() -> None:
    print("hoglah v0.1.0 — initial scaffold")
    print("Core implementation not yet present.")
    print("See README.md and docs/ for requirements and current status.")
    print()
    print("Planned usage (once ready):")
    print("  hoglah list --status queued,processing")
    print("  hoglah status <job-id>")
    print("  hoglah models")
    sys.exit(0)


if __name__ == "__main__":
    main()
