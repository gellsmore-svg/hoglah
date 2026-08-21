"""Allow ``python -m hoglah`` (portable zip and embeddable CPython)."""

from hoglah.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
