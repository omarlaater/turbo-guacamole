#!/usr/bin/env python3
"""Compatibility launcher for the packaged CLI."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC_DIR = ROOT / "src"
PKG_DIR = SRC_DIR / "bitbucket_language_scraper"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# This file shares the package name. Expose package path when imported so
# `import bitbucket_language_scraper.cli` resolves to src/ package modules.
if __name__ != "__main__":
    __path__ = [str(PKG_DIR)]

from bitbucket_language_scraper.cli import entrypoint


if __name__ == "__main__":
    entrypoint()
