#!/usr/bin/env python3
"""
Bitbucket workspace language scraper.

Fetches repositories from a Bitbucket workspace and reports repository language usage.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import List

import requests
from bitbucket_client import (
    BitbucketApiError,
    RepoLanguageInfo,
    build_session,
    count_languages,
    fetch_repositories,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape languages used by repositories in a Bitbucket workspace."
    )
    parser.add_argument(
        "--workspace",
        required=True,
        help="Bitbucket workspace slug, e.g. 'my-team'.",
    )
    parser.add_argument(
        "--username",
        default=os.getenv("BITBUCKET_USERNAME"),
        help="Bitbucket username (or set BITBUCKET_USERNAME).",
    )
    parser.add_argument(
        "--app-password",
        default=os.getenv("BITBUCKET_APP_PASSWORD"),
        help="Bitbucket app password (or set BITBUCKET_APP_PASSWORD).",
    )
    parser.add_argument(
        "--pagelen",
        type=int,
        default=100,
        help="Repositories per API page (10-100). Default: 100.",
    )
    parser.add_argument(
        "--max-repos",
        type=int,
        default=0,
        help="Stop after N repositories (0 means no limit).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds. Default: 20.",
    )
    parser.add_argument(
        "--include-unknown",
        action="store_true",
        help="Include repositories with missing language in output lists.",
    )
    parser.add_argument(
        "--output-json",
        help="Optional path to write repository language data as JSON.",
    )
    parser.add_argument(
        "--output-csv",
        help="Optional path to write repository language data as CSV.",
    )
    return parser.parse_args()


def print_summary(repos: List[RepoLanguageInfo], include_unknown: bool) -> None:
    print(f"Total repositories scanned: {len(repos)}")
    language_counts = count_languages(repos, include_unknown=include_unknown)
    if not language_counts:
        print("No language metadata found.")
        return

    print("\nLanguage usage:")
    width = max(len(lang) for lang in language_counts.keys())
    for language, count in language_counts.most_common():
        print(f"  {language.ljust(width)}  {count}")

    print("\nRepositories:")
    filtered = [
        repo for repo in repos if include_unknown or repo.language
    ]
    for repo in filtered:
        lang = repo.language or "Unknown"
        visibility = "private" if repo.is_private else "public"
        print(f"  - {repo.full_name} [{lang}] ({visibility})")


def export_json(path: str, repos: List[RepoLanguageInfo]) -> None:
    payload = [repo.to_dict() for repo in repos]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def export_csv(path: str, repos: List[RepoLanguageInfo]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["name", "full_name", "language", "is_private", "html_url"]
        )
        writer.writeheader()
        for repo in repos:
            writer.writerow(repo.to_dict())


def main() -> int:
    args = parse_args()
    session = build_session(args.username, args.app_password)

    try:
        repos = fetch_repositories(
            session=session,
            workspace=args.workspace,
            pagelen=args.pagelen,
            timeout=args.timeout,
            max_repos=args.max_repos,
        )
    except requests.RequestException as exc:
        print(f"Request failed: {exc}", file=sys.stderr)
        return 1
    except BitbucketApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.output_json:
        export_json(args.output_json, repos)
        print(f"Wrote JSON: {args.output_json}")
    if args.output_csv:
        export_csv(args.output_csv, repos)
        print(f"Wrote CSV: {args.output_csv}")

    print_summary(repos, include_unknown=args.include_unknown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
