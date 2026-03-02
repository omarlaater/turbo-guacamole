#!/usr/bin/env python3
"""
Bitbucket Language Scraper
Supports Bitbucket Cloud (bitbucket.org) and Bitbucket Server/Data Center.
"""

from __future__ import annotations

import argparse
import csv
import fnmatch
import json
import os
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Bitbucket Cloud
WORKSPACE = os.getenv("BB_WORKSPACE", "your-workspace-slug")
BB_USERNAME = os.getenv("BB_USERNAME", "your-email@example.com")
BB_APP_PASSWORD = os.getenv("BB_APP_PASSWORD", "your-app-password")

# Bitbucket Server / Data Center
SERVER_URL = os.getenv("BB_SERVER_URL", "https://bitbucket.mycompany.com")
SERVER_TOKEN = os.getenv("BB_SERVER_TOKEN", "your-personal-access-token")
SERVER_USER = os.getenv("BB_SERVER_USER", "")
SERVER_PASS = os.getenv("BB_SERVER_PASS", "")
CA_BUNDLE = os.getenv("BB_CA_BUNDLE", "")
INSECURE = os.getenv("BB_INSECURE", "").strip().lower() in {"1", "true", "yes", "on"}

# Output
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "bitbucket_languages.csv")
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "bitbucket_languages.json")

# Concurrency for server scanning
DEFAULT_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))


def make_session(
    retries: int = 4,
    backoff_factor: float = 0.5,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    verify: Union[bool, str] = True,
) -> requests.Session:
    """Create a requests session with retry/backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset({"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.verify = verify
    return session


def normalize_server_url(base_url: str) -> str:
    """
    Normalize user input to a clean Bitbucket Server/Data Center base URL.
    Keeps context path (e.g. /bitbucket) and removes accidental UI/API suffixes.
    """
    raw = (base_url or "").strip()
    if not raw:
        raise ValueError("Server URL is empty.")

    if "://" not in raw:
        raw = f"https://{raw}"

    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid server URL: {base_url!r}")

    path_parts = [part for part in parsed.path.split("/") if part and part != "%20"]
    lower_parts = [part.lower() for part in path_parts]

    # If user passed .../rest/api/1.0, trim that suffix while preserving context path.
    for index in range(max(0, len(lower_parts) - 2)):
        if lower_parts[index : index + 3] == ["rest", "api", "1.0"]:
            path_parts = path_parts[:index]
            lower_parts = lower_parts[:index]
            break

    # If user passed web UI projects path, trim it.
    if lower_parts and lower_parts[-1] == "projects":
        path_parts = path_parts[:-1]

    normalized_path = ("/" + "/".join(path_parts)) if path_parts else ""
    return urlunparse((parsed.scheme, parsed.netloc, normalized_path.rstrip("/"), "", "", ""))


class BitbucketCloudScraper:
    BASE = "https://api.bitbucket.org/2.0"

    def __init__(
        self,
        workspace: str,
        username: str,
        app_password: str,
        verify: Union[bool, str] = True,
    ) -> None:
        self.workspace = workspace
        self.session = make_session(verify=verify)
        self.auth = (username, app_password)

    def _paginate(self, url: str, params: Optional[dict] = None):
        params = dict(params or {})
        while url:
            response = self.session.get(url, auth=self.auth, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            for value in data.get("values", []):
                yield value
            url = data.get("next")
            params = {}

    def get_projects(self) -> List[dict]:
        url = f"{self.BASE}/workspaces/{self.workspace}/projects"
        return list(self._paginate(url))

    def get_repos(self, project_key: str) -> List[dict]:
        url = f"{self.BASE}/repositories/{self.workspace}"
        return list(self._paginate(url, {"q": f'project.key="{project_key}"'}))

    def get_language(self, repo_slug: str) -> str:
        url = f"{self.BASE}/repositories/{self.workspace}/{repo_slug}"
        response = self.session.get(url, auth=self.auth, timeout=30)
        response.raise_for_status()
        language = response.json().get("language")
        return (language or "unknown") or "unknown"

    def scrape(self) -> List[dict]:
        results: List[dict] = []
        projects = self.get_projects()
        print(f"Found {len(projects)} projects in workspace '{self.workspace}'")

        for project in projects:
            project_key = project["key"]
            project_name = project.get("name", project_key)
            print(f"\n  Project: {project_name} ({project_key})")
            repos = self.get_repos(project_key)
            print(f"    {len(repos)} repositories")

            for repo in repos:
                slug = repo["slug"]
                name = repo.get("name", slug)
                try:
                    language = self.get_language(slug)
                except Exception as exc:
                    print(f"      ! error fetching language for {slug}: {exc}")
                    language = "unknown"

                clone = ""
                for link in repo.get("links", {}).get("clone", []):
                    if link.get("name") in ("https", "http"):
                        clone = link.get("href", "")
                        break

                print(f"      {name:45s}  {language}")
                results.append(
                    {
                        "project_key": project_key,
                        "project_name": project_name,
                        "repo_slug": slug,
                        "repo_name": name,
                        "primary_language": language,
                        "clone_url": clone,
                    }
                )
        return results


# Landmark files checked before extension counting
LANDMARK_FILES = {
    "pom.xml": "Java",
    "build.gradle": "Java/Kotlin",
    "build.gradle.kts": "Kotlin",
    "cargo.toml": "Rust",
    "go.mod": "Go",
    "package.json": "JavaScript/TypeScript",
    "requirements.txt": "Python",
    "pyproject.toml": "Python",
    "setup.py": "Python",
    "pipfile": "Python",
    "gemfile": "Ruby",
    "*.gemspec": "Ruby",
    "composer.json": "PHP",
    "*.csproj": "C#",
    "*.sln": "C#",
    "mix.exs": "Elixir",
    "build.sbt": "Scala",
    "project.clj": "Clojure",
    "pubspec.yaml": "Dart/Flutter",
    "stack.yaml": "Haskell",
    "cabal.project": "Haskell",
}

IGNORE_DIRS = {
    "node_modules",
    "vendor",
    "dist",
    "build",
    "target",
    ".terraform",
    "__pycache__",
    ".git",
    ".idea",
    ".vscode",
    "bin",
    "obj",
    "out",
    "coverage",
    ".next",
    ".nuxt",
    "venv",
    ".venv",
    "env",
    "site-packages",
}

# extension -> (language_label, weight)
EXT_MAP = {
    ".py": ("Python", 3),
    ".java": ("Java", 3),
    ".kt": ("Kotlin", 3),
    ".kts": ("Kotlin", 3),
    ".js": ("JavaScript", 3),
    ".jsx": ("JavaScript", 3),
    ".ts": ("TypeScript", 3),
    ".tsx": ("TypeScript", 3),
    ".cs": ("C#", 3),
    ".cpp": ("C++", 3),
    ".cc": ("C++", 3),
    ".cxx": ("C++", 3),
    ".c": ("C", 3),
    ".h": ("C/C++", 1),
    ".hpp": ("C++", 2),
    ".go": ("Go", 3),
    ".rs": ("Rust", 3),
    ".rb": ("Ruby", 3),
    ".php": ("PHP", 3),
    ".swift": ("Swift", 3),
    ".scala": ("Scala", 3),
    ".ex": ("Elixir", 3),
    ".exs": ("Elixir", 3),
    ".erl": ("Erlang", 3),
    ".hs": ("Haskell", 3),
    ".clj": ("Clojure", 3),
    ".r": ("R", 3),
    ".dart": ("Dart", 3),
    ".lua": ("Lua", 3),
    ".m": ("Objective-C", 3),
    ".groovy": ("Groovy", 3),
    # scripts / infra
    ".sh": ("Shell", 2),
    ".bash": ("Shell", 2),
    ".ps1": ("PowerShell", 2),
    ".tf": ("Terraform", 2),
    ".hcl": ("HCL", 2),
    ".proto": ("Protobuf", 2),
    ".sql": ("SQL", 2),
    # markup / config (low weight)
    ".yaml": ("YAML", 1),
    ".yml": ("YAML", 1),
    ".json": ("JSON", 1),
    ".xml": ("XML", 1),
    ".html": ("HTML", 1),
    ".css": ("CSS", 1),
    ".scss": ("SCSS", 1),
    ".md": ("Markdown", 0),
    ".txt": ("Text", 0),
    ".lock": ("Lockfile", 0),
}


class BitbucketServerScraper:
    def __init__(
        self,
        base_url: str,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: Union[bool, str] = True,
    ) -> None:
        normalized = normalize_server_url(base_url)
        self.base = normalized.rstrip("/") + "/rest/api/1.0"
        self.session = make_session(verify=verify)
        self.headers = {"Content-Type": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
            self.auth = None
        else:
            self.auth = (username, password)

    def _paginate(self, url: str, params: Optional[dict] = None):
        params = dict(params or {})
        params.setdefault("limit", 100)
        start = params.get("start", 0)
        while True:
            params["start"] = start
            response = self.session.get(
                url,
                headers=self.headers,
                auth=self.auth,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for value in data.get("values", []):
                yield value
            if data.get("isLastPage", True):
                break
            start = data.get("nextPageStart", start + params["limit"])

    def get_projects(self) -> List[dict]:
        return list(self._paginate(f"{self.base}/projects"))

    def get_repos(self, project_key: str) -> List[dict]:
        return list(self._paginate(f"{self.base}/projects/{project_key}/repos"))

    def _is_in_ignored_dir(self, path: str) -> bool:
        parts = [segment.lower() for segment in path.split("/") if segment]
        return any(segment in IGNORE_DIRS for segment in parts)

    def _detect_landmark(self, path_basename: str) -> Optional[str]:
        name = path_basename.lower()
        for pattern, label in LANDMARK_FILES.items():
            if fnmatch.fnmatch(name, pattern.lower()):
                return label
        return None

    def get_languages(self, project_key: str, repo_slug: str) -> Tuple[Dict[str, int], str]:
        """
        Returns (lang_score_map, primary_language).
        """
        url = f"{self.base}/projects/{project_key}/repos/{repo_slug}/files"
        try:
            files = list(self._paginate(url))
        except Exception as exc:
            print(f"      ! error listing files for {repo_slug}: {exc}")
            return {}, "unknown"

        lang_scores: Dict[str, int] = {}
        for file_path in files:
            if not file_path or self._is_in_ignored_dir(file_path):
                continue
            basename = os.path.basename(file_path)
            landmark = self._detect_landmark(basename)
            if landmark:
                lang_scores[landmark] = lang_scores.get(landmark, 0) + 50

        for file_path in files:
            if not file_path or self._is_in_ignored_dir(file_path):
                continue
            _, ext = os.path.splitext(file_path)
            if not ext:
                continue
            key = ext.lower()
            lang_entry = EXT_MAP.get(key)
            if lang_entry:
                lang_label, weight = lang_entry
            else:
                lang_label, weight = (key.lstrip("."), 1)
            lang_scores[lang_label] = lang_scores.get(lang_label, 0) + weight

        primary = max(lang_scores.items(), key=lambda item: item[1])[0] if lang_scores else "unknown"
        return lang_scores, primary

    def scrape_repo_worker(self, proj_tuple: Tuple[str, str, dict]) -> dict:
        proj_key, proj_name, repo = proj_tuple
        slug = repo["slug"]
        name = repo.get("name", slug)
        lang_counts, primary = self.get_languages(proj_key, slug)

        top5 = ", ".join(
            f"{lang}({count})"
            for lang, count in sorted(lang_counts.items(), key=lambda item: -item[1])[:5]
        ) or primary

        clone = ""
        for link in repo.get("links", {}).get("clone", []):
            if link.get("name") in ("http", "https"):
                clone = link.get("href", "")
                break

        print(f"      {name:45s}  {primary}  [{top5}]")
        return {
            "project_key": proj_key,
            "project_name": proj_name,
            "repo_slug": slug,
            "repo_name": name,
            "primary_language": primary,
            "all_languages": top5,
            "clone_url": clone,
        }

    def scrape(self, parallel: bool = True, max_workers: int = DEFAULT_MAX_WORKERS) -> List[dict]:
        results: List[dict] = []
        projects = self.get_projects()
        print(f"Found {len(projects)} projects on server")

        for project in projects:
            project_key = project["key"]
            project_name = project.get("name", project_key)
            print(f"\n  Project: {project_name} ({project_key})")
            repos = self.get_repos(project_key)
            print(f"    {len(repos)} repositories")

            if parallel and repos:
                max_workers = max(1, max_workers)
                tasks = [(project_key, project_name, repo) for repo in repos]
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(self.scrape_repo_worker, task): task for task in tasks
                    }
                    for future in as_completed(futures):
                        try:
                            results.append(future.result())
                        except Exception as exc:
                            task = futures[future]
                            print(f"      ! error scanning repo {task[2].get('slug')}: {exc}")
            else:
                for repo in repos:
                    results.append(self.scrape_repo_worker((project_key, project_name, repo)))
        return results


def save_csv(results: List[dict], path: str) -> None:
    if not results:
        print("No results to save.")
        return
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV saved  -> {path}")


def save_json(results: List[dict], path: str) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(results, file, indent=2, ensure_ascii=False)
    print(f"JSON saved -> {path}")


def print_summary(results: List[dict]) -> None:
    if not results:
        return
    lang_counter = Counter(result["primary_language"] for result in results)
    print("\n" + "=" * 50)
    print("LANGUAGE SUMMARY")
    print("=" * 50)
    for language, count in lang_counter.most_common():
        bar = "#" * min(count, 40)
        print(f"  {language:20s} {count:4d}  {bar}")
    print(f"\nTotal repos scanned: {len(results)}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bitbucket Language Scraper")
    parser.add_argument(
        "--mode",
        choices=["cloud", "server"],
        default="cloud",
        help="cloud = bitbucket.org | server = self-hosted",
    )
    parser.add_argument("--workspace", default=WORKSPACE)
    parser.add_argument("--bb-username", default=BB_USERNAME)
    parser.add_argument("--bb-app-password", default=BB_APP_PASSWORD)
    parser.add_argument("--server-url", default=SERVER_URL)
    parser.add_argument("--server-token", default=SERVER_TOKEN)
    parser.add_argument("--server-user", default=SERVER_USER)
    parser.add_argument("--server-pass", default=SERVER_PASS)
    parser.add_argument(
        "--ca-bundle",
        default=CA_BUNDLE,
        help="Path to corporate CA bundle PEM file for TLS verification.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        default=INSECURE,
        help="Disable TLS certificate verification (last resort).",
    )
    parser.add_argument("--out-csv", default=OUTPUT_CSV)
    parser.add_argument("--out-json", default=OUTPUT_JSON)
    parser.add_argument("--no-parallel", action="store_true", help="Disable server scan concurrency")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Max worker threads for server scan",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.max_workers < 1:
        parser.error("--max-workers must be >= 1")

    verify: Union[bool, str] = True
    if args.insecure:
        verify = False
        print("! TLS verification disabled (--insecure). Use only in trusted networks.")
    elif args.ca_bundle:
        verify = args.ca_bundle
        print(f"Using CA bundle: {args.ca_bundle}")

    if args.mode == "cloud":
        print("=== Bitbucket Cloud mode ===")
        scraper = BitbucketCloudScraper(
            workspace=args.workspace,
            username=args.bb_username,
            app_password=args.bb_app_password,
            verify=verify,
        )
        try:
            results = scraper.scrape()
        except requests.exceptions.SSLError as exc:
            print(f"TLS error: {exc}")
            print("Tip: provide --ca-bundle <corp-ca.pem> or set BB_CA_BUNDLE.")
            print("Temporary fallback: --insecure")
            return 2
        except requests.RequestException as exc:
            print(f"Request failed: {exc}")
            return 1
    else:
        print("=== Bitbucket Server / Data Center mode ===")
        try:
            scraper = BitbucketServerScraper(
                base_url=args.server_url,
                token=(args.server_token or None),
                username=(args.server_user or None),
                password=(args.server_pass or None),
                verify=verify,
            )
            print(f"Server API base: {scraper.base}")
            results = scraper.scrape(
                parallel=not args.no_parallel,
                max_workers=args.max_workers,
            )
        except ValueError as exc:
            print(f"Invalid server URL: {exc}")
            return 2
        except requests.exceptions.SSLError as exc:
            print(f"TLS error: {exc}")
            print("Tip: use --ca-bundle <corp-ca.pem> and keep --server-url without trailing /projects.")
            print("Temporary fallback: --insecure")
            return 2
        except requests.RequestException as exc:
            print(f"Request failed: {exc}")
            return 1

    save_csv(results, args.out_csv)
    save_json(results, args.out_json)
    print_summary(results)
    return 0


def entrypoint() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    entrypoint()
