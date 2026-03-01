from __future__ import annotations

import collections
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import requests
from requests.auth import HTTPBasicAuth

BITBUCKET_API_BASE = "https://api.bitbucket.org/2.0"


class BitbucketApiError(RuntimeError):
    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class RepoLanguageInfo:
    name: str
    full_name: str
    language: Optional[str]
    is_private: bool
    html_url: str

    def to_dict(self) -> Dict[str, object]:
        return {
            "name": self.name,
            "full_name": self.full_name,
            "language": self.language,
            "is_private": self.is_private,
            "html_url": self.html_url,
        }


def build_session(username: Optional[str], app_password: Optional[str]) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    if username and app_password:
        session.auth = HTTPBasicAuth(username, app_password)
    return session


def _extract_repo(repo: Dict[str, object]) -> RepoLanguageInfo:
    links = repo.get("links", {}) if isinstance(repo.get("links"), dict) else {}
    html_link = links.get("html", {}) if isinstance(links.get("html"), dict) else {}
    html_url = html_link.get("href") if isinstance(html_link.get("href"), str) else ""
    language = repo.get("language")
    if not isinstance(language, str):
        language = None
    return RepoLanguageInfo(
        name=str(repo.get("name", "")),
        full_name=str(repo.get("full_name", "")),
        language=language,
        is_private=bool(repo.get("is_private", False)),
        html_url=html_url,
    )


def fetch_repositories(
    session: requests.Session,
    workspace: str,
    pagelen: int,
    timeout: int,
    max_repos: int = 0,
) -> List[RepoLanguageInfo]:
    url = f"{BITBUCKET_API_BASE}/repositories/{workspace}"
    params = {"pagelen": max(10, min(100, pagelen))}
    repos: List[RepoLanguageInfo] = []

    while url:
        response = session.get(url, params=params if "?" not in url else None, timeout=timeout)
        if response.status_code == 401:
            raise BitbucketApiError(
                "Authentication failed (401). Check credentials.",
                status_code=401,
            )
        if response.status_code == 403:
            raise BitbucketApiError(
                "Access forbidden (403). Your credentials may not have permission for this workspace.",
                status_code=403,
            )
        if response.status_code == 404:
            raise BitbucketApiError(
                f"Workspace '{workspace}' not found (404). Check the workspace slug.",
                status_code=404,
            )
        if response.status_code >= 400:
            raise BitbucketApiError(
                f"Bitbucket API error ({response.status_code}).",
                status_code=response.status_code,
            )

        payload = response.json()
        values = payload.get("values", [])
        if not isinstance(values, list):
            break

        for repo in values:
            if isinstance(repo, dict):
                repos.append(_extract_repo(repo))
                if max_repos > 0 and len(repos) >= max_repos:
                    return repos

        next_url = payload.get("next")
        url = next_url if isinstance(next_url, str) else ""
        params = None

    return repos


def count_languages(
    repos: Iterable[RepoLanguageInfo], include_unknown: bool = False
) -> collections.Counter:
    counter: collections.Counter = collections.Counter()
    for repo in repos:
        if repo.language:
            counter[repo.language] += 1
        elif include_unknown:
            counter["Unknown"] += 1
    return counter
