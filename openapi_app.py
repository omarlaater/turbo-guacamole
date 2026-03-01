from __future__ import annotations

import os
from typing import Dict, List, Optional

import requests
from fastapi import FastAPI, Header, HTTPException, Path, Query
from pydantic import BaseModel, Field

from bitbucket_client import (
    BitbucketApiError,
    RepoLanguageInfo,
    build_session,
    count_languages,
    fetch_repositories,
)


class RepoLanguageModel(BaseModel):
    name: str
    full_name: str
    language: Optional[str] = None
    is_private: bool
    html_url: str


class LanguageScrapeResponse(BaseModel):
    workspace: str
    total_repositories_scanned: int = Field(description="Total repositories fetched from Bitbucket.")
    language_counts: Dict[str, int]
    repositories: List[RepoLanguageModel]


class HealthResponse(BaseModel):
    status: str


app = FastAPI(
    title="Bitbucket Language Scraper API",
    version="1.0.0",
    description=(
        "OpenAPI service that fetches repositories from a Bitbucket workspace and returns "
        "language usage statistics."
    ),
)


@app.get("/health", response_model=HealthResponse, tags=["System"])
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get(
    "/api/v1/workspaces/{workspace}/languages",
    response_model=LanguageScrapeResponse,
    tags=["Bitbucket"],
)
def scrape_workspace_languages(
    workspace: str = Path(description="Bitbucket workspace slug, for example: my-team"),
    include_unknown: bool = Query(
        default=False,
        description="Include repositories that have no language metadata.",
    ),
    pagelen: int = Query(
        default=100,
        ge=10,
        le=100,
        description="Repositories per Bitbucket API page.",
    ),
    max_repos: int = Query(
        default=0,
        ge=0,
        description="Maximum number of repositories to scan. 0 means no limit.",
    ),
    timeout: int = Query(
        default=20,
        ge=1,
        le=120,
        description="Bitbucket API request timeout in seconds.",
    ),
    x_bitbucket_username: Optional[str] = Header(
        default=None,
        alias="X-Bitbucket-Username",
        description=(
            "Bitbucket username for private repositories. "
            "If missing, BITBUCKET_USERNAME env var is used."
        ),
    ),
    x_bitbucket_app_password: Optional[str] = Header(
        default=None,
        alias="X-Bitbucket-App-Password",
        description=(
            "Bitbucket app password for private repositories. "
            "If missing, BITBUCKET_APP_PASSWORD env var is used."
        ),
    ),
) -> LanguageScrapeResponse:
    username = x_bitbucket_username or os.getenv("BITBUCKET_USERNAME")
    app_password = x_bitbucket_app_password or os.getenv("BITBUCKET_APP_PASSWORD")

    if (username and not app_password) or (app_password and not username):
        raise HTTPException(
            status_code=400,
            detail="Provide both username and app password, or neither.",
        )

    session = build_session(username=username, app_password=app_password)

    try:
        repos = fetch_repositories(
            session=session,
            workspace=workspace,
            pagelen=pagelen,
            timeout=timeout,
            max_repos=max_repos,
        )
    except BitbucketApiError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to call Bitbucket API: {exc}",
        ) from exc

    language_counts = dict(count_languages(repos, include_unknown=include_unknown).most_common())
    visible_repos = [repo for repo in repos if include_unknown or repo.language]
    repos_payload = [_to_repo_model(repo) for repo in visible_repos]

    return LanguageScrapeResponse(
        workspace=workspace,
        total_repositories_scanned=len(repos),
        language_counts=language_counts,
        repositories=repos_payload,
    )


def _to_repo_model(repo: RepoLanguageInfo) -> RepoLanguageModel:
    return RepoLanguageModel(
        name=repo.name,
        full_name=repo.full_name,
        language=repo.language,
        is_private=repo.is_private,
        html_url=repo.html_url,
    )
