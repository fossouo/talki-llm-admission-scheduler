"""Client GitHub authentifié via App `kola-team-bot`.

Génère + cache l'installation token (1h). Fournit des opérations minimales :
list_open_prs, get_pr, comment_pr, add_labels, merge_pr, close_pr,
list_open_issues, comment_issue, set_issue_labels.

Toutes les écritures GitHub passent par cette classe pour avoir un point
unique d'audit.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path
from typing import Any

import httpx
import jwt

from .config import (
    GH_APP_ID,
    GH_APP_INSTALLATION_ID,
    GH_APP_KEY_PATH,
    GH_REPO,
    TEAM_ROOT,
)

log = logging.getLogger(__name__)

GH_API = "https://api.github.com"
TOKEN_RENEW_MARGIN_S = 600  # renouveler quand il reste < 10 min


class GitHubAppError(Exception):
    pass


class GitHubApp:
    def __init__(self) -> None:
        self._token: str | None = None
        self._expires: dt.datetime | None = None
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        async with self._lock:
            if self._token and self._expires:
                remaining = (self._expires - dt.datetime.now(dt.timezone.utc)).total_seconds()
                if remaining > TOKEN_RENEW_MARGIN_S:
                    return self._token
            await self._refresh()
            assert self._token
            return self._token

    async def _refresh(self) -> None:
        if not GH_APP_ID or not GH_APP_INSTALLATION_ID:
            raise GitHubAppError(
                "KOLA_TEAM_APP_ID ou KOLA_TEAM_APP_INSTALLATION_ID manquant dans .env"
            )
        key_path = Path(GH_APP_KEY_PATH)
        if not key_path.is_absolute():
            key_path = (TEAM_ROOT / key_path).resolve()
        if not key_path.exists():
            raise GitHubAppError(f"clé privée GitHub App introuvable : {key_path}")
        pem = key_path.read_text()

        now = int(time.time())
        app_jwt = jwt.encode(
            {"iat": now - 60, "exp": now + 540, "iss": GH_APP_ID},
            pem,
            algorithm="RS256",
        )
        async with httpx.AsyncClient(timeout=15.0) as cli:
            r = await cli.post(
                f"{GH_API}/app/installations/{GH_APP_INSTALLATION_ID}/access_tokens",
                headers={
                    "Authorization": f"Bearer {app_jwt}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
        if r.status_code != 201:
            raise GitHubAppError(f"echec génération installation token : {r.status_code} {r.text[:300]}")
        d = r.json()
        self._token = d["token"]
        self._expires = dt.datetime.fromisoformat(d["expires_at"].replace("Z", "+00:00"))
        log.info("github-app token refreshed, expires %s", self._expires.isoformat())

    async def _req(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        token = await self.token()
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.request(
                method,
                f"{GH_API}{path}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                json=json_body,
                params=params,
            )
        if r.status_code >= 400:
            raise GitHubAppError(f"{method} {path} → {r.status_code} : {r.text[:400]}")
        if r.status_code == 204 or not r.content:
            return None
        return r.json()

    # ---- PRs ----

    async def list_open_prs(self) -> list[dict[str, Any]]:
        return await self._req("GET", f"/repos/{GH_REPO}/pulls", params={"state": "open", "per_page": 50})

    async def get_pr(self, number: int) -> dict[str, Any]:
        return await self._req("GET", f"/repos/{GH_REPO}/pulls/{number}")

    async def get_pr_files(self, number: int) -> list[dict[str, Any]]:
        return await self._req("GET", f"/repos/{GH_REPO}/pulls/{number}/files", params={"per_page": 100})

    async def get_pr_status(self, number: int) -> dict[str, Any]:
        """Retourne un payload {"check_runs": [...]} compatible avec
        l'ancien format Check API. Si l'App n'a pas `checks:read`, on
        retombe sur l'API `actions/runs?head_sha=` qui ne nécessite que
        `metadata:read`, et on convertit chaque workflow run en check_run."""
        pr = await self.get_pr(number)
        head_sha = pr["head"]["sha"]
        try:
            return await self._req("GET", f"/repos/{GH_REPO}/commits/{head_sha}/check-runs")
        except GitHubAppError as e:
            if "403" not in str(e):
                raise
            # fallback : workflow runs
            runs = await self._req(
                "GET", f"/repos/{GH_REPO}/actions/runs",
                params={"head_sha": head_sha, "per_page": 50},
            )
            return {
                "total_count": runs.get("total_count", 0),
                "check_runs": [
                    {
                        "name": r["name"],
                        "status": r["status"],            # queued/in_progress/completed
                        "conclusion": r.get("conclusion"), # success/failure/...
                    }
                    for r in runs.get("workflow_runs", [])
                ],
            }

    async def comment_pr(self, number: int, body: str) -> dict[str, Any]:
        return await self._req(
            "POST", f"/repos/{GH_REPO}/issues/{number}/comments", json_body={"body": body}
        )

    async def add_labels(self, number: int, labels: list[str]) -> Any:
        return await self._req(
            "POST", f"/repos/{GH_REPO}/issues/{number}/labels", json_body={"labels": labels}
        )

    async def approve_pr(self, number: int, body: str = "Auto-approved by CEO Agent.") -> dict[str, Any]:
        return await self._req(
            "POST",
            f"/repos/{GH_REPO}/pulls/{number}/reviews",
            json_body={"event": "APPROVE", "body": body},
        )

    async def merge_pr(self, number: int, commit_title: str, commit_message: str) -> dict[str, Any]:
        return await self._req(
            "PUT",
            f"/repos/{GH_REPO}/pulls/{number}/merge",
            json_body={
                "commit_title": commit_title,
                "commit_message": commit_message,
                "merge_method": "squash",
            },
        )

    async def close_pr(self, number: int) -> dict[str, Any]:
        return await self._req(
            "PATCH", f"/repos/{GH_REPO}/pulls/{number}", json_body={"state": "closed"}
        )

    # ---- Issues ----

    async def list_open_issues(self, labels: list[str] | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"state": "open", "per_page": 50}
        if labels:
            params["labels"] = ",".join(labels)
        # /issues retourne issues + PRs ; filtrer côté caller via "pull_request" absent.
        all_ = await self._req("GET", f"/repos/{GH_REPO}/issues", params=params)
        return [i for i in all_ if "pull_request" not in i]

    async def comment_issue(self, number: int, body: str) -> dict[str, Any]:
        return await self._req(
            "POST", f"/repos/{GH_REPO}/issues/{number}/comments", json_body={"body": body}
        )

    async def list_issue_comments(self, number: int) -> list[dict[str, Any]]:
        return await self._req(
            "GET", f"/repos/{GH_REPO}/issues/{number}/comments",
            params={"per_page": 100},
        )

    async def set_issue_labels(self, number: int, labels: list[str]) -> Any:
        return await self._req(
            "PUT", f"/repos/{GH_REPO}/issues/{number}/labels", json_body={"labels": labels}
        )

    async def remove_label(self, number: int, label: str) -> Any:
        token = await self.token()
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.request(
                "DELETE",
                f"{GH_API}/repos/{GH_REPO}/issues/{number}/labels/{label}",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise GitHubAppError(f"DELETE label {label} → {r.status_code} : {r.text[:200]}")
        return r.json() if r.content else None

    # ---- Repo / PR creation ----

    async def get_repo(self) -> dict[str, Any]:
        return await self._req("GET", f"/repos/{GH_REPO}")

    async def create_pull_request(
        self, *, title: str, body: str, head: str, base: str = "main", draft: bool = False
    ) -> dict[str, Any]:
        return await self._req(
            "POST",
            f"/repos/{GH_REPO}/pulls",
            json_body={"title": title, "body": body, "head": head, "base": base, "draft": draft},
        )

    async def get_file_contents(self, path: str, ref: str = "main") -> str | None:
        """Récupère le contenu décodé d'un fichier du repo. None si absent."""
        token = await self.token()
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.get(
                f"{GH_API}/repos/{GH_REPO}/contents/{path}",
                params={"ref": ref},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/vnd.github.raw",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise GitHubAppError(f"GET file {path}@{ref} → {r.status_code} : {r.text[:200]}")
        return r.text
