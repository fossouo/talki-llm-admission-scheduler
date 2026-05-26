"""Gestion des git worktrees pour les agents Backend/Frontend/QA/Doc.

Chaque agent travaille dans son propre worktree isolé du POC repo, avec
identité git locale (rôle) et pre-commit hook installé. Cela permet à
plusieurs agents de tourner en parallèle sans piétiner la working tree
principale du dev humain.

Convention :
  ~/Documents/GitHub/kola-money-poc-agents/<role>/<issue_id>-<slug>/
  branche : agents/<role>/<issue_id>-<slug>
"""
from __future__ import annotations

import asyncio
import logging
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import POC_ROOT, Role, TEAM_ROOT

log = logging.getLogger(__name__)

WORKTREES_ROOT = POC_ROOT.parent / "kola-money-poc-agents"


def _slug(text: str, max_len: int = 40) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return s[:max_len] or "task"


@dataclass
class Worktree:
    path: Path
    branch: str
    role: Role


class WorktreeError(Exception):
    pass


async def _run(args: list[str], cwd: Path | None = None) -> tuple[int, str, str]:
    p = await asyncio.create_subprocess_exec(
        *args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(cwd) if cwd else None,
    )
    out, err = await p.communicate()
    return p.returncode or 0, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


async def _git(args: list[str], cwd: Path) -> tuple[int, str, str]:
    return await _run(["git", *args], cwd=cwd)


async def ensure_base_uptodate() -> None:
    """`git fetch origin` sur POC_ROOT pour que les worktrees démarrent de main à jour."""
    if not POC_ROOT.exists():
        raise WorktreeError(f"POC repo introuvable : {POC_ROOT}")
    code, _, err = await _git(["fetch", "origin", "main", "--quiet"], cwd=POC_ROOT)
    if code != 0:
        raise WorktreeError(f"git fetch origin main a échoué : {err.strip()}")


async def create_worktree(role: Role, issue_id: int, title: str) -> Worktree:
    """Crée un worktree pour un agent. Idempotent : si le path existe déjà
    avec la bonne branche, on le réutilise. Sinon erreur (ne pas écraser).
    """
    branch = f"agents/{role.name}/{issue_id}-{_slug(title)}"
    wt_dir = WORKTREES_ROOT / role.name / f"{issue_id}-{_slug(title)}"
    WORKTREES_ROOT.mkdir(parents=True, exist_ok=True)

    if wt_dir.exists():
        # vérifie qu'il est bien lié au POC repo et sur la bonne branche
        code, out, _ = await _git(["rev-parse", "--abbrev-ref", "HEAD"], cwd=wt_dir)
        if code == 0 and out.strip() == branch:
            log.info("worktree existant réutilisé : %s", wt_dir)
            await _configure_identity(wt_dir, role)
            await _install_hook(wt_dir)
            return Worktree(path=wt_dir, branch=branch, role=role)
        raise WorktreeError(f"worktree path existe avec mauvaise branche : {wt_dir}")

    code, _, err = await _git(
        ["worktree", "add", "-b", branch, str(wt_dir), "origin/main"],
        cwd=POC_ROOT,
    )
    if code != 0:
        raise WorktreeError(f"git worktree add a échoué : {err.strip()}")

    await _configure_identity(wt_dir, role)
    await _install_hook(wt_dir)
    log.info("worktree créé : %s (branch=%s)", wt_dir, branch)
    return Worktree(path=wt_dir, branch=branch, role=role)


async def _configure_identity(wt_dir: Path, role: Role) -> None:
    for k, v in [("user.name", role.git_name), ("user.email", role.git_email)]:
        code, _, err = await _git(["config", "--local", k, v], cwd=wt_dir)
        if code != 0:
            raise WorktreeError(f"git config {k} a échoué : {err.strip()}")


async def _install_hook(wt_dir: Path) -> None:
    script = TEAM_ROOT / "scripts" / "install-precommit-hook.sh"
    if not script.exists():
        raise WorktreeError(f"hook installer introuvable : {script}")
    code, _, err = await _run(["bash", str(script), str(wt_dir)])
    if code != 0:
        raise WorktreeError(f"install-precommit-hook a échoué : {err.strip()}")


async def remove_worktree(wt: Worktree, *, force: bool = False) -> None:
    """Supprime le worktree. À appeler après push réussi + PR ouverte.
    Si force=True, supprime même si dirty (utiliser avec précaution)."""
    args = ["worktree", "remove", str(wt.path)]
    if force:
        args.append("--force")
    code, _, err = await _git(args, cwd=POC_ROOT)
    if code != 0:
        # fallback : supprimer le dossier manuellement et `git worktree prune`
        log.warning("git worktree remove a échoué (%s), cleanup manuel", err.strip())
        if wt.path.exists():
            shutil.rmtree(wt.path, ignore_errors=True)
        await _git(["worktree", "prune"], cwd=POC_ROOT)
