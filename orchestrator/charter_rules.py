"""Règles déterministes de la charte §1/§3/§4 — fonctions pures, sans I/O.

Source unique de vérité côté Python pour ce qui matchera comme zone interdite
et ce qui passe les conditions auto-merge / auto-reject. Synchronisé à la main
avec :
  - kola-money-poc:.github/CODEOWNERS
  - kola-money-poc:.github/workflows/forbidden-zones-check.yml
  - kola-money-team:scripts/install-precommit-hook.sh
"""
from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass

# Charte §1 — patterns bash glob
FORBIDDEN_PATTERNS = [
    ".env", ".env.*", "*.env",
    "docker-compose*.yml", "Dockerfile*", ".dockerignore",
    "infra/docker/*",
    ".github/*",
    "apps/api/alembic.ini", "apps/api/alembic/versions/*",
    "alembic.ini", "alembic/versions/*",
    "apps/api/routers/auth*.py", "apps/api/routers/auth/*",
    "apps/api/services/aml_service*.py",
    "apps/api/services/kyc_service*.py",
    "apps/api/models.py", "apps/api/models/*",
    "apps/api/db.py", "apps/api/main.py", "apps/api/config*.py",
    "apps/web/next.config.*", "apps/web/tsconfig.json",
    "apps/web/package.json", "apps/web/middleware.ts",
    "apps/web/app/layout.tsx",
    "CODEOWNERS",
]

AGENT_USER_PATTERNS = re.compile(r"^kola-(ceo|backend|frontend|qa|doc|triage)-agent$|^kola-team-bot")

# Charte §3
REQUIRED_PR_SECTIONS = ("## What", "## Why", "## Test")
MAX_PR_DIFF_LINES = 400
DEP_FILENAMES = {"package.json", "requirements.txt", "pyproject.toml"}

# Charte §4
SECRET_REGEX = re.compile(r"(sk-[a-f0-9]{16,}|ghp_[a-zA-Z0-9]{20,}|AKIA[A-Z0-9]{16})")
BAD_PROD_KEYWORDS = re.compile(r"\b(prod|production|live|mainnet)\b", re.IGNORECASE)
CONFIG_EXTS = (".yaml", ".yml", ".toml", ".ini", ".env")


@dataclass
class CheckResult:
    ok: bool
    reason: str = ""


def matches_forbidden(path: str) -> str | None:
    """Retourne le pattern qui matche, ou None."""
    for pat in FORBIDDEN_PATTERNS:
        if fnmatch.fnmatch(path, pat):
            return pat
    return None


def is_agent_pr(pr: dict) -> bool:
    user = (pr.get("user") or {}).get("login", "")
    return bool(AGENT_USER_PATTERNS.match(user))


def check_charter_3(pr: dict, files: list[dict], status: dict) -> CheckResult:
    """Charte §3 — toutes conditions doivent être vraies pour auto-merge."""

    # §3.1 — auteur agent
    if not is_agent_pr(pr):
        return CheckResult(False, "auteur != agent kola-team")

    # §3.2 — CI verte
    check_runs = status.get("check_runs", [])
    if not check_runs:
        return CheckResult(False, "aucun status check exécuté")
    for cr in check_runs:
        if cr.get("conclusion") not in ("success", "skipped", "neutral"):
            return CheckResult(False, f"status check `{cr['name']}` = {cr.get('conclusion')}")

    # §3.3 — zones interdites
    for f in files:
        pat = matches_forbidden(f["filename"])
        if pat:
            return CheckResult(False, f"zone interdite §1 modifiée : {f['filename']} (match {pat})")

    # §3.5 — diff < cap
    total = sum((f.get("additions", 0) + f.get("deletions", 0)) for f in files)
    if total >= MAX_PR_DIFF_LINES:
        return CheckResult(False, f"diff {total} lignes ≥ cap {MAX_PR_DIFF_LINES}")

    # §3.6 — pas de nouvelle dépendance
    for f in files:
        base = f["filename"].split("/")[-1]
        if base in DEP_FILENAMES and f.get("status") in ("added", "modified"):
            return CheckResult(False, f"dépendance modifiée : {f['filename']} (revue humaine requise)")

    # §3.7 — pas de placeholder
    for f in files:
        patch = f.get("patch", "") or ""
        for line in patch.splitlines():
            if not line.startswith("+") or line.startswith("+++"):
                continue
            stripped = line[1:].strip()
            if stripped in ("pass", "..."):
                return CheckResult(False, f"placeholder `{stripped}` introduit dans {f['filename']}")
            if "TODO" in stripped or "FIXME" in stripped:
                return CheckResult(False, f"TODO/FIXME introduit dans {f['filename']}")

    # §3.8 — sections requises
    body = pr.get("body") or ""
    for section in REQUIRED_PR_SECTIONS:
        if section not in body:
            return CheckResult(False, f"section `{section}` manquante dans le body de la PR")

    return CheckResult(True, "")


def check_charter_4(pr: dict, files: list[dict]) -> CheckResult:
    """Charte §4 — auto-close si une de ces conditions est vraie."""

    # zones interdites
    for f in files:
        pat = matches_forbidden(f["filename"])
        if pat:
            return CheckResult(False, f"zone interdite §1 (close immédiate) : {f['filename']}")

    # secrets en clair
    for f in files:
        patch = f.get("patch", "") or ""
        for line in patch.splitlines():
            if line.startswith("+") and not line.startswith("+++") and SECRET_REGEX.search(line):
                return CheckResult(False, f"secret détecté dans patch de {f['filename']}")

    # chaînes prod-like dans configs
    for f in files:
        if not any(f["filename"].endswith(ext) for ext in CONFIG_EXTS):
            continue
        patch = f.get("patch", "") or ""
        for line in patch.splitlines():
            if line.startswith("+") and not line.startswith("+++") and BAD_PROD_KEYWORDS.search(line):
                return CheckResult(False, f"keyword prod-like dans config {f['filename']}")

    return CheckResult(True, "")
