"""Configuration centrale du daemon orchestrateur.

Charte §7 — budgets tokens/jour par rôle. Charte alias LiteLLM, timeouts
conservateurs, paths repos. Tout en un endroit pour éviter la dérive.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


# Racine du repo kola-money-team (parent du dossier orchestrator/).
TEAM_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    """Charge .env du repo (sans écraser env vars déjà définies)."""
    envfile = TEAM_ROOT / ".env"
    if not envfile.exists():
        return
    for line in envfile.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip("'\"")
        os.environ.setdefault(k, v)


_load_dotenv()

# Chemin du repo kola-money-poc (cloné en sibling).
POC_ROOT = (TEAM_ROOT.parent / "kola-money-poc").resolve()

# LiteLLM canonique
LITELLM_BASE = os.environ.get("LITELLM_BASE", "http://inference-gateway.local:4000")
LITELLM_KEY = os.environ.get("LITELLM_MASTER_KEY", "")

# Redis pour budget tokens (réutilise l'instance kola-money POC :6379)
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# GitHub
GH_REPO = os.environ.get("KOLA_TEAM_GH_REPO", "fossouo/kola-money-poc")
GH_APP_ID = os.environ.get("KOLA_TEAM_APP_ID", "")
GH_APP_INSTALLATION_ID = os.environ.get("KOLA_TEAM_APP_INSTALLATION_ID", "")
GH_APP_KEY_PATH = os.environ.get("KOLA_TEAM_APP_PRIVATE_KEY_PATH", "secrets/kola-team-bot.pem")

# Heartbeat
HEARTBEAT_SECONDS = int(os.environ.get("KOLA_HEARTBEAT_SECONDS", "300"))  # 5 min


@dataclass(frozen=True)
class Role:
    """Profil d'un rôle d'agent. Aliments la charte §7."""

    name: str                       # ex: "ceo", "backend"
    git_name: str                   # ex: "kola-ceo-agent"
    git_email: str                  # ex: "ceo@kola-money.local"
    primary_alias: str              # alias LiteLLM principal
    fallback_alias: str | None      # fallback côté daemon (5xx only)
    daily_token_cap: int            # charte §7
    llm_timeout_s: float            # timeout par appel LLM (conservateur)
    fallback_timeout_s: float       # timeout du fallback s'il s'active


# Charte §7 — Source unique de vérité.
ROLES: dict[str, Role] = {
    "ceo": Role(
        name="ceo",
        git_name="kola-ceo-agent",
        git_email="ceo@kola-money.local",
        primary_alias="code",
        fallback_alias="brain-exec",
        daily_token_cap=300_000,
        llm_timeout_s=600.0,        # heavy + queue éventuelle
        fallback_timeout_s=300.0,
    ),
    "backend": Role(
        name="backend",
        git_name="kola-backend-agent",
        git_email="backend@kola-money.local",
        primary_alias="code",
        fallback_alias="brain-exec",
        daily_token_cap=500_000,
        llm_timeout_s=600.0,
        fallback_timeout_s=300.0,
    ),
    "frontend": Role(
        name="frontend",
        git_name="kola-frontend-agent",
        git_email="frontend@kola-money.local",
        primary_alias="code",
        fallback_alias="brain-exec",
        daily_token_cap=400_000,
        llm_timeout_s=600.0,
        fallback_timeout_s=300.0,
    ),
    "qa": Role(
        name="qa",
        git_name="kola-qa-agent",
        git_email="qa@kola-money.local",
        primary_alias="brain-exec",
        fallback_alias=None,
        daily_token_cap=300_000,
        llm_timeout_s=300.0,
        fallback_timeout_s=0.0,
    ),
    "doc": Role(
        name="doc",
        git_name="kola-doc-agent",
        git_email="doc@kola-money.local",
        primary_alias="reasoning-model",
        fallback_alias=None,
        daily_token_cap=150_000,
        llm_timeout_s=300.0,
        fallback_timeout_s=0.0,
    ),
    "triage": Role(
        name="triage",
        git_name="kola-triage-agent",
        git_email="triage@kola-money.local",
        primary_alias="tool-worker",
        fallback_alias=None,
        daily_token_cap=100_000,
        llm_timeout_s=120.0,
        fallback_timeout_s=0.0,
    ),
}


def role(name: str) -> Role:
    """Lookup avec erreur claire si le nom est faux."""
    if name not in ROLES:
        raise KeyError(f"rôle inconnu : {name!r}. Connus : {sorted(ROLES)}")
    return ROLES[name]
