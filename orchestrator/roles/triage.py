"""Triage agent — label les nouvelles issues, marque ready-for-dev ou needs-info.

Charte ORG_CHART rôle Triage : aucun write sur les fichiers ; opère uniquement
sur les issues GitHub via labels et commentaires.
"""
from __future__ import annotations

import json
import logging
import re
import textwrap
from pathlib import Path

from ..audit import log_event
from ..budget import (
    BudgetExceeded,
    BudgetUnavailable,
    Reservation,
    check_and_reserve,
    commit,
    get_client,
)
from ..config import TEAM_ROOT, role
from ..github_app import GitHubApp
from ..llm import LLMError, LLMTimeout, call

log = logging.getLogger(__name__)


VALID_AREAS = {"area/api", "area/web", "area/infra", "area/docs", "area/team-ops"}
VALID_KINDS = {"kind/bug", "kind/feature", "kind/chore", "kind/task"}

PROMPT_PATH = TEAM_ROOT / "agents" / "triage.md"


def _load_prompt() -> str:
    if not PROMPT_PATH.exists():
        return (
            "Tu es l'agent Triage de l'équipe kola-team. Analyse l'issue et "
            "réponds en JSON strict avec les clés: area (parmi area/api, "
            "area/web, area/infra, area/docs, area/team-ops), kind (parmi "
            "kind/bug, kind/feature, kind/chore, kind/task), ready (true|false), "
            "missing_info (string court, vide si ready=true). Pas de markdown, "
            "pas d'explication, juste le JSON."
        )
    return PROMPT_PATH.read_text()


JSON_RE = re.compile(r"\{.*?\}", re.DOTALL)


def _extract_json(text: str) -> dict | None:
    m = JSON_RE.search(text)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None


class TriageAgent:
    def __init__(self, gh: GitHubApp) -> None:
        self.gh = gh
        self.role = role("triage")
        self._prompt = _load_prompt()

    async def heartbeat(self) -> None:
        try:
            issues = await self.gh.list_open_issues()
        except Exception as e:
            log.error("Triage: list_open_issues failed : %s", e)
            await log_event("triage", "heartbeat-fail", error=str(e))
            return

        for issue in issues:
            try:
                await self._triage_one(issue)
            except Exception as e:
                log.exception("Triage issue #%s a échoué", issue.get("number"))
                await log_event("triage", "fail", issue=issue.get("number"), error=str(e))

    async def _triage_one(self, issue: dict) -> None:
        number = issue["number"]
        labels = {lbl["name"] for lbl in issue.get("labels", [])}

        # déjà triagé ?
        has_area = any(l.startswith("area/") for l in labels)
        has_kind = any(l.startswith("kind/") for l in labels)
        if has_area and has_kind and ("ready-for-dev" in labels or "needs-info" in labels):
            return

        # appeler LLM
        red = await get_client()
        expected = 600  # heuristique : ~prompt + small JSON
        reservation: Reservation | None = None
        try:
            reservation = await check_and_reserve(red, self.role, expected)
        except BudgetExceeded as e:
            log.warning("Triage budget atteint : %s", e)
            await log_event("triage", "budget-exceeded", issue=number, detail=str(e))
            return
        except BudgetUnavailable as e:
            await log_event("triage", "budget-unavailable", issue=number, detail=str(e))

        body_preview = textwrap.shorten(issue.get("body") or "(empty body)", width=4000, placeholder="…")
        messages = [
            {"role": "system", "content": self._prompt},
            {
                "role": "user",
                "content": (
                    f"Issue #{number}: {issue['title']}\n\n"
                    f"{body_preview}\n\n"
                    f"Labels actuels : {sorted(labels)}\n\n"
                    "Réponds en JSON."
                ),
            },
        ]
        try:
            res = await call(self.role, messages, max_tokens=200, temperature=0.0)
        except LLMTimeout:
            log.warning("Triage timeout sur issue #%s — on retry au prochain heartbeat", number)
            if reservation is not None:
                await commit(red, reservation, expected)
            return
        except LLMError as e:
            log.error("Triage LLM erreur issue #%s : %s", number, e)
            if reservation is not None:
                await commit(red, reservation, expected)
            return
        if reservation is not None:
            await commit(red, reservation, res.total_tokens)

        data = _extract_json(res.content)
        if not data:
            await log_event("triage", "json-parse-fail", issue=number, raw=res.content[:200])
            return

        new_labels: list[str] = list(labels)
        area = data.get("area")
        kind = data.get("kind")
        if area in VALID_AREAS and area not in new_labels:
            new_labels.append(area)
        if kind in VALID_KINDS and kind not in new_labels:
            new_labels.append(kind)

        ready = bool(data.get("ready"))
        missing = (data.get("missing_info") or "").strip()
        if ready and "needs-info" not in new_labels:
            if "ready-for-dev" not in new_labels:
                new_labels.append("ready-for-dev")
        elif missing:
            if "needs-info" not in new_labels:
                new_labels.append("needs-info")
            if "needs-info-commented" not in labels:
                await self.gh.comment_issue(
                    number,
                    f"🤖 Triage Agent — il me manque de l'info pour assigner cette issue :\n\n> {missing}\n\nPeux-tu compléter le body ?",
                )
                new_labels.append("needs-info-commented")

        # set complete label set (replace)
        if set(new_labels) != labels:
            await self.gh.set_issue_labels(number, new_labels)
            await log_event(
                "triage", "labeled", issue=number,
                added=sorted(set(new_labels) - labels),
                removed=sorted(labels - set(new_labels)),
                tokens=res.total_tokens, model=res.model,
            )
