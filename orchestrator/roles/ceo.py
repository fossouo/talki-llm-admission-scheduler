"""CEO Agent — orchestration : lit les PRs, applique charter_rules, agit.

Pas de génération créative. Délègue toutes les décisions déterministes à
`orchestrator.charter_rules`, ne s'occupe que de l'I/O GitHub + audit.
"""
from __future__ import annotations

import logging
import os

from ..audit import log_event
from ..charter_rules import check_charter_3, check_charter_4, is_agent_pr
from ..config import role
from ..github_app import GitHubApp, GitHubAppError

log = logging.getLogger(__name__)


class CEOAgent:
    def __init__(self, gh: GitHubApp) -> None:
        self.gh = gh
        self.role = role("ceo")

    async def heartbeat(self) -> None:
        if os.environ.get("KOLA_CEO_PAUSED") == "1":
            await log_event("ceo", "paused", reason="KOLA_CEO_PAUSED=1")
            return
        try:
            prs = await self.gh.list_open_prs()
        except GitHubAppError as e:
            log.error("CEO heartbeat: list_open_prs failed : %s", e)
            await log_event("ceo", "heartbeat-fail", error=str(e))
            return

        for pr in prs:
            try:
                await self._review_pr(pr)
            except Exception as e:
                log.exception("CEO review PR #%s a échoué", pr.get("number"))
                await log_event("ceo", "review-fail", pr=pr.get("number"), error=str(e))

    async def _review_pr(self, pr: dict) -> None:
        number = pr["number"]
        if pr.get("draft"):
            return
        if not is_agent_pr(pr):
            return  # PRs humaines : pas notre rôle

        files = await self.gh.get_pr_files(number)
        status = await self.gh.get_pr_status(number)

        rej = check_charter_4(pr, files)
        if not rej.ok:
            await self.gh.comment_pr(
                number,
                f"🛑 CEO Agent — rejection automatique (charte §4) :\n\n- {rej.reason}\n\nFermeture immédiate. Voir CHARTER.md §1/§4.",
            )
            await self.gh.close_pr(number)
            await log_event("ceo", "auto-close", pr=number, reason=rej.reason)
            return

        ok = check_charter_3(pr, files, status)
        if ok.ok:
            try:
                await self.gh.approve_pr(number, "Auto-approved by CEO Agent (charte §3, all conditions met).")
            except GitHubAppError as e:
                # Bot ne peut pas approuver sa propre PR — c'est OK, on tente le merge.
                log.debug("approve_pr a échoué (normal pour bot self-approve) : %s", e)
            try:
                await self.gh.merge_pr(
                    number,
                    commit_title=pr["title"],
                    commit_message=(
                        "Auto-merged by CEO Agent (charte §3).\n\n"
                        f"Original body:\n{pr.get('body', '')[:500]}"
                    ),
                )
            except GitHubAppError as e:
                # Branch protection peut bloquer (approval required, status missing…).
                # On marque needs-human-review, on n'échoue pas le heartbeat.
                if "405" in str(e) or "branch protection" in str(e).lower() or "approving review" in str(e).lower():
                    labels = {lbl["name"] for lbl in pr.get("labels", [])}
                    if "needs-human-review" not in labels:
                        await self.gh.add_labels(number, ["needs-human-review"])
                        await self.gh.comment_pr(
                            number,
                            f"⚠️ CEO Agent — conditions §3 OK mais merge bloqué par branch protection :\n\n```\n{str(e)[:300]}\n```\n\n@fossouo : approve + merge manuellement, ou ajuster la protection pour permettre merge sans review humaine si CI = green.",
                        )
                    await log_event("ceo", "merge-blocked", pr=number, error=str(e)[:300])
                    return
                raise
            # nettoie un éventuel needs-human-review hérité d'un retry précédent
            labels = {lbl["name"] for lbl in pr.get("labels", [])}
            if "needs-human-review" in labels:
                try:
                    await self.gh.remove_label(number, "needs-human-review")
                except GitHubAppError:
                    pass
            await self.gh.add_labels(number, ["auto-merged-by-ceo-agent"])
            await log_event("ceo", "auto-merge", pr=number, sha=pr["head"]["sha"])
            return

        # ni rejet, ni merge auto → needs human review
        labels = {lbl["name"] for lbl in pr.get("labels", [])}
        if "needs-human-review" in labels:
            return
        await self.gh.add_labels(number, ["needs-human-review"])
        await self.gh.comment_pr(
            number,
            f"⚠️ CEO Agent — revue humaine requise (charte §3) :\n\n- {ok.reason}\n\nAucune action automatique. @fossouo doit décider.",
        )
        await log_event("ceo", "needs-human-review", pr=number, reason=ok.reason)
